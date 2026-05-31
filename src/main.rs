//! `animus-workflow-runner-default` binary entrypoint.
//!
//! Implements the v0.5 plugin contract: newline-delimited JSON-RPC 2.0 over
//! stdio. The host (the daemon's plugin manager) speaks
//! `animus-plugin-protocol@1.1.0`; this binary handles `initialize`,
//! `$/ping`, `health/check`, `shutdown`, `exit`, plus the two
//! `workflow_runner` methods (`workflow/execute`, `workflow/run_phase`).
//!
//! Supports `--manifest` for install-time discovery (matches the convention
//! used by `animus-plugin-runtime`'s providers).

use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes as core_error_codes, HealthCheckResult, HealthStatus, InitializeParams, RpcError, RpcRequest,
    RpcResponse,
};
use animus_workflow_runner_default::plugin::{
    classify_error, handle_workflow_execute, handle_workflow_run_phase, plugin_initialize_result, plugin_manifest,
};
use animus_workflow_runner_protocol::{METHOD_WORKFLOW_EXECUTE, METHOD_WORKFLOW_RUN_PHASE};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    handle_cli_args();

    if io::stdin().is_terminal() {
        eprintln!("animus-workflow-runner-default is a STDIO plugin; pipe JSON-RPC on stdin or pass --manifest");
        std::process::exit(2);
    }

    init_tracing();

    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();

    while let Some(line) = reader.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request: RpcRequest = match serde_json::from_str(trimmed) {
            Ok(r) => r,
            Err(error) => {
                tracing::warn!(%error, "invalid JSON-RPC frame");
                continue;
            }
        };

        let stdout = stdout.clone();
        tokio::spawn(async move {
            handle_request(request, stdout).await;
        });
    }

    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .try_init();
}

fn handle_cli_args() {
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--manifest" | "-m" => print_manifest_and_exit(),
            "--help" | "-h" => {
                eprintln!("animus-workflow-runner-default — STDIO workflow_runner plugin for Animus v0.5");
                eprintln!("Usage:");
                eprintln!("  animus-workflow-runner-default --manifest    Print plugin manifest as JSON and exit");
                eprintln!("  animus-workflow-runner-default               Run JSON-RPC loop on stdin/stdout");
                std::process::exit(0);
            }
            _ => {}
        }
    }
}

fn print_manifest_and_exit() -> ! {
    let mut stdout = io::stdout().lock();
    let _ = writeln!(stdout, "{}", serde_json::to_string(&plugin_manifest()).expect("serialize manifest"));
    let _ = stdout.flush();
    std::process::exit(0);
}

async fn handle_request(request: RpcRequest, stdout: Arc<Mutex<tokio::io::Stdout>>) {
    let id = request.id.clone();
    let response = match request.method.as_str() {
        "initialize" => Some(match parse_init_params(request.params) {
            Ok(params) => match plugin_initialize_result(&params) {
                Ok(result) => match serde_json::to_value(result) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, internal_error_rpc(format!("encode initialize: {error}"))),
                },
                Err(error) => RpcResponse::err(id, internal_error_rpc(error.to_string())),
            },
            Err(error) => RpcResponse::err(id, invalid_params_rpc(error)),
        }),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(
            match serde_json::to_value(HealthCheckResult {
                status: HealthStatus::Healthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: None,
            }) {
                Ok(value) => RpcResponse::ok(id, value),
                Err(error) => RpcResponse::err(id, internal_error_rpc(format!("encode health: {error}"))),
            },
        ),
        METHOD_WORKFLOW_EXECUTE => Some(dispatch_workflow_execute(id, request.params).await),
        METHOD_WORKFLOW_RUN_PHASE => Some(dispatch_workflow_run_phase(id, request.params).await),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        "exit" => std::process::exit(0),
        other if other.starts_with("$/") => None,
        other => Some(RpcResponse::err(
            id,
            RpcError {
                code: core_error_codes::METHOD_NOT_FOUND,
                message: format!("method '{other}' not implemented by animus-workflow-runner-default"),
                data: None,
            },
        )),
    };

    if let Some(response) = response {
        write_frame(&stdout, &response).await;
    }
}

fn parse_init_params(params: Option<Value>) -> Result<InitializeParams, String> {
    let params = params.ok_or_else(|| "missing initialize params".to_string())?;
    serde_json::from_value(params).map_err(|e| format!("invalid initialize params: {e}"))
}

async fn dispatch_workflow_execute(id: Option<Value>, params: Option<Value>) -> RpcResponse {
    let params = match params {
        Some(p) => p,
        None => return RpcResponse::err(id, invalid_params_rpc("missing params for workflow/execute")),
    };
    let request: animus_workflow_runner_protocol::WorkflowExecuteRequest = match serde_json::from_value(params) {
        Ok(r) => r,
        Err(error) => {
            return RpcResponse::err(id, invalid_params_rpc(format!("invalid workflow/execute params: {error}")))
        }
    };

    match handle_workflow_execute(request).await {
        Ok(result) => match serde_json::to_value(result) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, internal_error_rpc(format!("encode workflow/execute result: {error}"))),
        },
        Err(error) => {
            let (code, message) = classify_error(&error);
            RpcResponse::err(id, RpcError { code, message, data: None })
        }
    }
}

async fn dispatch_workflow_run_phase(id: Option<Value>, params: Option<Value>) -> RpcResponse {
    let params = match params {
        Some(p) => p,
        None => return RpcResponse::err(id, invalid_params_rpc("missing params for workflow/run_phase")),
    };
    let request: animus_workflow_runner_protocol::WorkflowPhaseRunRequest = match serde_json::from_value(params) {
        Ok(r) => r,
        Err(error) => {
            return RpcResponse::err(id, invalid_params_rpc(format!("invalid workflow/run_phase params: {error}")))
        }
    };

    match handle_workflow_run_phase(request).await {
        Ok(result) => match serde_json::to_value(result) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => {
                RpcResponse::err(id, internal_error_rpc(format!("encode workflow/run_phase result: {error}")))
            }
        },
        Err(error) => {
            let (code, message) = classify_error(&error);
            RpcResponse::err(id, RpcError { code, message, data: None })
        }
    }
}

fn invalid_params_rpc(message: impl Into<String>) -> RpcError {
    RpcError { code: core_error_codes::INVALID_PARAMS, message: message.into(), data: None }
}

fn internal_error_rpc(message: impl Into<String>) -> RpcError {
    RpcError { code: core_error_codes::INTERNAL_ERROR, message: message.into(), data: None }
}

async fn write_frame<T: serde::Serialize>(stdout: &Arc<Mutex<tokio::io::Stdout>>, frame: &T) {
    if let Ok(mut payload) = serde_json::to_string(frame) {
        payload.push('\n');
        let mut guard = stdout.lock().await;
        let _ = guard.write_all(payload.as_bytes()).await;
        let _ = guard.flush().await;
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn handler_supports_protocol_1_1_x() {
        assert!(animus_plugin_protocol::PROTOCOL_VERSION.starts_with("1."), "plugin builds against protocol v1.x",);
    }
}
