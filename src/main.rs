//! `animus-workflow-runner-default` binary entrypoint.
//!
//! Implements TWO modes:
//!
//! 1. **JSON-RPC stdio plugin** (default). Listens for newline-delimited
//!    JSON-RPC 2.0 frames on stdin and replies on stdout per the
//!    `animus-plugin-protocol@1.1.0` contract. Handles `initialize`,
//!    `$/ping`, `health/check`, `shutdown`, `exit`, plus the two
//!    `workflow_runner` methods (`workflow/execute`, `workflow/run_phase`).
//!
//! 2. **Direct-execute CLI** (`execute` subcommand). Invoked by the daemon
//!    scheduler as a subprocess to run a single workflow end-to-end. This
//!    matches the legacy `animus-workflow-runner` CLI surface so the
//!    daemon's `build_runner_command` path keeps working unchanged.
//!
//! Also supports `--manifest` for install-time discovery.

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

mod direct_execute;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Some(action) = parse_cli_action() {
        match action {
            CliAction::Manifest => print_manifest_and_exit(),
            CliAction::Help => print_help_and_exit(),
            CliAction::Execute(args) => {
                init_tracing();
                let code = direct_execute::run_execute(*args).await;
                std::process::exit(code as i32);
            }
        }
    }

    if io::stdin().is_terminal() {
        eprintln!("animus-workflow-runner-default is a STDIO plugin; pipe JSON-RPC on stdin or pass --manifest");
        std::process::exit(2);
    }

    init_tracing();
    run_stdio_plugin_loop().await
}

enum CliAction {
    Manifest,
    Help,
    Execute(Box<direct_execute::ExecuteArgs>),
}

fn parse_cli_action() -> Option<CliAction> {
    let mut args = std::env::args().skip(1).peekable();
    let first = args.peek()?.clone();
    match first.as_str() {
        "--manifest" | "-m" => Some(CliAction::Manifest),
        "--help" | "-h" => Some(CliAction::Help),
        "execute" => {
            args.next();
            match direct_execute::ExecuteArgs::parse(args) {
                Ok(parsed) => Some(CliAction::Execute(Box::new(parsed))),
                Err(error) => {
                    eprintln!("animus-workflow-runner-default execute: {error}");
                    std::process::exit(2);
                }
            }
        }
        _ => None,
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .try_init();
}

fn print_manifest_and_exit() -> ! {
    let mut stdout = io::stdout().lock();
    let _ = writeln!(stdout, "{}", serde_json::to_string(&plugin_manifest()).expect("serialize manifest"));
    let _ = stdout.flush();
    std::process::exit(0);
}

fn print_help_and_exit() -> ! {
    eprintln!("animus-workflow-runner-default — Animus v0.5 workflow_runner plugin + direct-execute runner");
    eprintln!("Usage:");
    eprintln!("  animus-workflow-runner-default --manifest    Print plugin manifest as JSON and exit");
    eprintln!("  animus-workflow-runner-default               Run JSON-RPC loop on stdin/stdout");
    eprintln!("  animus-workflow-runner-default execute ...   Run a workflow end-to-end (CLI mode)");
    std::process::exit(0);
}

async fn run_stdio_plugin_loop() -> anyhow::Result<()> {
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
