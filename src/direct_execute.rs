//! Direct-execute CLI mode for `animus-workflow-runner-default`.
//!
//! Mirrors the legacy `animus-workflow-runner execute ...` CLI surface so
//! the daemon scheduler's `build_runner_command` path keeps working without
//! changes. Bridges argv into [`crate::workflow_execute::execute_workflow_with_hub`]
//! after constructing a fresh [`orchestrator_core::FileServiceHub`] and
//! composing the back-channel emitter set
//! ([`animus_runtime_shared::workflow_event_emitter::SubprocessPipeEmitter`] +
//! [`animus_runtime_shared::reattach::ReattachListenerEmitter`]).

use std::collections::HashMap;
use std::sync::Arc;

use animus_runtime_shared::reattach::ReattachListenerEmitter;
use animus_runtime_shared::workflow_event_emitter::{FanoutEmitter, SharedWorkflowEventEmitter, SubprocessPipeEmitter};
use animus_workflow_runner_default::workflow_execute::{execute_workflow_with_hub, WorkflowExecuteInternalParams};
use orchestrator_core::{FileServiceHub, WorkflowStatus};
use orchestrator_logging::Logger;
use serde::Serialize;

pub struct ExecuteArgs {
    pub workflow_id: Option<String>,
    pub task_id: Option<String>,
    pub requirement_id: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub workflow_ref: Option<String>,
    pub input_json: Option<String>,
    pub project_root: String,
    /// Reserved for forward compatibility with the legacy in-tree
    /// `animus-workflow-runner` CLI surface; not yet consumed.
    #[allow(dead_code)]
    pub config_path: Option<String>,
    pub model: Option<String>,
    pub tool: Option<String>,
    pub phase_timeout_secs: Option<u64>,
    pub phase_routing_json: Option<String>,
    pub mcp_config_json: Option<String>,
}

impl ExecuteArgs {
    pub fn parse<I: Iterator<Item = String>>(args: I) -> Result<Self, String> {
        // Normalize Clap-style `--key=value` tokens into the `--key`, `value`
        // pair shape this parser expects so scripted invocations carried over
        // from the legacy clap-based runner keep working.
        let normalized: Vec<String> = args.flat_map(split_equals).collect();
        let mut args = normalized.into_iter();
        let mut workflow_id = None;
        let mut task_id = None;
        let mut requirement_id = None;
        let mut title = None;
        let mut description = None;
        let mut workflow_ref = None;
        let mut input_json = None;
        let mut project_root: Option<String> = None;
        let mut config_path = None;
        let mut model = None;
        let mut tool = None;
        let mut phase_timeout_secs = None;
        let mut phase_routing_json = None;
        let mut mcp_config_json = None;

        while let Some(arg) = args.next() {
            let key = arg.as_str();
            let value = match key {
                "--workflow-id"
                | "--task-id"
                | "--requirement-id"
                | "--title"
                | "--description"
                | "--workflow-ref"
                | "--input-json"
                | "--project-root"
                | "--config-path"
                | "--model"
                | "--tool"
                | "--phase-timeout-secs"
                | "--phase-routing-json"
                | "--mcp-config-json" => args.next().ok_or_else(|| format!("missing value for {key}"))?,
                "--help" | "-h" => {
                    eprintln!("animus-workflow-runner-default execute --project-root <path> [--task-id <id> | --requirement-id <id> | --title <s>] [--workflow-ref <ref>] ...");
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}")),
            };
            match key {
                "--workflow-id" => workflow_id = Some(value),
                "--task-id" => task_id = Some(value),
                "--requirement-id" => requirement_id = Some(value),
                "--title" => title = Some(value),
                "--description" => description = Some(value),
                "--workflow-ref" => workflow_ref = Some(value),
                "--input-json" => input_json = Some(value),
                "--project-root" => project_root = Some(value),
                "--config-path" => config_path = Some(value),
                "--model" => model = Some(value),
                "--tool" => tool = Some(value),
                "--phase-timeout-secs" => {
                    phase_timeout_secs = Some(value.parse().map_err(|e| format!("invalid --phase-timeout-secs: {e}"))?)
                }
                "--phase-routing-json" => phase_routing_json = Some(value),
                "--mcp-config-json" => mcp_config_json = Some(value),
                _ => unreachable!(),
            }
        }

        let project_root = project_root.ok_or_else(|| "--project-root is required".to_string())?;
        Ok(Self {
            workflow_id,
            task_id,
            requirement_id,
            title,
            description,
            workflow_ref,
            input_json,
            project_root,
            config_path,
            model,
            tool,
            phase_timeout_secs,
            phase_routing_json,
            mcp_config_json,
        })
    }
}

#[derive(Debug, Serialize)]
struct RunnerEvent {
    event: &'static str,
    task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_status: Option<WorkflowStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_code: Option<i32>,
}

fn compose_event_emitter() -> Option<SharedWorkflowEventEmitter> {
    let mut sinks: Vec<SharedWorkflowEventEmitter> = Vec::new();
    if let Some(pipe) = SubprocessPipeEmitter::from_env() {
        sinks.push(pipe as SharedWorkflowEventEmitter);
    }
    if let Some(listener) = ReattachListenerEmitter::from_env() {
        sinks.push(listener as SharedWorkflowEventEmitter);
    }
    match sinks.len() {
        0 => None,
        1 => Some(sinks.into_iter().next().expect("len 1")),
        _ => Some(FanoutEmitter::new(sinks) as SharedWorkflowEventEmitter),
    }
}

pub async fn run_execute(args: ExecuteArgs) -> u8 {
    match run_execute_inner(args).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("animus-workflow-runner-default failed: {error}");
            1
        }
    }
}

async fn run_execute_inner(args: ExecuteArgs) -> anyhow::Result<u8> {
    let subject_id = args
        .workflow_id
        .as_deref()
        .or(args.task_id.as_deref())
        .or(args.requirement_id.as_deref())
        .or(args.title.as_deref())
        .unwrap_or("unknown")
        .to_string();

    let startup = RunnerEvent {
        event: "runner_start",
        task_id: subject_id.clone(),
        workflow_id: args.workflow_id.clone(),
        workflow_ref: args.workflow_ref.clone(),
        workflow_status: None,
        exit_code: None,
    };
    eprintln!("{}", serde_json::to_string(&startup).unwrap_or_default());

    let phase_routing = args.phase_routing_json.as_deref().and_then(|json| serde_json::from_str(json).ok());
    let mcp_config = args.mcp_config_json.as_deref().and_then(|json| serde_json::from_str(json).ok());
    let log_project_root = args.project_root.clone();
    let log_workflow_ref = args.workflow_ref.clone().unwrap_or_default();
    let wf_log_root = log_project_root.clone();
    let wf_log_ref = log_workflow_ref.clone();

    let params = WorkflowExecuteInternalParams {
        project_root: args.project_root.clone(),
        workflow_id: args.workflow_id,
        task_id: args.task_id,
        requirement_id: args.requirement_id,
        title: args.title,
        description: args.description,
        workflow_ref: args.workflow_ref.clone(),
        input: args.input_json.as_deref().map(serde_json::from_str).transpose()?,
        vars: HashMap::new(),
        model: args.model,
        tool: args.tool,
        phase_timeout_secs: args.phase_timeout_secs,
        phase_filter: None,
        phase_routing,
        mcp_config,
    };

    let hub: Arc<dyn orchestrator_core::services::ServiceHub> = Arc::new(
        FileServiceHub::new(std::path::Path::new(&args.project_root))
            .map_err(|error| anyhow::anyhow!("failed to open FileServiceHub: {error}"))?,
    );

    {
        let wf_logger = Logger::for_project(std::path::Path::new(&wf_log_root));
        wf_logger
            .info("workflow.start", format!("started {}", wf_log_ref))
            .subject(subject_id.as_str())
            .meta(serde_json::json!({"workflow_ref": wf_log_ref}))
            .emit();
    }

    let event_emitter = compose_event_emitter();
    let wf_start = std::time::Instant::now();
    let result = execute_workflow_with_hub(params, hub, event_emitter).await;
    let wf_duration = wf_start.elapsed();

    {
        let wf_logger = Logger::for_project(std::path::Path::new(&wf_log_root));
        let success = matches!(&result, Ok(r) if r.success);
        let mut b = if success {
            wf_logger.info("workflow.complete", format!("{} completed", wf_log_ref))
        } else {
            wf_logger.error("workflow.complete", format!("{} failed", wf_log_ref))
        };
        b = b
            .subject(subject_id.as_str())
            .duration(wf_duration.as_millis() as u64)
            .meta(serde_json::json!({"workflow_ref": wf_log_ref}));
        if let Err(ref e) = result {
            b = b.err(e.to_string());
        } else if let Ok(ref r) = result {
            b = b.meta(serde_json::json!({
                "workflow_ref": wf_log_ref,
                "phases_completed": r.phases_completed,
                "phases_total": r.phases_total,
            }));
        }
        b.emit();
    }
    let _ = log_project_root;

    let exit_code: i32 = match &result {
        Ok(r) if r.success => 0,
        Ok(_) => 1,
        Err(_) => 1,
    };

    let workflow_ref = match &result {
        Ok(value) => {
            if value.workflow_ref.trim().is_empty() {
                args.workflow_ref.clone()
            } else {
                Some(value.workflow_ref.clone())
            }
        }
        Err(_) => args.workflow_ref.clone(),
    };
    let workflow_status = match &result {
        Ok(value) => Some(value.workflow_status),
        Err(_) => Some(WorkflowStatus::Failed),
    };

    let completion = RunnerEvent {
        event: "runner_complete",
        task_id: subject_id,
        workflow_id: result.as_ref().ok().map(|value| value.workflow_id.clone()),
        workflow_ref,
        workflow_status,
        exit_code: Some(exit_code),
    };
    eprintln!("{}", serde_json::to_string(&completion).unwrap_or_default());

    if let Err(ref error) = result {
        eprintln!("workflow execution failed: {error}");
    }

    Ok(clamp_exit_code(exit_code))
}

/// Normalize a single argv token into one-or-two output tokens so a
/// `--key=value` form coming from clap-style scripts is treated identically
/// to the legacy `--key value` form. Only flags (tokens that start with
/// `--`) are split; bare value tokens are passed through unchanged so values
/// containing literal `=` (eg JSON blobs) are not corrupted.
fn split_equals(arg: String) -> Vec<String> {
    if !arg.starts_with("--") {
        return vec![arg];
    }
    match arg.split_once('=') {
        Some((flag, value)) if !flag.is_empty() => vec![flag.to_string(), value.to_string()],
        _ => vec![arg],
    }
}

fn clamp_exit_code(code: i32) -> u8 {
    match u8::try_from(code) {
        Ok(value) => value,
        Err(_) => {
            if code < 0 {
                1
            } else {
                u8::MAX
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_exit_code_zero() {
        assert_eq!(clamp_exit_code(0), 0);
    }

    #[test]
    fn clamp_exit_code_normal() {
        assert_eq!(clamp_exit_code(1), 1);
        assert_eq!(clamp_exit_code(255), 255);
    }

    #[test]
    fn clamp_exit_code_negative() {
        assert_eq!(clamp_exit_code(-1), 1);
    }

    #[test]
    fn clamp_exit_code_overflow() {
        assert_eq!(clamp_exit_code(256), u8::MAX);
        assert_eq!(clamp_exit_code(i32::MAX), u8::MAX);
    }

    #[test]
    fn execute_args_parse_minimal_task() {
        let argv = vec![
            "--task-id".to_string(),
            "TASK-001".to_string(),
            "--project-root".to_string(),
            "/tmp/project".to_string(),
            "--workflow-ref".to_string(),
            "default".to_string(),
        ];
        let args = ExecuteArgs::parse(argv.into_iter()).expect("parse");
        assert_eq!(args.task_id.as_deref(), Some("TASK-001"));
        assert_eq!(args.project_root, "/tmp/project");
        assert_eq!(args.workflow_ref.as_deref(), Some("default"));
        assert!(args.requirement_id.is_none());
        assert!(args.title.is_none());
    }

    #[test]
    fn execute_args_parse_custom_subject_with_input() {
        let argv = vec![
            "--title".to_string(),
            "ad-hoc".to_string(),
            "--description".to_string(),
            "scheduled".to_string(),
            "--workflow-ref".to_string(),
            "ops".to_string(),
            "--project-root".to_string(),
            "/tmp/p".to_string(),
            "--input-json".to_string(),
            "{\"nightly\":true}".to_string(),
            "--phase-routing-json".to_string(),
            "{}".to_string(),
            "--mcp-config-json".to_string(),
            "{}".to_string(),
        ];
        let args = ExecuteArgs::parse(argv.into_iter()).expect("parse");
        assert_eq!(args.title.as_deref(), Some("ad-hoc"));
        assert_eq!(args.description.as_deref(), Some("scheduled"));
        assert_eq!(args.input_json.as_deref(), Some("{\"nightly\":true}"));
        assert_eq!(args.phase_routing_json.as_deref(), Some("{}"));
        assert_eq!(args.mcp_config_json.as_deref(), Some("{}"));
    }

    #[test]
    fn execute_args_missing_project_root_is_error() {
        let argv = vec!["--task-id".to_string(), "TASK-001".to_string()];
        let result = ExecuteArgs::parse(argv.into_iter());
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.contains("--project-root"));
    }

    #[test]
    fn execute_args_supports_equals_style_flags() {
        // Scripts carried over from the clap-based legacy runner use the
        // `--key=value` form. The parser must split these so both surfaces
        // accept the same argv.
        let argv = vec![
            "--project-root=/repo".to_string(),
            "--task-id=TASK-XYZ".to_string(),
            "--workflow-ref=default".to_string(),
        ];
        let args = ExecuteArgs::parse(argv.into_iter()).expect("parse");
        assert_eq!(args.project_root, "/repo");
        assert_eq!(args.task_id.as_deref(), Some("TASK-XYZ"));
        assert_eq!(args.workflow_ref.as_deref(), Some("default"));
    }

    #[test]
    fn execute_args_unknown_flag_is_error() {
        let argv = vec!["--bogus".to_string()];
        let result = ExecuteArgs::parse(argv.into_iter());
        assert!(result.is_err());
    }

    #[test]
    fn compose_event_emitter_returns_none_when_neither_env_set() {
        // Defensive: in a clean test environment neither the legacy pipe nor
        // the reattach socket env vars are set, so compose should yield None
        // (callers fall back to the noop emitter).
        let prev_pipe =
            std::env::var(animus_runtime_shared::workflow_event_emitter::ANIMUS_WORKFLOW_EVENT_PIPE_ENV).ok();
        let prev_reattach = std::env::var(animus_runtime_shared::reattach::ANIMUS_WORKFLOW_REATTACH_SOCKET_ENV).ok();
        std::env::remove_var(animus_runtime_shared::workflow_event_emitter::ANIMUS_WORKFLOW_EVENT_PIPE_ENV);
        std::env::remove_var(animus_runtime_shared::reattach::ANIMUS_WORKFLOW_REATTACH_SOCKET_ENV);
        assert!(compose_event_emitter().is_none());
        if let Some(v) = prev_pipe {
            std::env::set_var(animus_runtime_shared::workflow_event_emitter::ANIMUS_WORKFLOW_EVENT_PIPE_ENV, v);
        }
        if let Some(v) = prev_reattach {
            std::env::set_var(animus_runtime_shared::reattach::ANIMUS_WORKFLOW_REATTACH_SOCKET_ENV, v);
        }
    }
}
