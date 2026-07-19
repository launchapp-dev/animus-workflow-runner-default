//! Plugin shell: JSON-RPC handler functions, initialize-time project binding,
//! and the request → internal-params translation for the v0.5 wire contract.
//!
//! Public entry points:
//! - [`plugin_manifest`] — used when the binary is invoked with `--manifest`.
//! - [`plugin_initialize_result`] — built and returned for the `initialize`
//!   RPC. Reads `init_extensions.project_binding.project_root`, constructs
//!   the per-project [`PluginState`], and stores it in process-global memory.
//! - [`handle_workflow_execute`] — implements `workflow/execute`.
//! - [`handle_workflow_run_phase`] — implements `workflow/run_phase`.
//!
//! All public types here are wire-shaped (no `Arc<dyn ServiceHub>`, no
//! closures); the heavy in-process types live in `crate::workflow_execute`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use animus_plugin_protocol::{
    InitializeResult, PluginCapabilities, PluginInfo, PluginManifest, PROTOCOL_VERSION as PLUGIN_PROTOCOL_VERSION,
};
use animus_workflow_runner_protocol::{
    error_codes, phase_status, workflow_status, PhaseResultSnapshot, WorkflowExecuteRequest, WorkflowExecuteResult,
    WorkflowPhaseRunRequest, WorkflowPhaseRunResult, KIND as WORKFLOW_RUNNER_KIND,
};
use anyhow::{anyhow, Context, Result};
use orchestrator_core::{services::ServiceHub, FileServiceHub, WorkflowStatus};
use serde_json::Value;

use crate::phase_event_recorder::PhaseEventRecorder;
use crate::workflow_execute::{execute_workflow_with_hub, WorkflowExecuteInternalParams};

/// Plugin and binary name.
pub const PLUGIN_NAME: &str = "animus-workflow-runner-default";
/// Plugin semver (sourced from `Cargo.toml` so it can never drift).
pub const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Plugin description.
pub const PLUGIN_DESCRIPTION: &str =
    "Reference workflow_runner plugin for Animus v0.5 (stdio JSON-RPC + direct-execute CLI; replaces in-tree workflow-runner-v2)";
/// Init-extension key for the v0.5 project binding map.
pub const PROJECT_BINDING_EXTENSION: &str = "project_binding";
/// Init-extension key for an explicit memory MCP stdio command path supplied
/// by the host daemon (codex P2 #4). When set, the plugin uses this command
/// instead of probing for a sibling `animus` binary. Documented in README.
pub const MEMORY_MCP_STDIO_COMMAND_EXTENSION: &str = "memory_mcp_stdio_command";

/// Per-process plugin state established at `initialize` time.
pub struct PluginState {
    pub project_root: PathBuf,
    pub repo_scope: Option<String>,
    pub hub: Arc<dyn ServiceHub>,
    /// Optional host-supplied memory MCP stdio command path. When `Some`,
    /// `inject_memory_mcp_for_capable_agent` prefers it over the
    /// sibling-binary discovery fallback. See README "init_extensions".
    pub memory_mcp_stdio_command: Option<String>,
}

static PLUGIN_STATE: OnceLock<Mutex<Option<Arc<PluginState>>>> = OnceLock::new();

fn state_slot() -> &'static Mutex<Option<Arc<PluginState>>> {
    PLUGIN_STATE.get_or_init(|| Mutex::new(None))
}

/// Test-only escape hatch: install a custom hub before any RPC. Production
/// `initialize` flows through [`plugin_initialize_result`].
pub fn install_plugin_state(state: PluginState) {
    let mut guard = state_slot().lock().unwrap();
    *guard = Some(Arc::new(state));
}

/// Best-effort read of the optional `memory_mcp_stdio_command` override
/// supplied via init_extensions. Returns `None` when the plugin is not
/// initialized or no override was supplied; callers MUST fall back to
/// sibling-binary discovery (and finally refusal) in that case.
pub fn memory_mcp_stdio_command_override() -> Option<String> {
    state_slot().lock().ok().and_then(|guard| guard.as_ref().and_then(|state| state.memory_mcp_stdio_command.clone()))
}

/// Read the current plugin state. Returns an error if `initialize` has not
/// yet been processed.
fn current_state() -> Result<Arc<PluginState>> {
    state_slot()
        .lock()
        .map_err(|_| anyhow!("plugin state lock poisoned"))?
        .clone()
        .ok_or_else(|| anyhow!("plugin not initialized (workflow/execute called before initialize handshake)"))
}

/// Reject requests whose implied project root differs from the bound one.
/// Returns an `error_codes::PROJECT_BINDING_MISMATCH`-tagged anyhow error
/// so callers can map to the wire error code.
///
/// Canonicalizes both the bound root and the candidate path before the
/// `starts_with` check (codex P1 round 2 — lexical `starts_with` accepts
/// `/bound/../other`). If `canonicalize()` fails for the candidate (e.g.
/// the worktree directory has not been created yet), the binding check
/// uses the lexically-normalized form so freshly-issued execution
/// directories are still acceptable when they syntactically nest under
/// the bound root.
fn enforce_project_binding(state: &PluginState, candidate: &str) -> Result<()> {
    let candidate_path = std::path::Path::new(candidate);
    let bound_path = state.project_root.as_path();

    let bound_canonical = bound_path.canonicalize().unwrap_or_else(|_| bound_path.to_path_buf());
    let candidate_canonical = candidate_path.canonicalize().unwrap_or_else(|_| lexical_normalize(candidate_path));

    if candidate_canonical == bound_canonical || candidate_canonical.starts_with(&bound_canonical) {
        return Ok(());
    }

    Err(anyhow!(
        "PROJECT_BINDING_MISMATCH: plugin bound to {} but request implied {} (canonical: {})",
        bound_canonical.display(),
        candidate_path.display(),
        candidate_canonical.display(),
    ))
}

/// Best-effort lexical normalization for a path whose target may not yet
/// exist on disk. Resolves `.` and `..` components purely syntactically.
fn lexical_normalize(path: &std::path::Path) -> PathBuf {
    use std::path::Component;
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

/// Return the static `--manifest` shape used by `animus plugin install`.
pub fn plugin_manifest() -> PluginManifest {
    PluginManifest {
        name: PLUGIN_NAME.to_string(),
        version: PLUGIN_VERSION.to_string(),
        plugin_kind: WORKFLOW_RUNNER_KIND.to_string(),
        // rc.6 multi-kind manifests: this plugin serves a single role, declared
        // via the legacy `plugin_kind` field; no additional kinds.
        plugin_kinds: Vec::new(),
        description: PLUGIN_DESCRIPTION.to_string(),
        protocol_version: PLUGIN_PROTOCOL_VERSION.to_string(),
        capabilities: vec![
            animus_workflow_runner_protocol::METHOD_WORKFLOW_EXECUTE.to_string(),
            animus_workflow_runner_protocol::METHOD_WORKFLOW_RUN_PHASE.to_string(),
        ],
        env_required: Vec::new(),
        notification_buffer_size: None,
        // rc.6 (TASK-277): MCP capability is undeclared for this workflow_runner
        // plugin; the kernel applies its default. `None` = not an MCP provider.
        supports_mcp: None,
    }
}

/// Build the `initialize` response, side-effect: install plugin state from
/// the `project_binding` extension. Idempotent for repeated `initialize`
/// calls against the same project root.
pub fn plugin_initialize_result(params: &Value) -> Result<InitializeResult> {
    let protocol_version = params.get("protocol_version").and_then(Value::as_str).unwrap_or_default();
    if !protocol_version.starts_with("1.") {
        return Err(anyhow!("incompatible host protocol version '{}'; plugin requires 1.x", protocol_version));
    }

    // `init_extensions` is sent by the kernel in the raw `initialize` params
    // JSON (the typed `InitializeParams` struct no longer carries it as of the
    // current animus-cli protocol). Parse it directly from the params Value.
    let empty = serde_json::Map::new();
    let init_extensions = params.get("init_extensions").and_then(Value::as_object).unwrap_or(&empty);

    let binding_value = init_extensions
        .get(PROJECT_BINDING_EXTENSION)
        .ok_or_else(|| anyhow!("missing project_binding init extension"))?;
    let project_root = binding_value
        .get("project_root")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("project_binding.project_root must be a string"))?;
    let repo_scope = binding_value.get("repo_scope").and_then(Value::as_str).map(ToOwned::to_owned);

    let project_root_path = PathBuf::from(project_root);
    let hub: Arc<dyn ServiceHub> = Arc::new(
        FileServiceHub::new(&project_root_path)
            .map_err(|error| anyhow!("failed to open FileServiceHub for {project_root}: {error}"))?,
    );

    // Codex P2 #4: optional `memory_mcp_stdio_command` init-extension lets the
    // daemon supply an explicit memory MCP binary path so the plugin does not
    // need to discover a sibling `animus` binary. Accepts either a bare
    // string or an object with `command` / `path`.
    let memory_mcp_stdio_command = init_extensions
        .get(MEMORY_MCP_STDIO_COMMAND_EXTENSION)
        .and_then(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| value.get("command").and_then(Value::as_str).map(ToOwned::to_owned))
                .or_else(|| value.get("path").and_then(Value::as_str).map(ToOwned::to_owned))
        })
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    install_plugin_state(PluginState { project_root: project_root_path, repo_scope, hub, memory_mcp_stdio_command });

    Ok(InitializeResult {
        protocol_version: PLUGIN_PROTOCOL_VERSION.to_string(),
        plugin_info: PluginInfo {
            name: PLUGIN_NAME.to_string(),
            version: PLUGIN_VERSION.to_string(),
            plugin_kind: WORKFLOW_RUNNER_KIND.to_string(),
            plugin_kinds: Vec::new(),
            description: Some(PLUGIN_DESCRIPTION.to_string()),
        },
        capabilities: PluginCapabilities {
            methods: vec![
                animus_workflow_runner_protocol::METHOD_WORKFLOW_EXECUTE.to_string(),
                animus_workflow_runner_protocol::METHOD_WORKFLOW_RUN_PHASE.to_string(),
            ],
            streaming: false,
            progress: false,
            cancellation: false,
            projections: Vec::new(),
            subject_kinds: Vec::new(),
            mcp_tools: Vec::new(),
        },
        // Typed per-kind capability map (protocol v1.1.0+). Empty here keeps
        // the v1.0.0 back-compat posture; the legacy `capabilities.methods`
        // allowlist above remains authoritative for this runner.
        kind_capabilities: std::collections::HashMap::new(),
    })
}

/// Implementation of `workflow/execute`.
pub async fn handle_workflow_execute(request: WorkflowExecuteRequest) -> Result<WorkflowExecuteResult> {
    let state = current_state()?;
    let recorder = Arc::new(PhaseEventRecorder::new(state.project_root.clone()));
    let project_root_str = state.project_root.display().to_string();

    // Resolve the subject envelope. The protocol allows either
    // `subject_dispatch`, or `subject_ref` (with optional convenience
    // fields), or the legacy convenience fields directly. The lifted
    // `resolve_input` only understands the convenience fields, so we
    // project the v0.5 envelopes down to the equivalent task / requirement
    // / title triple before constructing `WorkflowExecuteInternalParams`.
    let (task_id, requirement_id, title, description) =
        resolve_subject_fields(&request).map_err(|e| anyhow!("invalid subject envelope: {e}"))?;

    let params = WorkflowExecuteInternalParams {
        project_root: project_root_str.clone(),
        workflow_id: request.workflow_id.clone(),
        task_id,
        requirement_id,
        // The stdio plugin path resolves its subject envelope via
        // `resolve_subject_fields` into the task/requirement/title triple;
        // `--subject-id` is only used by the direct-execute (daemon-dispatch)
        // path, so it is None here.
        subject_id: None,
        title,
        description,
        workflow_ref: request.workflow_ref.clone(),
        input: request.input.clone(),
        vars: request.vars.clone(),
        model: request.model.clone(),
        tool: request.tool.clone(),
        phase_timeout_secs: request.phase_timeout_secs,
        phase_filter: request.phase_filter.clone(),
        phase_routing: request.phase_routing.clone().and_then(|value| serde_json::from_value(value).ok()),
        // Wire the host-supplied per-call MCP runtime config through
        // `WorkflowExecuteInternalParams` -> `execute_workflow_with_hub` ->
        // `PhaseRunParams` -> `inject_default_stdio_mcp_with_config`.
        // When absent (CLI path), the phase falls back to
        // `McpRuntimeConfig::default()`.
        mcp_config: request.mcp_config.clone().and_then(|value| serde_json::from_value(value).ok()),
        // Relay the transport-asserted actor verbatim from the inbound
        // execute request. The runner never synthesizes or interprets it.
        actor: request.actor.clone(),
    };

    let recorder_dyn: crate::workflow_event_emitter::SharedWorkflowEventEmitter = recorder.clone();
    let internal = execute_workflow_with_hub(params, state.hub.clone(), Some(recorder_dyn), None).await?;

    let phase_results = internal.phase_results.into_iter().map(snapshot_from_value).collect();

    // Per the protocol spec (`WorkflowExecuteResult.success`): "True iff
    // workflow_status == COMPLETED". Override the lifted code's
    // single-phase shortcut where `success` was set to "the phase did
    // not fail" even though the workflow had not yet hit a terminal
    // status. Codex P2 round 3.
    let wire_workflow_status = workflow_status_to_wire(internal.workflow_status).to_string();
    let success = wire_workflow_status == workflow_status::COMPLETED;

    Ok(WorkflowExecuteResult {
        workflow_id: internal.workflow_id,
        workflow_ref: internal.workflow_ref,
        workflow_status: wire_workflow_status,
        subject_id: internal.subject_id,
        execution_cwd: internal.execution_cwd,
        phases_requested: internal.phases_requested,
        phases_completed: internal.phases_completed,
        phases_total: internal.phases_total,
        total_duration_secs: internal.total_duration.as_secs(),
        phase_results,
        post_success: internal.post_success,
        success,
        phase_events: recorder.take_events(),
    })
}

fn snapshot_from_value(value: Value) -> PhaseResultSnapshot {
    let phase_id = value.get("phase_id").and_then(Value::as_str).unwrap_or("").to_string();
    let status_raw = value.get("status").and_then(Value::as_str).unwrap_or("completed").to_string();
    let status = match status_raw.as_str() {
        "rework" => phase_status::REWORK.to_string(),
        "closed" => phase_status::CLOSED.to_string(),
        "failed" | "dispatch_retry" | "persist_failed" | "blocked_unreplayable_marker" => {
            phase_status::FAILED.to_string()
        }
        "manual_pending" => phase_status::MANUAL_PENDING.to_string(),
        "replayed_completion_marker" => phase_status::COMPLETED.to_string(),
        _ => phase_status::COMPLETED.to_string(),
    };
    let duration_secs = value.get("duration_secs").and_then(Value::as_u64).unwrap_or(0);
    let outcome = value.get("outcome").cloned().unwrap_or(Value::Null);
    let metadata = value.get("metadata").cloned().unwrap_or(Value::Null);
    let next_phase_id = value.get("next_phase_id").and_then(Value::as_str).map(ToOwned::to_owned);
    let close_reason = value.get("close_reason").and_then(Value::as_str).map(ToOwned::to_owned);
    PhaseResultSnapshot { phase_id, status, duration_secs, outcome, metadata, next_phase_id, close_reason }
}

/// Project the protocol's three-way subject envelope onto the lifted
/// `(task_id, requirement_id, title, description)` tuple expected by
/// `resolve_input` in `workflow_execute.rs`.
///
/// Priority order (codex P1 round 3 — generic subjects were previously
/// rejected):
///
/// 1. Explicit convenience fields (`task_id`, `requirement_id`,
///    `title`+`description`) take precedence.
/// 2. `subject_dispatch.subject` is inspected next; task / requirement
///    kinds project onto the matching id, custom kinds project onto a
///    synthetic title.
/// 3. `subject_ref` is the final fallback with the same projection.
///
/// Generic non-task / non-requirement subjects (e.g. Linear issues) are
/// projected as a custom title + description so the lifted code can run
/// them as ad-hoc subjects until first-class generic-subject support
/// lands. (Tracked as v0.6 work.)
type SubjectFieldsResult =
    std::result::Result<(Option<String>, Option<String>, Option<String>, Option<String>), String>;

fn resolve_subject_fields(request: &WorkflowExecuteRequest) -> SubjectFieldsResult {
    if request.task_id.is_some() || request.requirement_id.is_some() || request.title.is_some() {
        return Ok((
            request.task_id.clone(),
            request.requirement_id.clone(),
            request.title.clone(),
            request.description.clone(),
        ));
    }

    if let Some(dispatch) = &request.subject_dispatch {
        // rc.6: `SubjectDispatch.subject` is `Option<SubjectRef>` (subjectless
        // dispatch). Only project a selector when a subject is bound; a
        // subjectless dispatch falls through to the other selectors below.
        if let Some(subject) = &dispatch.subject {
            let kind = subject.kind();
            let id = subject.id().to_string();
            if kind.eq_ignore_ascii_case(animus_subject_protocol::subject_kind::TASK) {
                return Ok((Some(id), None, None, None));
            }
            if kind.eq_ignore_ascii_case(animus_subject_protocol::subject_kind::REQUIREMENT) {
                return Ok((None, Some(id), None, None));
            }
            // Generic kind — project as a custom subject.
            let title = subject.title.clone().unwrap_or_else(|| id.clone());
            let description = subject.description.clone().unwrap_or_default();
            return Ok((None, None, Some(title), Some(description)));
        }
    }

    if let Some(subject_ref) = &request.subject_ref {
        let kind = subject_ref.kind();
        let id = subject_ref.id().to_string();
        if kind.eq_ignore_ascii_case(animus_subject_protocol::subject_kind::TASK) {
            return Ok((Some(id), None, None, None));
        }
        if kind.eq_ignore_ascii_case(animus_subject_protocol::subject_kind::REQUIREMENT) {
            return Ok((None, Some(id), None, None));
        }
        let title = subject_ref.title.clone().unwrap_or_else(|| id.clone());
        let description = subject_ref.description.clone().unwrap_or_default();
        return Ok((None, None, Some(title), Some(description)));
    }

    Err("one of task_id / requirement_id / title / subject_ref / subject_dispatch must be set".to_string())
}

fn workflow_status_to_wire(status: WorkflowStatus) -> &'static str {
    match status {
        WorkflowStatus::Completed => workflow_status::COMPLETED,
        WorkflowStatus::Running => workflow_status::RUNNING,
        WorkflowStatus::Pending => workflow_status::PENDING,
        WorkflowStatus::Failed => workflow_status::FAILED,
        WorkflowStatus::Escalated => workflow_status::ESCALATED,
        WorkflowStatus::Cancelled => workflow_status::CANCELLED,
        WorkflowStatus::Paused => workflow_status::PAUSED,
    }
}

/// Implementation of `workflow/run_phase`. Runs exactly one phase through
/// the lifted `run_workflow_phase` function and returns the result snapshot.
pub async fn handle_workflow_run_phase(request: WorkflowPhaseRunRequest) -> Result<WorkflowPhaseRunResult> {
    let state = current_state()?;
    // Strict project-binding enforcement (codex P1 round 1 — initial draft
    // logged + continued, which broke the v0.5 isolation contract). If the
    // requested execution_cwd is not the bound project root or a subdirectory
    // (e.g. a worktree path), the plugin returns PROJECT_BINDING_MISMATCH
    // and the daemon must route the request to a plugin process bound to
    // the correct project.
    enforce_project_binding(&state, &request.execution_cwd)?;

    let project_root = state.project_root.display().to_string();
    let pipeline_vars = request.pipeline_vars.clone();
    let routing = request
        .phase_routing
        .clone()
        .and_then(|value| serde_json::from_value::<protocol::PhaseRoutingConfig>(value).ok())
        .unwrap_or_default();

    let overrides = crate::phase_executor::PhaseExecuteOverrides {
        tool: request.tool_override.clone(),
        model: request.model_override.clone(),
        rework_context: request.rework_context.clone(),
    };

    let task_complexity = request.task_complexity.as_deref().and_then(parse_task_complexity);

    // Per-call MCP runtime config (v0.2.0 protocol). Precedence:
    //   1. request.mcp_config (per-call override)
    //   2. None — runtime contract falls back to McpRuntimeConfig::default().
    // The plugin does not persist a workflow-level mcp_config (workflow/execute
    // carries it through `WorkflowExecuteInternalParams` only for the duration
    // of that call), so per-phase replays MUST resupply it on the wire.
    let mcp_config: Option<protocol::McpRuntimeConfig> =
        request.mcp_config.clone().and_then(|value| serde_json::from_value(value).ok());

    // REQUIREMENT-048 cross-phase broker: the daemon dispatches ONE runner
    // subprocess per phase, so a runner-owned per-phase node cannot share a
    // workspace across phases. When the daemon sets the broker env vars it owns
    // ONE node per workflow RUN behind a private socket; this per-phase entry
    // ACQUIREs a handle to that shared node and execs through it (no prepare, no
    // teardown — the daemon owns the lifecycle). Absent the broker env vars this
    // stays `None` (the byte-for-byte local path). A broker acquire failure fails
    // the phase and NEVER falls back to an owned prepare.
    // The run's target repo (the subject's `git_repo` custom field — the SAME
    // value a command phase renders as `{{git_repo}}`) rides the acquire spec's
    // `metadata.github_repo` so the environment plugin repo-scopes the minted
    // GitHub App installation token to THAT repo. The broker single-flights the
    // node per run, so this matters on the run's FIRST phase acquire; a bare run
    // without `git_repo` leaves the metadata untouched.
    let subject_git_repo =
        crate::phase_command::subject_git_repo(&project_root, "", &request.subject_id).await;
    let brokered_environment: Option<std::sync::Arc<crate::phase_environment::BrokeredEnvironment>> =
        match crate::phase_environment::BrokeredEnvironment::acquire_from_env(subject_git_repo).await {
            Some(result) => Some(std::sync::Arc::new(result.with_context(|| {
                format!("failed to acquire brokered environment for phase '{}'", request.phase_id)
            })?)),
            None => None,
        };
    let held_environment: Option<&dyn crate::phase_environment::HeldEnvironment> =
        brokered_environment.as_deref().map(|brokered| brokered as &dyn crate::phase_environment::HeldEnvironment);

    let started = std::time::Instant::now();
    let run_result = crate::phase_executor::run_workflow_phase(&crate::phase_executor::PhaseRunParams {
        project_root: &project_root,
        execution_cwd: &request.execution_cwd,
        workflow_id: &request.workflow_id,
        workflow_ref: &request.workflow_ref,
        subject_id: &request.subject_id,
        // `WorkflowPhaseRunRequest` carries no subject kind, so the id is left
        // as sent. When the daemon supplies an already-qualified id
        // (`<kind>:<native>`) the command phase resolves the right backend;
        // a bare id stays bare (unchanged pre-fix behavior).
        subject_kind: "",
        subject_title: &request.subject_title,
        subject_description: &request.subject_description,
        task_complexity,
        phase_id: &request.phase_id,
        phase_attempt: request.phase_attempt,
        overrides: Some(&overrides),
        pipeline_vars: if pipeline_vars.is_empty() { None } else { Some(&pipeline_vars) },
        dispatch_input: request.dispatch_input.as_deref(),
        schedule_input: request.schedule_input.as_deref(),
        routing: &routing,
        phase_timeout_secs: request.phase_timeout_secs,
        mcp_config: mcp_config.as_ref(),
        // Relay the transport-asserted actor verbatim from the inbound
        // phase-run request. The runner never synthesizes or interprets it.
        actor: request.actor.as_ref(),
        // The per-run environment node is owned by the daemon broker and reached
        // over a private socket (REQUIREMENT-048); `held_environment` is the
        // acquired handle when the broker env vars are set, else `None` (local).
        held_environment,
    })
    .await;
    let elapsed: Duration = started.elapsed();

    match run_result {
        Ok(result) => {
            let status = match &result.outcome {
                crate::phase_executor::PhaseExecutionOutcome::Completed { .. } => phase_status::COMPLETED.to_string(),
                crate::phase_executor::PhaseExecutionOutcome::ManualPending { .. } => {
                    phase_status::MANUAL_PENDING.to_string()
                }
            };
            Ok(WorkflowPhaseRunResult {
                phase_status: status,
                duration_secs: elapsed.as_secs(),
                outcome: serde_json::to_value(&result.outcome).unwrap_or(Value::Null),
                metadata: serde_json::to_value(&result.metadata).unwrap_or(Value::Null),
                signals: result.signals.into_iter().filter_map(|sig| serde_json::to_value(sig).ok()).collect(),
                model: result.model,
                tool: result.tool,
            })
        }
        Err(error) => Ok(WorkflowPhaseRunResult {
            phase_status: phase_status::FAILED.to_string(),
            duration_secs: elapsed.as_secs(),
            outcome: serde_json::json!({ "error": error.to_string() }),
            metadata: Value::Null,
            signals: Vec::new(),
            model: None,
            tool: None,
        }),
    }
}

fn parse_task_complexity(s: &str) -> Option<orchestrator_core::Complexity> {
    match s.to_ascii_lowercase().as_str() {
        // The lifted enum only knows Low/Medium/High; "minimal" and "critical"
        // collapse to the nearest bucket. Future versions of
        // `orchestrator_core::Complexity` MAY add finer-grained variants.
        "minimal" | "low" => Some(orchestrator_core::Complexity::Low),
        "medium" => Some(orchestrator_core::Complexity::Medium),
        "high" | "critical" => Some(orchestrator_core::Complexity::High),
        _ => None,
    }
}

/// Map an internal anyhow error to a structured `(code, message)` pair for
/// the JSON-RPC error envelope. Recognizes the `PROJECT_BINDING_MISMATCH`
/// prefix written by [`enforce_project_binding`].
pub fn classify_error(error: &anyhow::Error) -> (i32, String) {
    let message = error.to_string();
    if message.starts_with("PROJECT_BINDING_MISMATCH") {
        (error_codes::PROJECT_BINDING_MISMATCH, message)
    } else if message.starts_with("plugin not initialized") {
        (animus_plugin_protocol::error_codes::PLUGIN_NOT_INITIALIZED, message)
    } else {
        (animus_plugin_protocol::error_codes::INTERNAL_ERROR, message)
    }
}

// (No re-export of `ProtoPhaseEvent`; callers import directly from
// `animus_workflow_runner_protocol::PhaseEvent`.)

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    /// Build the raw `initialize` params Value the kernel sends, carrying
    /// `init_extensions.project_binding` (the typed `InitializeParams` no
    /// longer carries `init_extensions`, so the plugin reads it from the raw
    /// params — see [`plugin_initialize_result`]).
    fn make_init(project_root: &str) -> Value {
        json!({
            "protocol_version": PLUGIN_PROTOCOL_VERSION,
            "host_info": { "name": "test-host", "version": "0.0.0" },
            "capabilities": { "progress": false, "cancellation": false, "streaming": false },
            "init_extensions": {
                PROJECT_BINDING_EXTENSION: { "project_root": project_root },
            },
        })
    }

    #[test]
    fn manifest_lists_two_methods() {
        let manifest = plugin_manifest();
        assert_eq!(manifest.plugin_kind, "workflow_runner");
        assert_eq!(manifest.capabilities.len(), 2);
        assert!(manifest.capabilities.contains(&"workflow/execute".to_string()));
        assert!(manifest.capabilities.contains(&"workflow/run_phase".to_string()));
        // No hyphenated method names:
        for method in &manifest.capabilities {
            assert!(!method.contains('-'), "method name '{method}' must not contain '-'");
        }
    }

    #[test]
    fn initialize_requires_project_binding() {
        let mut params = make_init("/tmp/whatever");
        params.as_object_mut().unwrap().remove("init_extensions");
        let result = plugin_initialize_result(&params);
        assert!(result.is_err(), "initialize without project_binding must error");
    }

    #[test]
    fn project_binding_mismatch_is_classified() {
        let err = anyhow!("PROJECT_BINDING_MISMATCH: bound to /a but request implied /b");
        let (code, _msg) = classify_error(&err);
        assert_eq!(code, error_codes::PROJECT_BINDING_MISMATCH);
    }

    #[test]
    fn workflow_status_to_wire_emits_paused_vocabulary() {
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Paused), workflow_status::PAUSED);
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Paused), "paused");
    }

    #[test]
    fn workflow_status_to_wire_emits_pending_vocabulary() {
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Pending), workflow_status::PENDING);
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Pending), "pending");
    }

    #[test]
    fn workflow_status_to_wire_preserves_existing_states() {
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Completed), workflow_status::COMPLETED);
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Running), workflow_status::RUNNING);
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Failed), workflow_status::FAILED);
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Escalated), workflow_status::ESCALATED);
        assert_eq!(workflow_status_to_wire(WorkflowStatus::Cancelled), workflow_status::CANCELLED);
    }

    fn make_phase_run_request(mcp_config: Option<Value>) -> WorkflowPhaseRunRequest {
        WorkflowPhaseRunRequest {
            execution_cwd: "/tmp/proj".to_string(),
            workflow_id: "wf-1".to_string(),
            workflow_ref: "default".to_string(),
            subject_id: "task:TASK-1".to_string(),
            subject_title: "title".to_string(),
            subject_description: "desc".to_string(),
            phase_id: "implementation".to_string(),
            phase_attempt: 0,
            phase_timeout_secs: None,
            model_override: None,
            tool_override: None,
            task_complexity: None,
            rework_context: None,
            pipeline_vars: HashMap::new(),
            dispatch_input: None,
            schedule_input: None,
            phase_routing: None,
            mcp_config,
            actor: None,
        }
    }

    #[test]
    fn workflow_run_phase_request_parses_mcp_config_when_present() {
        let request = make_phase_run_request(Some(json!({
            "stdio_command": "/opt/host/bin/host-mcp",
            "stdio_args_json": "[\"--host\"]",
            "agent_id": "host-agent"
        })));
        let parsed: Option<protocol::McpRuntimeConfig> =
            request.mcp_config.clone().and_then(|value| serde_json::from_value(value).ok());
        let parsed = parsed.expect("WorkflowPhaseRunRequest.mcp_config must parse into McpRuntimeConfig");
        assert_eq!(parsed.stdio_command.as_deref(), Some("/opt/host/bin/host-mcp"));
        assert_eq!(parsed.stdio_args_json.as_deref(), Some("[\"--host\"]"));
        assert_eq!(parsed.agent_id.as_deref(), Some("host-agent"));
    }

    #[test]
    fn workflow_run_phase_request_without_mcp_config_falls_back_to_none() {
        let request = make_phase_run_request(None);
        let parsed: Option<protocol::McpRuntimeConfig> =
            request.mcp_config.clone().and_then(|value| serde_json::from_value(value).ok());
        assert!(parsed.is_none(), "absent mcp_config must surface as None so the runtime contract uses defaults");
    }

    #[test]
    fn workflow_run_phase_request_round_trips_mcp_config_through_serde() {
        let original = make_phase_run_request(Some(json!({
            "stdio_command": "/usr/bin/mcp",
            "agent_id": "agent-x"
        })));
        let serialized = serde_json::to_value(&original).expect("serialize");
        assert!(serialized.get("mcp_config").is_some(), "mcp_config must round-trip on the wire");
        let back: WorkflowPhaseRunRequest = serde_json::from_value(serialized).expect("deserialize");
        assert_eq!(back.mcp_config, original.mcp_config);
    }
}
