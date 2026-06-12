//! `animus-workflow-runner-default` — reference `workflow_runner` plugin for
//! Animus v0.5. Plugin-private modules (`phase_executor`, `workflow_execute`,
//! `phase_targets`, `phase_failover`, `phase_command`, `skill_dispatch`,
//! `git_provider`, `direct_execute`, `phase_event_recorder`, `plugin`) live
//! here; the shared runtime modules (`agent_state`, `config_context`, `ipc`,
//! `phase_session`, `phase_output`, `phase_prompt`, `phase_git`, `reattach`,
//! `runtime_contract`, `runtime_support`, `workflow_event_emitter`,
//! `workflow_helpers`, `workflow_merge_recovery`, `metrics_hook`,
//! `notification_log`, `payload_traversal`, `ensure_execution_cwd`,
//! `phase_metadata`) live in `animus-runtime-shared` and are re-exported
//! below.

pub mod git_provider;
pub mod phase_command;
pub mod phase_event_recorder;
pub mod phase_executor;
pub mod phase_failover;
pub mod phase_targets;
pub mod plugin;
pub mod skill_dispatch;
pub mod workflow_execute;

// Re-export every module from animus-runtime-shared so existing
// `animus_workflow_runner_default::<module>::*` imports keep working.
pub use animus_runtime_shared::{
    agent_state, config_context, ensure_execution_cwd, ipc, metrics_hook, notification_log, payload_traversal,
    phase_git, phase_metadata, phase_output, phase_prompt, phase_session, reattach, runtime_contract, runtime_support,
    workflow_event_emitter, workflow_helpers, workflow_merge_recovery,
};

pub use animus_runtime_shared::{
    append_agent_memory, block_reason_sideeffecting, block_reason_unknown, build_phase_prompt, classify_phase_recovery,
    clear_agent_memory, commit_implementation_changes, delete_agent_memory_entry,
    ensure_execution_cwd as _ensure_execution_cwd, ensure_git_identity, fallback_implementation_commit_message,
    git_has_pending_changes, install_memory_mcp_stdio_command_override, is_git_repo, is_phase_completed,
    list_agent_messages, load_agent_memory, parse_commit_message_from_text, parse_phase_decision_from_text,
    persist_phase_output, persist_resumed_phase_completion, phase_completion_marker_path, phase_idempotency_for,
    phase_output_dir, phase_requires_commit_message, phase_requires_commit_message_with_config,
    phase_requires_commit_message_with_ctx, phase_result_kind_for_ctx, read_persisted_decision, render_phase_prompt,
    render_phase_prompt_with_ctx, render_phase_prompt_with_ctx_overrides, send_agent_message, task_requires_research,
    validate_basic_json_schema, workflow_has_active_research, workflow_has_completed_research,
    write_phase_completion_marker, AgentMemoryDocument, AgentMemoryEntry, AgentMessage, FanoutEmitter,
    MergeConflictContext, NoopWorkflowEventEmitter, PersistedDecisionReadError, PersistedPhaseOutput,
    PhaseCompletionMarker, PhaseExecutionEvent, PhaseExecutionMetadata, PhaseExecutionOutcome, PhaseExecutionSignal,
    PhasePromptInputs, PhasePromptParams, PhaseRecoveryAction, PhaseRenderParams, RenderedPhasePrompt,
    RuntimeWorkflowEvent, RuntimeWorkflowEventKind, SharedWorkflowEventEmitter, SubprocessPipeEmitter,
    WireWorkflowEvent, WorkflowEventEmitter, ANIMUS_WORKFLOW_EVENT_PIPE_ENV,
};

pub use phase_event_recorder::{workflow_events_path, PhaseEventRecorder};
pub use phase_executor::{
    load_agent_runtime_config, run_workflow_phase, CliPhaseExecutor, PhaseExecuteOverrides, PhaseRunParams,
    PhaseRunResult,
};
pub use phase_failover::{classify_phase_failure, PhaseFailureClassifier, PhaseFailureKind};
pub use phase_targets::PhaseTargetPlanner;
pub use plugin::{
    handle_workflow_execute, handle_workflow_run_phase, plugin_initialize_result, plugin_manifest, PluginState,
    PROJECT_BINDING_EXTENSION,
};
pub use workflow_execute::{execute_workflow_with_hub, WorkflowExecuteInternalParams};

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    pub fn stable_test_home() -> &'static std::path::Path {
        static HOME: OnceLock<std::path::PathBuf> = OnceLock::new();
        HOME.get_or_init(|| {
            let home_dir = std::env::temp_dir()
                .join(format!("animus-workflow-runner-default-test-home-{}", std::process::id()))
                .join("home");
            std::fs::create_dir_all(&home_dir).expect("create shared workflow-runner-default test home");
            std::env::set_var("HOME", &home_dir);
            home_dir
        })
    }

    pub fn scoped_state_serializer() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        stable_test_home();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
