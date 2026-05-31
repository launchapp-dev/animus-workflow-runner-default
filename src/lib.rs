//! `animus-workflow-runner-default` — reference `workflow_runner` plugin for
//! Animus v0.5. Lift-and-shift of the in-tree `workflow-runner-v2` crate
//! wrapped with the JSON-RPC stdio plugin contract from
//! `animus-workflow-runner-protocol`.
//!
//! See `docs/architecture/v0.5-protocol-specs.md` §1 in the kernel repo for
//! the canonical wire contract.

pub mod agent_state;
pub mod config_context;
pub mod direct_exec;
pub mod ensure_execution_cwd;
// `ipc` here is the agent-runner Unix-socket bridge used by `phase_executor`
// to dispatch agent processes — it is NOT the plugin-host JSON-RPC stdio
// boundary. The kernel-extraction-v0.5.md brief's "delete ipc.rs" guidance
// targeted a hypothetical workflow-runner subprocess pipe; the actual file
// is load-bearing for in-process phase execution and is retained without
// `pub use ipc::*` (only used internally by `phase_executor`).
pub(crate) mod ipc;
pub mod metrics_hook;
pub mod notification_log;
pub mod payload_traversal;
pub mod phase_command;
pub mod phase_event_recorder;
pub mod phase_executor;
pub mod phase_failover;
pub mod phase_git;
pub mod phase_output;
pub mod phase_prompt;
pub mod phase_session;
pub mod phase_targets;
pub mod plugin;
pub mod runtime_contract;
pub mod runtime_support;
pub mod skill_dispatch;
pub mod workflow_event_emitter;
pub mod workflow_execute;
pub mod workflow_helpers;
pub mod workflow_merge_recovery;

pub use agent_state::{
    append_agent_memory, clear_agent_memory, delete_agent_memory_entry, list_agent_messages, load_agent_memory,
    send_agent_message, AgentMemoryDocument, AgentMemoryEntry, AgentMessage,
};
pub use ensure_execution_cwd::ensure_execution_cwd;
pub use payload_traversal::{
    fallback_implementation_commit_message, parse_commit_message_from_text, parse_phase_decision_from_text,
};
pub use phase_event_recorder::{workflow_events_path, PhaseEventRecorder};
pub use phase_executor::{
    load_agent_runtime_config, run_workflow_phase, CliPhaseExecutor, PhaseExecuteOverrides, PhaseExecutionMetadata,
    PhaseExecutionOutcome, PhaseExecutionSignal, PhaseRunParams, PhaseRunResult,
};
pub use phase_failover::{classify_phase_failure, PhaseFailureClassifier, PhaseFailureKind};
pub use phase_git::{commit_implementation_changes, ensure_git_identity, git_has_pending_changes, is_git_repo};
pub use phase_output::{
    is_phase_completed, persist_phase_output, persist_resumed_phase_completion, phase_completion_marker_path,
    phase_output_dir, read_persisted_decision, write_phase_completion_marker, PersistedDecisionReadError,
    PersistedPhaseOutput, PhaseCompletionMarker,
};
pub use phase_prompt::{
    build_phase_prompt, phase_requires_commit_message, phase_requires_commit_message_with_config, render_phase_prompt,
    PhasePromptInputs, PhasePromptParams, PhaseRenderParams, RenderedPhasePrompt,
};
pub use phase_targets::PhaseTargetPlanner;
pub use plugin::{
    handle_workflow_execute, handle_workflow_run_phase, plugin_initialize_result, plugin_manifest, PluginState,
    PROJECT_BINDING_EXTENSION,
};
pub use runtime_support::*;
pub use workflow_event_emitter::{
    NoopWorkflowEventEmitter, RuntimeWorkflowEvent, RuntimeWorkflowEventKind, SharedWorkflowEventEmitter,
    WireWorkflowEvent, WorkflowEventEmitter,
};
// `WorkflowExecuteParams` is an internal type (kept `pub(crate)` in
// `workflow_execute.rs`); the publicly callable surface is
// `handle_workflow_execute` (wire) and `execute_workflow_with_hub` (in-process
// integration tests).
pub use workflow_execute::execute_workflow_with_hub;
pub use workflow_helpers::{
    task_requires_research, workflow_has_active_research, workflow_has_completed_research, PhaseExecutionEvent,
};
pub use workflow_merge_recovery::{
    block_reason_sideeffecting, block_reason_unknown, classify_phase_recovery, phase_idempotency_for,
    MergeConflictContext, PhaseRecoveryAction,
};

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Returns the per-process test home directory and pins HOME to it on first call.
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

    /// Process-wide lock for tests that depend on `protocol::scoped_state_root`. Hold the guard
    /// for the entirety of the test body. Diagnosed cause: under parallel cargo-test execution,
    /// scope-dir state in `~/.animus/.../` accumulates from many concurrent tempdirs and triggers
    /// `find_existing_scope_by_origin` collisions / partial state visibility that flips
    /// scoped_state_root's resolved path between writes and reads. Serializing avoids the race.
    pub fn scoped_state_serializer() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        stable_test_home();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
