//! Generic sink for workflow lifecycle events surfaced by
//! [`crate::workflow_execute::execute_workflow`].
//!
//! v0.5 lift-and-shift change: the legacy `SubprocessPipeEmitter` /
//! `ANIMUS_WORKFLOW_EVENT_PIPE` back-channel was removed. The plugin process
//! lives behind a JSON-RPC stdio boundary and returns its phase events as a
//! `Vec<PhaseEvent>` field on the `workflow/execute` response (see
//! [`crate::phase_event_recorder`]). The `WorkflowEventEmitter` trait survives
//! because internal callers (workflow_execute, integration tests) still emit
//! events through it; the daemon collects them by inspecting the response
//! payload, not by listening on a side pipe.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Kind discriminator for a [`RuntimeWorkflowEvent`].
///
/// Mirrors the `kind` string values the protocol layer emits on the wire
/// (`workflow_events`). Kept as an enum here so the runner cannot
/// mis-spell a kind; the emitter implementation maps to the wire string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeWorkflowEventKind {
    PhaseStarted,
    PhaseCompleted,
    PhaseFailed,
    WorkflowCompleted,
    WorkflowFailed,
}

impl RuntimeWorkflowEventKind {
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::PhaseStarted => "phase_started",
            Self::PhaseCompleted => "phase_completed",
            Self::PhaseFailed => "phase_failed",
            Self::WorkflowCompleted => "workflow_completed",
            Self::WorkflowFailed => "workflow_failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeWorkflowEvent {
    pub workflow_id: String,
    pub kind: RuntimeWorkflowEventKind,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

/// Wire form retained for back-compat with any tooling that previously
/// consumed JSONL lines emitted by the deprecated subprocess pipe. The
/// in-tree daemon should consume events from the JSON-RPC response now.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireWorkflowEvent {
    pub workflow_id: String,
    pub kind: String,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

impl From<&RuntimeWorkflowEvent> for WireWorkflowEvent {
    fn from(event: &RuntimeWorkflowEvent) -> Self {
        Self {
            workflow_id: event.workflow_id.clone(),
            kind: event.kind.as_wire_str().to_string(),
            payload: event.payload.clone(),
            occurred_at: event.occurred_at,
        }
    }
}

pub trait WorkflowEventEmitter: Send + Sync {
    fn emit(&self, event: RuntimeWorkflowEvent);
}

pub type SharedWorkflowEventEmitter = Arc<dyn WorkflowEventEmitter>;

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopWorkflowEventEmitter;

impl WorkflowEventEmitter for NoopWorkflowEventEmitter {
    fn emit(&self, _event: RuntimeWorkflowEvent) {}
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct RecordingEmitter {
        events: Mutex<Vec<RuntimeWorkflowEvent>>,
    }

    impl RecordingEmitter {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        pub fn snapshot(&self) -> Vec<RuntimeWorkflowEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl WorkflowEventEmitter for RecordingEmitter {
        fn emit(&self, event: RuntimeWorkflowEvent) {
            self.events.lock().unwrap().push(event);
        }
    }
}
