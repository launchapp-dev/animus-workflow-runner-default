//! In-process accumulator for protocol-shaped `PhaseEvent`s, plus JSONL
//! persistence to `<project_root>/.animus/workflow-events/<run_id>.jsonl`.
//!
//! v0.5 lift-and-shift change: the legacy in-process `PhaseEventCallback`
//! closure on `WorkflowExecuteParams` is gone. The plugin entrypoint
//! constructs a [`PhaseEventRecorder`] before calling
//! `execute_workflow_with_hub`, and the `workflow/execute` JSON-RPC response
//! carries the recorded events back to the daemon as
//! `WorkflowExecuteResult::phase_events`.
//!
//! Per the v0.5 protocol spec "Known limitations" section, the recorder
//! also persists every `Decision` event and every `Completed { status:
//! "manual_pending" }` event to a per-run JSONL file. If the plugin crashes
//! mid-workflow, the in-memory vector is lost but the durable JSONL stream
//! survives for post-crash inspection.

use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use animus_workflow_runner_protocol::PhaseEvent;
use chrono::Utc;

use animus_runtime_shared::workflow_event_emitter::{
    RuntimeWorkflowEvent, RuntimeWorkflowEventKind, WorkflowEventEmitter,
};

/// Accumulates protocol-shaped `PhaseEvent`s (returned to the daemon) and
/// mirrors a `WorkflowEventEmitter` for the lifted internal call sites.
///
/// Each emitted event is appended to an in-memory vector. `Decision` and
/// `manual_pending` events are additionally written to
/// `<project_root>/.animus/workflow-events/<run_id>.jsonl` as a one-line
/// JSON record so post-crash investigators can replay critical decision
/// points even though the runner does not keep a workflow-event database
/// in v0.5.
pub struct PhaseEventRecorder {
    project_root: PathBuf,
    inner: Mutex<RecorderState>,
}

struct RecorderState {
    events: Vec<PhaseEvent>,
}

impl PhaseEventRecorder {
    pub fn new(project_root: impl Into<PathBuf>) -> Self {
        Self { project_root: project_root.into(), inner: Mutex::new(RecorderState { events: Vec::new() }) }
    }

    /// Take ownership of the accumulated `PhaseEvent`s. Returns an empty vec
    /// if called twice (the recorder is single-shot).
    pub fn take_events(&self) -> Vec<PhaseEvent> {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut guard.events)
    }

    /// Append a `PhaseEvent` to the in-memory vector. If the event is a
    /// `Decision` or a `manual_pending` `Completed`, also persist a JSONL
    /// line to `<project_root>/.animus/workflow-events/<run_id>.jsonl`.
    pub fn record(&self, run_id: &str, event: PhaseEvent) {
        let persist = match &event {
            PhaseEvent::Decision { .. } => true,
            PhaseEvent::Completed { status, .. } if status == "manual_pending" => true,
            _ => false,
        };
        if persist {
            self.persist_jsonl_line(run_id, &event);
        }
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        guard.events.push(event);
    }

    fn persist_jsonl_line(&self, run_id: &str, event: &PhaseEvent) {
        let dir = self.project_root.join(".animus").join("workflow-events");
        if let Err(error) = create_dir_all(&dir) {
            tracing::warn!(?error, dir = %dir.display(), "failed to create workflow-events dir");
            return;
        }
        let path = dir.join(format!("{run_id}.jsonl"));
        let line = match serde_json::to_string(event) {
            Ok(mut s) => {
                s.push('\n');
                s
            }
            Err(error) => {
                tracing::warn!(?error, "failed to serialize PhaseEvent for jsonl");
                return;
            }
        };
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(mut file) => {
                if let Err(error) = file.write_all(line.as_bytes()) {
                    tracing::warn!(?error, path = %path.display(), "failed to append workflow-events jsonl");
                }
            }
            Err(error) => {
                tracing::warn!(?error, path = %path.display(), "failed to open workflow-events jsonl");
            }
        }
    }
}

impl PhaseEventRecorder {
    fn run_id_from(event: &RuntimeWorkflowEvent) -> &str {
        &event.workflow_id
    }
}

/// Adapts a `PhaseEventRecorder` to the internal `WorkflowEventEmitter`
/// trait so existing call sites in `workflow_execute.rs` can continue to
/// call `emitter.emit(RuntimeWorkflowEvent { ... })` without changes.
impl WorkflowEventEmitter for PhaseEventRecorder {
    fn emit(&self, event: RuntimeWorkflowEvent) {
        let run_id = Self::run_id_from(&event).to_string();
        let ts = Utc::now().to_rfc3339();
        if let Some(proto_event) = runtime_event_to_phase_event(&event, &ts) {
            self.record(&run_id, proto_event);
        }
    }
}

/// Best-effort conversion of an internal `RuntimeWorkflowEvent` into the
/// protocol `PhaseEvent`. Workflow-terminal events
/// (`WorkflowCompleted` / `WorkflowFailed`) are not phase events and are
/// dropped. The internal `RuntimeWorkflowEventKind::PhaseStarted` carries
/// a `payload.phase_id` and `payload.phase_index`; we lift those out.
fn runtime_event_to_phase_event(event: &RuntimeWorkflowEvent, ts: &str) -> Option<PhaseEvent> {
    let phase_id = event.payload.get("phase_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if phase_id.is_empty() {
        return None;
    }
    match event.kind {
        RuntimeWorkflowEventKind::PhaseStarted => Some(PhaseEvent::Started {
            phase_id,
            attempt: event.payload.get("phase_attempt").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
            ts: ts.to_string(),
        }),
        RuntimeWorkflowEventKind::PhaseCompleted => {
            // PhaseCompleted is overloaded: a `phase_status: "decision"`
            // payload (emitted alongside the actual completion) carries
            // the decision verdict + confidence and maps to the protocol
            // `Decision` variant; everything else maps to `Completed`.
            let status = event.payload.get("phase_status").and_then(|v| v.as_str()).unwrap_or("completed");
            if status == "decision" {
                Some(PhaseEvent::Decision {
                    phase_id,
                    verdict: event.payload.get("verdict").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    confidence: event.payload.get("confidence").and_then(|v| v.as_f64()).map(|f| f as f32),
                    ts: ts.to_string(),
                })
            } else {
                Some(PhaseEvent::Completed { phase_id, status: status.to_string(), ts: ts.to_string() })
            }
        }
        RuntimeWorkflowEventKind::PhaseFailed => {
            Some(PhaseEvent::Completed { phase_id, status: "failed".to_string(), ts: ts.to_string() })
        }
        RuntimeWorkflowEventKind::WorkflowCompleted | RuntimeWorkflowEventKind::WorkflowFailed => None,
    }
}

/// Resolve the JSONL path for a given run id, for tests + the daemon.
pub fn workflow_events_path(project_root: &Path, run_id: &str) -> PathBuf {
    project_root.join(".animus").join("workflow-events").join(format!("{run_id}.jsonl"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    #[test]
    fn recorder_accumulates_events() {
        let tmp = TempDir::new().unwrap();
        let recorder = PhaseEventRecorder::new(tmp.path());
        recorder.record(
            "wf-1",
            PhaseEvent::Started { phase_id: "impl".into(), attempt: 0, ts: "2026-05-31T00:00:00Z".into() },
        );
        recorder.record(
            "wf-1",
            PhaseEvent::Decision {
                phase_id: "impl".into(),
                verdict: "advance".into(),
                confidence: Some(0.9),
                ts: "2026-05-31T00:00:01Z".into(),
            },
        );
        let events = recorder.take_events();
        assert_eq!(events.len(), 2);
        // Second take returns empty.
        assert!(recorder.take_events().is_empty());
    }

    #[test]
    fn recorder_persists_decision_to_jsonl() {
        let tmp = TempDir::new().unwrap();
        let recorder = PhaseEventRecorder::new(tmp.path());
        recorder.record(
            "wf-jsonl",
            PhaseEvent::Decision {
                phase_id: "impl".into(),
                verdict: "advance".into(),
                confidence: None,
                ts: "2026-05-31T00:00:00Z".into(),
            },
        );
        let path = workflow_events_path(tmp.path(), "wf-jsonl");
        let body = std::fs::read_to_string(&path).expect("jsonl file written");
        assert!(body.contains("\"kind\":\"decision\""));
        assert!(body.contains("\"verdict\":\"advance\""));
    }

    #[test]
    fn recorder_persists_manual_pending() {
        let tmp = TempDir::new().unwrap();
        let recorder = PhaseEventRecorder::new(tmp.path());
        recorder.record(
            "wf-mp",
            PhaseEvent::Completed {
                phase_id: "approval".into(),
                status: "manual_pending".into(),
                ts: "2026-05-31T00:00:00Z".into(),
            },
        );
        let path = workflow_events_path(tmp.path(), "wf-mp");
        let body = std::fs::read_to_string(&path).expect("jsonl file written");
        assert!(body.contains("manual_pending"));
    }

    #[test]
    fn recorder_does_not_persist_started() {
        let tmp = TempDir::new().unwrap();
        let recorder = PhaseEventRecorder::new(tmp.path());
        recorder.record(
            "wf-started",
            PhaseEvent::Started { phase_id: "impl".into(), attempt: 0, ts: "2026-05-31T00:00:00Z".into() },
        );
        let path = workflow_events_path(tmp.path(), "wf-started");
        assert!(!path.exists(), "started events should not be persisted");
    }
}
