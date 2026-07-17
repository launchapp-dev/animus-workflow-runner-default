//! REQUIREMENT-052: whole-workflow delegation to a session-capable environment
//! (the "remote-animus session").
//!
//! [`phase_environment`](crate::phase_environment) (REQUIREMENT-048) routes EACH
//! PHASE's harness command into a per-run node that the HOME runner still drives
//! phase-by-phase (`prepare` once, then `exec_stream`/`exec` per phase). This
//! module implements the opposite, coarser handoff the product design asks for:
//!
//! > "The node is a FULL animus that runs the whole workflow standalone and
//! > streams. From our perspective it should be just ONE workflow (`coding`)
//! > that has `environment = animus-environment-railway`."
//!
//! When the resolved `environment` plugin advertises the `environment/exec_session`
//! method (see [`capabilities_advertise_exec_session`]), it is a full standalone
//! animus: the runner hands it the ENTIRE workflow in ONE
//! [`EnvironmentClient::exec_session`] call. The node brings up its own daemon,
//! runs every phase through its OWN provider/journal layer, and streams
//! `environment/journal` notifications home. The runner forwards each journal
//! event through its normal [`WorkflowEventEmitter`](crate::workflow_event_emitter)
//! (so the home journal/UI sees phase progress exactly as for a local run) and
//! drives the run to terminal from the session's terminal
//! [`ExecSessionResponse`]. It executes NO phase itself and prepares NO per-phase
//! held node -- this REPLACES the REQUIREMENT-048 per-phase path for a
//! session-capable environment.
//!
//! ## Feature gate: `remote-animus-session`
//!
//! The parent-side `EnvironmentClient::exec_session` + `EnvironmentJournalEvent`
//! surface (orchestrator-core, REQ-052) and the `environment/exec_session` /
//! `environment/journal` protocol constants (animus-environment-protocol) are
//! NOT present in the runner's current pinned `orchestrator-core` rev
//! (`d03cd174`, the exec_session commit's parent) nor in the pinned
//! `animus-environment-protocol` tag (`v0.7.0-rc.6`). Until those pins are
//! advanced, the actual delegation call is compiled ONLY under the
//! `remote-animus-session` cargo feature. With the feature OFF (the default)
//! this module is behaviorally inert: [`environment_is_session_capable`] always
//! returns `false`, so `execute_workflow_with_hub` keeps the byte-for-byte
//! current per-phase / local paths. With the feature ON (node/CI builds against
//! a REQ-052 orchestrator-core rev + an environment-protocol tag that ship
//! `exec_session`), detection goes live and delegation engages.
//!
//! The PURE decision logic ([`capabilities_advertise_exec_session`],
//! [`map_journal_event_kind`], [`session_status_to_workflow_status`]) is compiled
//! and unit-tested unconditionally, so the mechanism is reviewable without the
//! feature.

use std::path::Path;

use anyhow::Result;
use orchestrator_core::WorkflowStatus;

use crate::workflow_event_emitter::{RuntimeWorkflowEventKind, SharedWorkflowEventEmitter};
use crate::workflow_execute::WorkflowExecuteInternalResult;

/// The `environment/exec_session` method id (REQ-052). Mirrors
/// `animus_environment_protocol::METHOD_ENVIRONMENT_EXEC_SESSION`, inlined so the
/// capability probe compiles against environment-protocol tags that predate the
/// const (the method + const land together with the `remote-animus-session`
/// feature's protocol bump). Keep in sync with the protocol crate.
pub(crate) const METHOD_ENVIRONMENT_EXEC_SESSION: &str = "environment/exec_session";

/// Plugin-kind discriminator for environment plugins. Mirrors
/// `animus_plugin_protocol::PLUGIN_KIND_ENVIRONMENT` (a stable wire string).
#[cfg(feature = "remote-animus-session")]
const PLUGIN_KIND_ENVIRONMENT: &str = "environment";

/// Whether a plugin manifest's advertised method list (`capabilities`) contains
/// `environment/exec_session` -- i.e. the environment can run a WHOLE workflow on
/// its own animus (REQ-052), as opposed to only serving the per-phase
/// `environment/exec` / `environment/exec_stream` surface (REQ-048).
///
/// This is the session-capability detection mechanism (investigation Q3): a
/// session-capable environment plugin declares `environment/exec_session` in the
/// `capabilities: Vec<String>` field of its `PluginManifest` ("Methods
/// implemented by the plugin"), discovered at install/spawn time.
#[cfg_attr(not(feature = "remote-animus-session"), allow(dead_code))]
pub(crate) fn capabilities_advertise_exec_session(capabilities: &[String]) -> bool {
    capabilities.iter().any(|method| method == METHOD_ENVIRONMENT_EXEC_SESSION)
}

/// Map a node-local journal event kind to the runner's coarse workflow-event
/// kind, forwarded home through the [`WorkflowEventEmitter`](crate::workflow_event_emitter).
///
/// Only PHASE-lifecycle events map: the single TERMINAL workflow event is emitted
/// by the driver from the session's [`ExecSessionResponse`] status (so it is not
/// double-emitted), and finer node events (output chunks, tool calls) have no
/// home-side lifecycle counterpart and are dropped from the coarse stream. Pure;
/// unit-tested.
#[cfg_attr(not(feature = "remote-animus-session"), allow(dead_code))]
pub(crate) fn map_journal_event_kind(event_kind: &str) -> Option<RuntimeWorkflowEventKind> {
    match event_kind {
        "phase_started" => Some(RuntimeWorkflowEventKind::PhaseStarted),
        "phase_completed" => Some(RuntimeWorkflowEventKind::PhaseCompleted),
        "phase_failed" => Some(RuntimeWorkflowEventKind::PhaseFailed),
        _ => None,
    }
}

/// Map the node's terminal `ExecSessionResponse.status` string onto the home
/// [`WorkflowStatus`]. Unknown / unrecognized statuses fail CLOSED (`Failed`) so
/// a node that reports an unexpected terminal state never masquerades as a
/// success. Pure; unit-tested.
#[cfg_attr(not(feature = "remote-animus-session"), allow(dead_code))]
pub(crate) fn session_status_to_workflow_status(status: &str) -> WorkflowStatus {
    match status.trim().to_ascii_lowercase().as_str() {
        "completed" | "complete" | "success" | "succeeded" | "done" => WorkflowStatus::Completed,
        "escalated" => WorkflowStatus::Escalated,
        "cancelled" | "canceled" => WorkflowStatus::Cancelled,
        "paused" => WorkflowStatus::Paused,
        _ => WorkflowStatus::Failed,
    }
}

/// Agent-run transcript event kinds -- the fine-grained session stream that
/// carries a [`protocol::AgentRunEvent`] payload and must be MIRRORED into the
/// parent run dir (so the daemon's log_storage supervisor offloads it under the
/// parent workflow id, the id the portal's `/api/workflows/<id>/logs` reads).
///
/// Workflow LIFECYCLE kinds (`phase_*`, `run_*`, `workflow_*`) are deliberately
/// EXCLUDED: they reach the parent journal via the upstream backend proxy, so
/// mirroring them here would double-journal. Pure; unit-tested.
#[cfg_attr(not(feature = "remote-animus-session"), allow(dead_code))]
pub(crate) fn is_transcript_event_kind(event_kind: &str) -> bool {
    matches!(
        event_kind,
        "output_chunk" | "tool_call" | "tool_result" | "thinking" | "started" | "finished" | "metadata" | "error" | "artifact"
    )
}

/// Re-key a node-local agent-run `run_id` onto the PARENT workflow id so the
/// portal groups the mirrored transcript under the parent run (the log read path
/// matches a `wf-<workflow_id>-` prefix). Swaps the node workflow id in place
/// when it is present in the run id (preserving the `-<phase>-<attempt>-...`
/// suffix); otherwise prefixes the parent id. Pure; unit-tested.
#[cfg_attr(not(feature = "remote-animus-session"), allow(dead_code))]
pub(crate) fn rekey_transcript_run_id(orig_run_id: &str, node_workflow_id: &str, parent_workflow_id: &str) -> String {
    if !node_workflow_id.is_empty() && orig_run_id.contains(node_workflow_id) {
        orig_run_id.replacen(node_workflow_id, parent_workflow_id, 1)
    } else if let Some(rest) = orig_run_id.strip_prefix("wf-") {
        format!("wf-{parent_workflow_id}-{rest}")
    } else {
        format!("wf-{parent_workflow_id}-{orig_run_id}")
    }
}

/// Mirror one relayed transcript event into the PARENT run dir's `events.jsonl`
/// (re-keyed to the parent workflow id) so the daemon's log_storage supervisor
/// offloads it to the same store the portal reads. Non-transcript events and
/// malformed payloads are ignored; a persist error is swallowed so it never
/// disturbs the delegated run.
#[cfg(feature = "remote-animus-session")]
fn persist_session_transcript(
    project_root: &str,
    parent_workflow_id: &str,
    event: &orchestrator_core::EnvironmentJournalEvent,
) {
    if !is_transcript_event_kind(&event.event_kind) {
        return;
    }
    let mut payload = event.payload.clone();
    let Some(obj) = payload.as_object_mut() else {
        return;
    };
    let Some(orig_run_id) = obj.get("run_id").and_then(|value| value.as_str()).map(str::to_string) else {
        return;
    };
    let node_workflow_id = event.workflow_id.as_deref().unwrap_or_default();
    let new_run_id = rekey_transcript_run_id(&orig_run_id, node_workflow_id, parent_workflow_id);
    obj.insert("run_id".to_string(), serde_json::Value::String(new_run_id.clone()));
    let Ok(agent_event) = serde_json::from_value::<protocol::AgentRunEvent>(payload) else {
        return;
    };
    let dir = crate::ipc::run_dir(project_root, &protocol::RunId(new_run_id), None);
    let _ = crate::ipc::persist_run_event(&dir, &agent_event);
}

/// Whether the environment `environment_id` is session-capable -- it advertises
/// `environment/exec_session` (REQ-052) -- so the whole workflow should be
/// delegated to it rather than run phase-by-phase.
///
/// Selection mirrors [`orchestrator_core::EnvironmentClient::resolve`]: an exact
/// match on the discovered plugin `name` wins; failing that, the sole installed
/// environment plugin is used. Any discovery error, a missing plugin, or an
/// ambiguous-no-match resolution yields `false` (fail safe -> the caller keeps
/// the REQUIREMENT-048 per-phase path).
///
/// With the `remote-animus-session` feature OFF this always returns `false`, so
/// the runner's behavior is byte-for-byte unchanged (see the module docs).
#[cfg(feature = "remote-animus-session")]
pub(crate) fn environment_is_session_capable(project_root: &Path, environment_id: &str) -> bool {
    let plugins = match orchestrator_plugin_host::discover_by_kind(project_root.to_path_buf(), PLUGIN_KIND_ENVIRONMENT)
    {
        Ok(plugins) => plugins,
        Err(_) => return false,
    };
    let selected = plugins
        .iter()
        .find(|plugin| plugin.name == environment_id)
        .or(if plugins.len() == 1 { plugins.first() } else { None });
    selected.map(|plugin| capabilities_advertise_exec_session(&plugin.manifest.capabilities)).unwrap_or(false)
}

#[cfg(not(feature = "remote-animus-session"))]
pub(crate) fn environment_is_session_capable(_project_root: &Path, _environment_id: &str) -> bool {
    // `remote-animus-session` disabled: the delegation surface is not compiled in,
    // so never claim an environment is session-capable -- the run keeps the
    // existing per-phase / local paths unchanged.
    false
}

/// Feature-off stub so the call site in `execute_workflow_with_hub` compiles.
/// UNREACHABLE in practice: with the feature off `environment_is_session_capable`
/// returns `false`, so this is never invoked. Kept as a hard error (not a silent
/// local fallback) to match `phase_environment`'s "never silently fall back to
/// local when a non-local environment was requested" posture.
#[cfg(not(feature = "remote-animus-session"))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn delegate_workflow_via_session(
    _hub: std::sync::Arc<dyn orchestrator_core::services::ServiceHub>,
    _project_root: &str,
    environment_id: &str,
    workflow_id: &str,
    _workflow_ref: &str,
    _subject_id: &str,
    _subject_git_repo: Option<&str>,
    _dispatch_input: Option<&str>,
    _execution_cwd: &str,
    _phases_requested: Vec<String>,
    _event_emitter: Option<&SharedWorkflowEventEmitter>,
) -> Result<WorkflowExecuteInternalResult> {
    anyhow::bail!(
        "workflow '{workflow_id}' resolved to session-capable environment '{environment_id}', but this \
         workflow-runner build was compiled without the `remote-animus-session` feature (REQUIREMENT-052). \
         Rebuild the runner with `--features remote-animus-session` against an orchestrator-core rev + \
         animus-environment-protocol tag that provide `environment/exec_session`."
    )
}

/// Delegate the ENTIRE workflow to the session-capable environment `environment_id`
/// via a single [`EnvironmentClient::exec_session`] (REQ-052): prepare a bare
/// node, hand it the subject + workflow ref, forward every `environment/journal`
/// event home through `event_emitter`, tear the node down, and synthesize a
/// [`WorkflowExecuteInternalResult`] from the terminal `ExecSessionResponse`.
///
/// `phases_requested` is the workflow's declared phase ids -- used only to fill
/// the result summary; the node, not the runner, actually drives them.
///
/// Blocking [`EnvironmentClient`] work runs on a DEDICATED OS thread with its own
/// multi-thread runtime so the resident-host stdio I/O driver (spawned during
/// lease acquisition) stays alive across `prepare` -> `exec_session` -> `teardown`
/// -- the same lifetime hazard `crate::phase_environment::PreparedEnvironment::prepare_off_runtime`
/// guards against.
#[cfg(feature = "remote-animus-session")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn delegate_workflow_via_session(
    hub: std::sync::Arc<dyn orchestrator_core::services::ServiceHub>,
    project_root: &str,
    environment_id: &str,
    workflow_id: &str,
    workflow_ref: &str,
    subject_id: &str,
    subject_git_repo: Option<&str>,
    dispatch_input: Option<&str>,
    execution_cwd: &str,
    phases_requested: Vec<String>,
    event_emitter: Option<&SharedWorkflowEventEmitter>,
) -> Result<WorkflowExecuteInternalResult> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use anyhow::{anyhow, Context};
    use orchestrator_core::{EnvironmentClient, EnvironmentJournalEvent};
    use serde_json::Value;

    use crate::workflow_event_emitter::RuntimeWorkflowEvent;

    let started = Instant::now();

    // Shared, thread-safe accumulators the journal-forwarding closure writes and
    // the driver reads back after the session ends.
    let phase_results: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let phases_completed = Arc::new(AtomicUsize::new(0));

    // Clones captured by the (Fn + Send + Sync) journal callback.
    let emitter = event_emitter.cloned();
    let workflow_id_for_events = workflow_id.to_string();
    let project_root_for_journal = project_root.to_string();
    let phase_results_sink = phase_results.clone();
    let phases_completed_sink = phases_completed.clone();

    let on_journal = move |event: &EnvironmentJournalEvent| {
        // Mirror the node's agent-run transcript (output chunks, tool calls, ...)
        // into the PARENT run dir; lifecycle events fall through to the coarse map.
        persist_session_transcript(&project_root_for_journal, &workflow_id_for_events, event);

        let Some(kind) = map_journal_event_kind(&event.event_kind) else {
            return;
        };
        if let Some(emitter) = emitter.as_ref() {
            emitter.emit(RuntimeWorkflowEvent {
                workflow_id: workflow_id_for_events.clone(),
                kind,
                payload: serde_json::json!({
                    "phase_id": event.phase_id,
                    "phase_status": event.status,
                    "node_workflow_id": event.workflow_id,
                    "event_kind": event.event_kind,
                    "ts": event.ts,
                    "source": "environment_session",
                    "payload": event.payload,
                }),
                occurred_at: chrono::Utc::now(),
            });
        }
        if matches!(kind, RuntimeWorkflowEventKind::PhaseCompleted) {
            phases_completed_sink.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut sink) = phase_results_sink.lock() {
                sink.push(serde_json::json!({
                    "phase_id": event.phase_id,
                    "status": event.status.clone().unwrap_or_else(|| "completed".to_string()),
                    "source": "environment_session",
                }));
            }
        }
    };

    // Bare node spec (no repos -- the node clones what it needs), carrying the
    // run's target repo on `metadata.github_repo` so the environment plugin
    // repo-scopes the minted GitHub App installation token (mirrors the
    // REQUIREMENT-048 `phase_environment` prepare spec).
    let spec = build_session_spec(environment_id, subject_git_repo);

    // Blocking prepare -> exec_session -> teardown on a dedicated runtime; see the
    // doc comment for why the runtime must outlive `prepare`.
    let project_root_owned = project_root.to_string();
    let environment_id_owned = environment_id.to_string();
    let subject_id_owned = subject_id.to_string();
    let workflow_ref_owned = workflow_ref.to_string();
    let dispatch_input_owned = dispatch_input.map(str::to_string);

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("building dedicated runtime for the remote-animus session host")?;
            runtime.block_on(async move {
                let client = EnvironmentClient::resolve(Path::new(&project_root_owned), &environment_id_owned)
                    .map_err(|err| {
                        anyhow!(
                            "workflow is routed to session-capable environment '{environment_id_owned}' but no \
                             usable environment plugin was resolved (the run is NOT executed locally when a \
                             session environment is requested): {err}"
                        )
                    })?;
                let handle = client.prepare(spec).map_err(|err| {
                    anyhow!("remote-animus session prepare failed for '{environment_id_owned}': {err:#}")
                })?;
                // Unbounded: an agent-run session's duration is not known up front.
                let response = client.exec_session(
                    &handle,
                    subject_id_owned,
                    Some(workflow_ref_owned),
                    dispatch_input_owned,
                    on_journal,
                );
                // Best-effort teardown regardless of the session outcome.
                if let Err(err) = client.teardown(&handle) {
                    eprintln!(
                        "warning: remote-animus session teardown failed for '{environment_id_owned}' (handle {}): {err:#}",
                        handle.id
                    );
                }
                response.map_err(|err| {
                    anyhow!("remote-animus session exec_session failed for '{environment_id_owned}': {err:#}")
                })
            })
        })();
        let _ = tx.send(result);
    });

    let response = rx.await.map_err(|_| anyhow!("remote-animus session thread terminated unexpectedly"))??;

    let workflow_status = session_status_to_workflow_status(&response.status);

    // REQ-052 exact-once: the delegated node already ran every phase; drive the
    // PARENT's persisted workflow state machine to terminal so its `journal_runs`
    // row leaves `running`. Without this the daemon's journal-resume sweep
    // (`resumable_orphans_for_redispatch`, past the 90s grace) re-dispatches the
    // run as a "resumable orphan" until a re-dispatch happens to terminalize the
    // row -- ~3 runs per dispatch instead of exactly 1.
    //
    // BEST-EFFORT: the single terminal event is still emitted below and the
    // synthesized result is returned unchanged; a transition hiccup must NEVER
    // fail an otherwise-successful delegated run. These are PURE state-machine
    // transitions (no agents, no post-success -- the node already did the work);
    // the bounded loop + `is_terminal_workflow_status` guard prevents any
    // rework/verdict loop from spinning.
    match workflow_status {
        WorkflowStatus::Completed => {
            for _ in 0..=phases_requested.len() {
                match hub.workflows().get(workflow_id).await {
                    Ok(wf) if crate::workflow_execute::is_terminal_workflow_status(wf.status) => break,
                    Ok(_) => {
                        if hub.workflows().complete_current_phase_with_decision(workflow_id, None).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
        WorkflowStatus::Failed | WorkflowStatus::Escalated => {
            let _ = hub
                .workflows()
                .mark_completed_failed(workflow_id, format!("remote-animus session ended {}", response.status))
                .await;
        }
        WorkflowStatus::Cancelled => {
            let _ = hub.workflows().cancel(workflow_id).await;
        }
        _ => {}
    }

    // Emit the single terminal workflow event home (the driver owns it; the
    // journal map deliberately does not forward node-level terminal events).
    if let Some(emitter) = event_emitter {
        match workflow_status {
            WorkflowStatus::Completed => emitter.emit(RuntimeWorkflowEvent {
                workflow_id: workflow_id.to_string(),
                kind: RuntimeWorkflowEventKind::WorkflowCompleted,
                payload: serde_json::json!({ "final_status": "completed", "source": "environment_session" }),
                occurred_at: chrono::Utc::now(),
            }),
            WorkflowStatus::Failed | WorkflowStatus::Escalated => emitter.emit(RuntimeWorkflowEvent {
                workflow_id: workflow_id.to_string(),
                kind: RuntimeWorkflowEventKind::WorkflowFailed,
                payload: serde_json::json!({
                    "final_status": format!("{:?}", workflow_status).to_ascii_lowercase(),
                    "node_status": response.status,
                    "source": "environment_session",
                }),
                occurred_at: chrono::Utc::now(),
            }),
            _ => {}
        }
    }

    let phases_total = phases_requested.len();
    let completed = phases_completed.load(Ordering::SeqCst);
    let collected = phase_results.lock().map(|guard| guard.clone()).unwrap_or_default();

    Ok(WorkflowExecuteInternalResult {
        success: !matches!(
            workflow_status,
            WorkflowStatus::Failed | WorkflowStatus::Escalated | WorkflowStatus::Cancelled
        ),
        workflow_id: workflow_id.to_string(),
        workflow_ref: workflow_ref.to_string(),
        workflow_status,
        subject_id: subject_id.to_string(),
        execution_cwd: execution_cwd.to_string(),
        phases_requested,
        phases_completed: completed,
        phases_total,
        total_duration: started.elapsed(),
        phase_results: collected,
        // The remote node runs the whole workflow including any merge/PR the
        // `coding` workflow performs, so home-side post-success is a no-op.
        post_success: serde_json::json!({
            "status": "skipped",
            "reason": "remote-animus session owns the full workflow (incl. post-success) on the node",
        }),
    })
}

/// Build the bare [`EnvironmentSpec`] for a remote-animus session: no repos (the
/// node clones what it needs), with the run's target repo merged onto
/// `metadata.github_repo` when present so the plugin repo-scopes its GitHub App
/// token. Split out so the metadata shaping is unit-testable without a plugin.
#[cfg(feature = "remote-animus-session")]
fn build_session_spec(environment_id: &str, github_repo: Option<&str>) -> animus_environment_protocol::EnvironmentSpec {
    use std::collections::BTreeMap;

    use animus_environment_protocol::EnvironmentSpec;
    use serde_json::Value;

    let mut metadata = Value::Null;
    if let Some(repo) = github_repo.map(str::trim).filter(|repo| !repo.is_empty()) {
        let mut map = serde_json::Map::new();
        map.insert("github_repo".to_string(), Value::String(repo.to_string()));
        metadata = Value::Object(map);
    }
    EnvironmentSpec {
        kind: environment_id.to_string(),
        repos: Vec::new(),
        image: None,
        resources: None,
        env: BTreeMap::new(),
        metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_detects_exec_session() {
        assert!(capabilities_advertise_exec_session(&[
            "environment/prepare".to_string(),
            "environment/exec".to_string(),
            "environment/exec_session".to_string(),
            "environment/teardown".to_string(),
        ]));
    }

    #[test]
    fn capabilities_absent_is_not_session_capable() {
        // A REQUIREMENT-048-only environment serves prepare/exec/exec_stream but
        // NOT exec_session -- it must not be treated as session-capable.
        assert!(!capabilities_advertise_exec_session(&[
            "environment/prepare".to_string(),
            "environment/exec".to_string(),
            "environment/exec_stream".to_string(),
            "environment/teardown".to_string(),
        ]));
        assert!(!capabilities_advertise_exec_session(&[]));
    }

    #[test]
    fn journal_kind_maps_only_phase_lifecycle() {
        assert_eq!(map_journal_event_kind("phase_started"), Some(RuntimeWorkflowEventKind::PhaseStarted));
        assert_eq!(map_journal_event_kind("phase_completed"), Some(RuntimeWorkflowEventKind::PhaseCompleted));
        assert_eq!(map_journal_event_kind("phase_failed"), Some(RuntimeWorkflowEventKind::PhaseFailed));
        // Terminal + fine-grained node events are NOT forwarded as coarse events
        // (the driver emits the single terminal event from the session response).
        assert_eq!(map_journal_event_kind("workflow_completed"), None);
        assert_eq!(map_journal_event_kind("run_completed"), None);
        assert_eq!(map_journal_event_kind("output_chunk"), None);
        assert_eq!(map_journal_event_kind("tool_call"), None);
    }

    #[test]
    fn transcript_kinds_exclude_lifecycle() {
        for kind in ["output_chunk", "tool_call", "tool_result", "thinking", "started", "finished", "metadata", "error", "artifact"] {
            assert!(is_transcript_event_kind(kind), "{kind} should mirror to the parent transcript");
        }
        for kind in ["phase_started", "phase_completed", "phase_failed", "run_completed", "workflow_completed"] {
            assert!(!is_transcript_event_kind(kind), "{kind} is lifecycle -- must not be mirrored");
        }
    }

    #[test]
    fn rekey_swaps_node_workflow_id_for_parent() {
        // A node run id embeds the node workflow uuid; swapping it for the parent's
        // preserves the phase/attempt suffix so the portal groups it under the parent.
        let orig = "wf-11111111-1111-1111-1111-111111111111-code-implement-0-c0-a1-deadbeef";
        let got = rekey_transcript_run_id(orig, "11111111-1111-1111-1111-111111111111", "PARENT");
        assert_eq!(got, "wf-PARENT-code-implement-0-c0-a1-deadbeef");
    }

    #[test]
    fn rekey_prefixes_when_node_id_absent() {
        assert_eq!(rekey_transcript_run_id("wf-abc-code-check-0", "", "PARENT"), "wf-PARENT-abc-code-check-0");
        assert_eq!(rekey_transcript_run_id("loose-id", "nope", "PARENT"), "wf-PARENT-loose-id");
    }

    #[test]
    fn terminal_status_maps_to_workflow_status() {
        assert_eq!(session_status_to_workflow_status("completed"), WorkflowStatus::Completed);
        assert_eq!(session_status_to_workflow_status("SUCCEEDED"), WorkflowStatus::Completed);
        assert_eq!(session_status_to_workflow_status("escalated"), WorkflowStatus::Escalated);
        assert_eq!(session_status_to_workflow_status("cancelled"), WorkflowStatus::Cancelled);
        assert_eq!(session_status_to_workflow_status("paused"), WorkflowStatus::Paused);
        assert_eq!(session_status_to_workflow_status("failed"), WorkflowStatus::Failed);
        // Fail closed on an unrecognized terminal status.
        assert_eq!(session_status_to_workflow_status("weird-node-state"), WorkflowStatus::Failed);
    }

    #[cfg(feature = "remote-animus-session")]
    #[test]
    fn session_spec_is_bare_with_optional_github_repo() {
        let spec = build_session_spec("animus-environment-railway", Some("acme/widgets"));
        assert_eq!(spec.kind, "animus-environment-railway");
        assert!(spec.repos.is_empty());
        assert_eq!(spec.metadata.pointer("/github_repo").and_then(|v| v.as_str()), Some("acme/widgets"));

        let bare = build_session_spec("animus-environment-railway", None);
        assert!(bare.metadata.is_null());
    }
}
