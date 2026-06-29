use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use animus_actor::Actor;
use serde_json::Value;

use orchestrator_config::{
    collect_workflow_refs, ensure_pack_execution_requirements, resolve_active_pack_for_workflow_ref,
    resolve_pack_registry,
};
use orchestrator_core::{
    dispatch_workflow_event, ensure_workflow_config_compiled, load_workflow_config,
    project_requirement_workflow_status, register_workflow_runner_pid, services::ServiceHub,
    subject_adapter::SubjectContext, unregister_workflow_runner_pid, OrchestratorTask, OrchestratorWorkflow,
    PhaseDecisionVerdict, SubjectRef, WorkflowEvent, WorkflowRunInput, WorkflowStatus, SUBJECT_KIND_CUSTOM,
};

use crate::config_context::RuntimeConfigContext;
use crate::ensure_execution_cwd::ensure_execution_cwd;
use crate::phase_evals::{decide_eval_gate, force_rework, run_phase_evals, EvalGateDecision};
use crate::phase_executor::{run_workflow_phase, PhaseExecuteOverrides, PhaseExecutionOutcome, PhaseRunParams};
use crate::phase_output::{
    is_phase_completed, persist_phase_output, phase_output_dir, read_persisted_decision, PersistedPhaseOutput,
};
use crate::workflow_event_emitter::{RuntimeWorkflowEvent, RuntimeWorkflowEventKind, SharedWorkflowEventEmitter};

// v0.5 IPC-safety changes:
//   * The legacy in-process `PhaseEvent<'a>` enum + `PhaseEventCallback`
//     closure on `WorkflowExecuteParams` were removed. The plugin
//     entrypoint constructs a `PhaseEventRecorder` and passes it as
//     `event_emitter`; events accumulate inside the plugin process and
//     are returned to the daemon as `WorkflowExecuteResult::phase_events`
//     in the JSON-RPC response (no closures cross IPC).
//   * The previous `params.hub: Option<Arc<dyn ServiceHub>>` field is gone;
//     `execute_workflow_with_hub` takes the hub as an explicit argument.
//     The plugin loads its hub once at `initialize` time and reuses it
//     across requests for the lifetime of the bound project.
//   * `params.workflow_event_emitter` is now passed as the explicit
//     `event_emitter` argument (still a `SharedWorkflowEventEmitter`); the
//     plugin entrypoint wires a `PhaseEventRecorder` here.
//
// `PhaseEvent` (the protocol-shaped enum) lives in
// `animus_workflow_runner_protocol::PhaseEvent`.

/// Internal parameter bundle for `execute_workflow_with_hub`. Crate-private
/// because it carries fields (`phase_routing`, `mcp_config`) that reference
/// ao-cli-local types — the public IPC surface is
/// `animus_workflow_runner_protocol::WorkflowExecuteRequest`.
pub struct WorkflowExecuteInternalParams {
    pub project_root: String,
    pub workflow_id: Option<String>,
    pub task_id: Option<String>,
    pub requirement_id: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub workflow_ref: Option<String>,
    pub input: Option<Value>,
    pub vars: HashMap<String, String>,
    pub model: Option<String>,
    pub tool: Option<String>,
    pub phase_timeout_secs: Option<u64>,
    pub phase_filter: Option<String>,
    pub phase_routing: Option<protocol::PhaseRoutingConfig>,
    pub mcp_config: Option<protocol::McpRuntimeConfig>,
    /// Transport-asserted caller identity relayed verbatim from the inbound
    /// `WorkflowExecuteRequest`. Threaded into every phase's `SessionRequest`
    /// so the provider/agent runs as the user. `None` for system-initiated
    /// runs (e.g. the CLI direct-execute path). The runner never interprets it.
    pub actor: Option<Actor>,
}

// Back-compat alias for the lifted in-tree call sites + tests that still
// reference `WorkflowExecuteParams` by name.
pub(crate) type WorkflowExecuteParams = WorkflowExecuteInternalParams;

pub struct WorkflowExecuteInternalResult {
    pub success: bool,
    pub workflow_id: String,
    pub workflow_ref: String,
    pub workflow_status: WorkflowStatus,
    pub subject_id: String,
    pub execution_cwd: String,
    pub phases_requested: Vec<String>,
    pub phases_completed: usize,
    pub phases_total: usize,
    pub total_duration: Duration,
    pub phase_results: Vec<Value>,
    pub post_success: Value,
}

// Back-compat alias for the lifted in-tree call sites + tests.
pub(crate) type WorkflowExecuteResult = WorkflowExecuteInternalResult;

/// Crate-private replacement for the deleted in-process `PhaseEvent<'_>`.
/// The lifted code is full of `emit(PhaseEvent::Started { ... })` calls; we
/// retain the type only so those call sites compile unchanged. Nothing is
/// done with the constructed variant (the `emit` closure inside
/// `execute_workflow_with_hub` is a no-op now — protocol-shaped phase
/// events flow through `event_emitter` instead).
#[allow(dead_code)]
pub(crate) enum LegacyPhaseEvent<'a> {
    Started {
        phase_id: &'a str,
        phase_index: usize,
        total_phases: usize,
    },
    Decision {
        phase_id: &'a str,
        decision: &'a orchestrator_core::PhaseDecision,
    },
    Completed {
        phase_id: &'a str,
        duration: Duration,
        success: bool,
        error: Option<String>,
        model: Option<String>,
        tool: Option<String>,
    },
}

// Aliased name used by the lifted call sites.
#[allow(dead_code)]
pub(crate) use LegacyPhaseEvent as PhaseEvent;

#[derive(Clone, Default)]
struct WorkflowPhaseInputs {
    dispatch_input: Option<String>,
    schedule_input: Option<String>,
}

struct WorkflowRunnerPidGuard {
    project_root: String,
    workflow_id: String,
}

impl WorkflowRunnerPidGuard {
    fn register(project_root: &str, workflow_id: &str) -> Result<Self> {
        register_workflow_runner_pid(Path::new(project_root), workflow_id, std::process::id())?;
        Ok(Self { project_root: project_root.to_string(), workflow_id: workflow_id.to_string() })
    }
}

impl Drop for WorkflowRunnerPidGuard {
    fn drop(&mut self) {
        let _ = unregister_workflow_runner_pid(Path::new(&self.project_root), &self.workflow_id);
    }
}

fn ensure_workflow_pack_execution_requirements(
    pack_registry: &orchestrator_config::ResolvedPackRegistry,
    workflow_config: &orchestrator_config::WorkflowConfig,
    workflow_ref: &str,
) -> Result<()> {
    let workflow_refs = collect_workflow_refs(&workflow_config.workflows, workflow_ref)
        .with_context(|| format!("failed to resolve workflow activation graph for '{}'", workflow_ref))?;
    let mut validated_pack_ids = HashSet::new();

    for referenced_workflow_ref in workflow_refs {
        let Some(entry) = resolve_active_pack_for_workflow_ref(pack_registry, &referenced_workflow_ref) else {
            continue;
        };
        if !validated_pack_ids.insert(entry.pack_id.to_ascii_lowercase()) {
            continue;
        }
        let Some(pack) = entry.loaded_manifest() else {
            continue;
        };
        ensure_pack_execution_requirements(pack).with_context(|| {
            format!(
                "workflow '{}' cannot activate pack '{}' required by workflow '{}' from {}",
                workflow_ref,
                pack.manifest.id,
                referenced_workflow_ref,
                pack.pack_root.display()
            )
        })?;
    }

    Ok(())
}

fn workflow_phase_inputs(workflow: &OrchestratorWorkflow) -> WorkflowPhaseInputs {
    let dispatch_input = workflow.input.as_ref().map(Value::to_string);
    let schedule_input = if workflow.subject.id().starts_with("schedule:") { dispatch_input.clone() } else { None };

    WorkflowPhaseInputs { dispatch_input, schedule_input }
}

/// In-process integration entrypoint. Wire callers go through
/// `crate::plugin::handle_workflow_execute`. The plugin process loads its
/// `FileServiceHub` once at `initialize` time and reuses it across
/// requests; tests may construct an `InMemoryServiceHub`.
pub async fn execute_workflow_with_hub(
    mut params: WorkflowExecuteInternalParams,
    hub: Arc<dyn ServiceHub>,
    event_emitter: Option<SharedWorkflowEventEmitter>,
) -> Result<WorkflowExecuteInternalResult> {
    let routing = params.phase_routing.take().unwrap_or_default();
    let phase_timeout_secs = params.phase_timeout_secs;
    // codex P2 #1: lift the per-call MCP runtime config out of `params` so it
    // can be borrowed by both the phase-filter and full-pipeline `PhaseRunParams`
    // construction sites below.
    let mcp_config = params.mcp_config.take();

    let mut workflow = match params.workflow_id.as_deref() {
        Some(workflow_id) => load_existing_workflow(hub.clone(), workflow_id, &params).await?,
        None => {
            let input = resolve_input(&params)?;
            let subject = input.subject().clone();
            let subject_id = subject.id().to_string();
            hub.workflows().run(input).await.or_else(|run_err| {
                if subject.kind().eq_ignore_ascii_case(SUBJECT_KIND_CUSTOM) {
                    return Err(run_err);
                }
                let all =
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(hub.workflows().list()))?;
                all.into_iter()
                    .find(|w| w.subject.id() == subject_id || w.task_id == subject_id)
                    .ok_or_else(|| anyhow!("no workflow found for subject '{}'", subject_id))
            })?
        }
    };
    let _runner_pid_guard = WorkflowRunnerPidGuard::register(&params.project_root, &workflow.id)
        .context("failed to register active workflow execution")?;
    let mut subject_context = resolve_execution_subject_context(
        hub.clone(),
        &workflow.subject,
        params.title.as_deref(),
        params.description.as_deref(),
    )
    .await?;
    let mut task = subject_context.task.take();

    let execution_cwd = ensure_execution_cwd(hub.clone(), &params.project_root, &workflow.subject, &subject_context)
        .await
        .context("failed to resolve execution cwd")?;

    if let Some(task_id) = task.as_ref().map(|t| t.id.clone()) {
        task = Some(
            hub.tasks()
                .get(&task_id)
                .await
                .with_context(|| format!("task '{}' not found after cwd preparation", task_id))?,
        );
    }

    if let Some(task) = task.as_ref() {
        subject_context.subject_title = task.title.clone();
        subject_context.subject_description = task.description.clone();
    }

    let phases_to_run: Vec<String> = if let Some(ref phase_filter) = params.phase_filter {
        vec![phase_filter.clone()]
    } else {
        workflow.phases.iter().map(|p| p.phase_id.clone()).collect()
    };

    if phases_to_run.is_empty() {
        return Err(anyhow!("workflow has no phases to execute"));
    }

    if let Err(err) = hub.daemon().start(Default::default()).await {
        eprintln!("warning: failed to auto-start runner for workflow execute: {err}");
    }

    let subject_id_str = workflow.subject.id().to_string();
    let subject_title = subject_context.subject_title.clone();
    let subject_description = subject_context.subject_description.clone();
    let task_complexity = task.as_ref().map(|t| t.complexity);

    ensure_workflow_config_compiled(Path::new(&params.project_root))?;
    let workflow_config = load_workflow_config(Path::new(&params.project_root))?;
    let workflow_ref = workflow.workflow_ref.clone().unwrap_or_else(|| workflow_config.default_workflow_ref.clone());
    let pack_registry = resolve_pack_registry(Path::new(&params.project_root))?;
    ensure_workflow_pack_execution_requirements(&pack_registry, &workflow_config, &workflow_ref)?;
    let phase_inputs = workflow_phase_inputs(&workflow);
    let workflow_vars = workflow.vars.clone();
    let mut rework_context: Option<String> = None;
    let mut results = Vec::new();
    let workflow_start = Instant::now();

    // v0.5: PhaseEventCallback was removed. The protocol-shaped PhaseEvents
    // are recorded inside the `event_emitter` (`PhaseEventRecorder`) via
    // `emit_runtime`. We retain the no-op `emit` shim so the lifted call
    // sites compile unchanged.
    let emit = |_event: LegacyPhaseEvent<'_>| {};

    let workflow_id_for_emitter = workflow.id.clone();
    let emit_runtime = |kind: RuntimeWorkflowEventKind, payload: Value| {
        if let Some(ref emitter) = event_emitter {
            emitter.emit(RuntimeWorkflowEvent {
                workflow_id: workflow_id_for_emitter.clone(),
                kind,
                payload,
                occurred_at: chrono::Utc::now(),
            });
        }
    };

    if let Some(phase_filter) = params.phase_filter.clone() {
        let phase_attempt = workflow
            .phases
            .iter()
            .find(|p| p.phase_id.eq_ignore_ascii_case(&phase_filter))
            .map(|p| p.attempt)
            .unwrap_or(0);

        emit(PhaseEvent::Started { phase_id: &phase_filter, phase_index: 0, total_phases: 1 });
        emit_runtime(
            RuntimeWorkflowEventKind::PhaseStarted,
            serde_json::json!({
                "phase_id": phase_filter,
                "phase_index": 0usize,
                "phase_attempt": phase_attempt,  // codex P2 round 4: retries
                "total_phases": 1usize,
            }),
        );
        let phase_start = Instant::now();

        let phase_overrides = PhaseExecuteOverrides {
            tool: params.tool.clone(),
            model: params.model.clone(),
            rework_context: rework_context.take(),
        };
        let run_result = run_workflow_phase(&PhaseRunParams {
            project_root: &params.project_root,
            execution_cwd: &execution_cwd,
            workflow_id: &workflow.id,
            workflow_ref: workflow_ref.as_str(),
            subject_id: &subject_id_str,
            subject_title: &subject_title,
            subject_description: &subject_description,
            task_complexity,
            phase_id: &phase_filter,
            phase_attempt,
            overrides: Some(&phase_overrides),
            pipeline_vars: if workflow_vars.is_empty() { None } else { Some(&workflow_vars) },
            dispatch_input: phase_inputs.dispatch_input.as_deref(),
            schedule_input: phase_inputs.schedule_input.as_deref(),
            routing: &routing,

            phase_timeout_secs,
            mcp_config: mcp_config.as_ref(),
            actor: params.actor.as_ref(),
        })
        .await;

        let phase_elapsed = phase_start.elapsed();

        match run_result {
            Ok(result) => {
                if let PhaseExecutionOutcome::Completed { phase_decision: Some(ref decision), .. } = &result.outcome {
                    emit(PhaseEvent::Decision { phase_id: &phase_filter, decision });
                }

                let phase_status = phase_result_status(&result.outcome);
                // Codex round 8 P2 #1: `--phase` runs persist JSON output
                // for inspection but MUST NOT write the recovery marker, or
                // the next full workflow run will skip this phase thinking
                // crash-recovery already completed it.
                let _ = persist_phase_output_without_marker(
                    &params.project_root,
                    &workflow.id,
                    &phase_filter,
                    &result.outcome,
                );
                let _ = phase_attempt; // unused without marker write
                emit(PhaseEvent::Completed {
                    phase_id: &phase_filter,
                    duration: phase_elapsed,
                    success: phase_status != "failed",
                    error: None,
                    model: result.model.clone(),
                    tool: result.tool.clone(),
                });
                emit_runtime(
                    RuntimeWorkflowEventKind::PhaseCompleted,
                    serde_json::json!({
                        "phase_id": phase_filter,
                        "phase_status": phase_status,
                    }),
                );
                results.push(serde_json::json!({
                    "phase_id": phase_filter,
                    "status": phase_status,
                    "duration_secs": phase_elapsed.as_secs(),
                    "outcome": result.outcome,
                    "metadata": result.metadata,
                }));

                let total_duration = workflow_start.elapsed();
                return Ok(WorkflowExecuteResult {
                    success: phase_status != "failed",
                    workflow_id: workflow.id.clone(),
                    workflow_ref,
                    workflow_status: workflow.status,
                    subject_id: subject_id_str,
                    execution_cwd,
                    phases_requested: vec![phase_filter],
                    phases_completed: usize::from(phase_status == "completed"),
                    phases_total: 1,
                    total_duration,
                    phase_results: results,
                    post_success: serde_json::json!({
                        "status": "skipped",
                        "reason": "post-success actions are not run for single-phase execution",
                    }),
                });
            }
            Err(err) => {
                emit(PhaseEvent::Completed {
                    phase_id: &phase_filter,
                    duration: phase_elapsed,
                    success: false,
                    error: Some(err.to_string()),
                    model: None,
                    tool: None,
                });
                emit_runtime(
                    RuntimeWorkflowEventKind::PhaseFailed,
                    serde_json::json!({
                        "phase_id": phase_filter,
                        "phase_status": "failed",
                        "error": err.to_string(),
                    }),
                );
                results.push(serde_json::json!({
                    "phase_id": phase_filter,
                    "status": "failed",
                    "duration_secs": phase_elapsed.as_secs(),
                    "error": err.to_string(),
                }));
                let total_duration = workflow_start.elapsed();
                return Ok(WorkflowExecuteResult {
                    success: false,
                    workflow_id: workflow.id.clone(),
                    workflow_ref,
                    workflow_status: workflow.status,
                    subject_id: subject_id_str,
                    execution_cwd,
                    phases_requested: vec![phase_filter],
                    phases_completed: 0,
                    phases_total: 1,
                    total_duration,
                    phase_results: results,
                    post_success: serde_json::json!({
                        "status": "skipped",
                        "reason": "post-success actions are not run for single-phase execution",
                    }),
                });
            }
        }
    }

    let mut phases_to_run: Vec<String> = workflow.phases.iter().map(|p| p.phase_id.clone()).collect();
    if phases_to_run.is_empty() {
        return Err(anyhow!("workflow has no phases to execute"));
    }

    // Per-phase eval gate state. `config_ctx` exposes the compiled
    // `phases.<id>.evals` block (workflow YAML wins over agent_runtime_config);
    // `eval_rework_counts` tracks eval-driven reworks per phase, distinct from
    // the workflow state machine's own rework budget — once a phase's
    // `max_reworks` eval budget is spent, the gate falls through to Block.
    //
    // TODO(codex-p2): this counter is in-memory for the duration of a single
    // `execute_workflow_with_hub` call. A runner restart/resume mid-pipeline
    // resets it to zero, so a phase with `on_fail = rework` can re-consume its
    // `max_reworks` eval budget after each restart instead of falling through
    // to Block. Durable tracking needs a scoped-state sidecar (alongside the
    // phase completion markers) keyed by workflow+phase; deferred as a
    // follow-up since it touches the crash-recovery durability model.
    let config_ctx = RuntimeConfigContext::load(&params.project_root);
    let mut eval_rework_counts: HashMap<String, u32> = HashMap::new();

    let mut phase_idx: usize = workflow.current_phase_index;
    let mut reported_workflow_status = workflow.status;
    while phase_idx < phases_to_run.len() && !is_terminal_workflow_status(workflow.status) {
        let phase_id = phases_to_run[phase_idx].clone();
        let phase_attempt = workflow.phases.iter().find(|p| p.phase_id == phase_id).map(|p| p.attempt).unwrap_or(0);

        if is_phase_completed(&params.project_root, &workflow.id, &phase_id, phase_attempt) {
            match read_persisted_decision(&params.project_root, &workflow.id, &phase_id) {
                Ok(decision) => {
                    let updated =
                        hub.workflows().complete_current_phase_with_decision(&workflow.id, Some(decision)).await?;
                    let next_status = updated.status;
                    let next_phase_index = updated.current_phase_index;
                    workflow = updated;
                    reported_workflow_status = next_status;
                    phases_to_run = workflow.phases.iter().map(|phase| phase.phase_id.clone()).collect();
                    results.push(serde_json::json!({
                        "phase_id": phase_id,
                        "status": "replayed_completion_marker",
                        "duration_secs": 0,
                        "workflow_status": format!("{:?}", next_status).to_ascii_lowercase(),
                    }));
                    if matches!(
                        workflow.status,
                        WorkflowStatus::Failed | WorkflowStatus::Escalated | WorkflowStatus::Cancelled
                    ) {
                        break;
                    }
                    phase_idx = next_phase_index;
                    continue;
                }
                Err(err) => {
                    let reason = format!(
                        "phase '{phase_id}' marker exists but decision could not be replayed ({err}); run `animus workflow resume {workflow_id} --force` after manual investigation",
                        workflow_id = workflow.id,
                    );
                    eprintln!("warning: {reason}");
                    workflow = hub.workflows().fail_current_phase(&workflow.id, reason.clone()).await?;
                    reported_workflow_status = workflow.status;
                    phases_to_run = workflow.phases.iter().map(|phase| phase.phase_id.clone()).collect();
                    results.push(serde_json::json!({
                        "phase_id": phase_id,
                        "status": "blocked_unreplayable_marker",
                        "duration_secs": 0,
                        "workflow_status": format!("{:?}", workflow.status).to_ascii_lowercase(),
                        "error": reason,
                    }));
                    break;
                }
            }
        }

        emit(PhaseEvent::Started { phase_id: &phase_id, phase_index: phase_idx, total_phases: phases_to_run.len() });
        emit_runtime(
            RuntimeWorkflowEventKind::PhaseStarted,
            serde_json::json!({
                "phase_id": phase_id,
                "phase_index": phase_idx,
                "phase_attempt": phase_attempt,  // codex P2 round 4: retries
                "total_phases": phases_to_run.len(),
            }),
        );
        let phase_start = Instant::now();

        let phase_overrides = PhaseExecuteOverrides {
            tool: params.tool.clone(),
            model: params.model.clone(),
            rework_context: rework_context.take(),
        };
        let run_result = run_workflow_phase(&PhaseRunParams {
            project_root: &params.project_root,
            execution_cwd: &execution_cwd,
            workflow_id: &workflow.id,
            workflow_ref: workflow_ref.as_str(),
            subject_id: &subject_id_str,
            subject_title: &subject_title,
            subject_description: &subject_description,
            task_complexity,
            phase_id: &phase_id,
            phase_attempt,
            overrides: Some(&phase_overrides),
            pipeline_vars: if workflow_vars.is_empty() { None } else { Some(&workflow_vars) },
            dispatch_input: phase_inputs.dispatch_input.as_deref(),
            schedule_input: phase_inputs.schedule_input.as_deref(),
            routing: &routing,

            phase_timeout_secs,
            mcp_config: mcp_config.as_ref(),
            actor: params.actor.as_ref(),
        })
        .await;

        let phase_elapsed = phase_start.elapsed();

        match run_result {
            Ok(mut result) => {
                // Eval gate. Runs only when the phase produced a Completed
                // outcome with an advancing verdict (Advance/Unknown) and the
                // phase declares `evals.checks`. The gate may rewrite the
                // outcome BEFORE persistence/advance below so the normal
                // rework (re-run current phase) and manual-pause (Block ->
                // ManualPending) machinery carries it through unchanged.
                let mut eval_summary: Option<Value> = None;
                let eval_advancing = matches!(
                    &result.outcome,
                    PhaseExecutionOutcome::Completed { phase_decision, .. }
                        if phase_decision
                            .as_ref()
                            .map(|d| matches!(
                                d.verdict,
                                PhaseDecisionVerdict::Advance | PhaseDecisionVerdict::Unknown
                            ))
                            .unwrap_or(true)
                );
                if eval_advancing {
                    if let Some(evals) = config_ctx.phase_evals(&phase_id).filter(|e| !e.checks.is_empty()).cloned() {
                        let phase_context = phase_eval_context(&result.outcome);
                        let report = run_phase_evals(
                            &params.project_root,
                            &execution_cwd,
                            &config_ctx,
                            &evals,
                            &phase_context,
                            params.actor.as_ref(),
                        )
                        .await;
                        let used = eval_rework_counts.get(&phase_id).copied().unwrap_or(0);
                        let gate = decide_eval_gate(&evals, &report, used);
                        eprintln!(
                            "[ao][evals] phase={} pass_rate={:.2} threshold={:.2} ({}/{} passed) gate={}",
                            phase_id,
                            report.pass_rate,
                            evals.pass_threshold,
                            report.passed,
                            report.total,
                            match &gate {
                                EvalGateDecision::Pass => "pass",
                                EvalGateDecision::Rework { .. } => "rework",
                                EvalGateDecision::Block { .. } => "block",
                            }
                        );
                        let mut summary = report.to_json();
                        match gate {
                            EvalGateDecision::Pass => {
                                summary["gate"] = serde_json::json!("pass");
                            }
                            EvalGateDecision::Rework { reason } => {
                                eval_rework_counts.insert(phase_id.clone(), used + 1);
                                summary["gate"] = serde_json::json!("rework");
                                summary["reason"] = serde_json::json!(reason);
                                summary["eval_reworks_used"] = serde_json::json!(used + 1);
                                force_rework(&mut result.outcome, &phase_id, format!("eval gate rework: {reason}"));
                            }
                            EvalGateDecision::Block { reason } => {
                                summary["gate"] = serde_json::json!("block");
                                summary["reason"] = serde_json::json!(reason.clone());
                                result.outcome = PhaseExecutionOutcome::ManualPending {
                                    instructions: format!("eval gate blocked phase '{phase_id}': {reason}"),
                                    approval_note_required: true,
                                };
                            }
                        }
                        eval_summary = Some(summary);
                    }
                }

                if let PhaseExecutionOutcome::Completed { phase_decision: Some(ref decision), .. } = &result.outcome {
                    emit(PhaseEvent::Decision { phase_id: &phase_id, decision });
                    // codex P2 round 2: also forward the verdict through the
                    // runtime emitter so it lands in `phase_events` + the
                    // durable workflow-events JSONL. The recorder maps this
                    // back into a protocol-shaped `PhaseEvent::Decision`.
                    emit_runtime(
                        RuntimeWorkflowEventKind::PhaseCompleted,
                        serde_json::json!({
                            "phase_id": phase_id,
                            "phase_status": "decision",
                            "verdict": format!("{:?}", decision.verdict).to_ascii_lowercase(),
                            "confidence": decision.confidence,
                        }),
                    );
                }

                // Skip persistence for ManualPending: the .completed marker
                // would otherwise advertise the phase as done. On crash
                // between this write and the pause-state mutation, replay
                // would read `verdict: manual_pending` (an unrecognised
                // verdict for `read_persisted_decision`) and silently fail
                // OR — worse — treat the phase as advanced past the
                // manual gate. The pause is itself durable via the task
                // status update issued in the ManualPending arm below.
                //
                // FATAL: for non-ManualPending outcomes, `persist_phase_output`
                // writes the durable output + completion marker that the
                // recovery oracle (`is_phase_completed` /
                // `read_persisted_decision`) reads. Advancing the workflow
                // state before this write is durable means a crash between
                // here and the `complete_current_phase_with_decision`
                // call below would leave the workflow on the NEXT phase
                // with no completion marker for THIS phase — the
                // resumed-agent path (`daemon_run.rs` ~line 373) already
                // treats this exact failure as fatal; the normal path must
                // do the same. On failure the workflow stays on the current
                // phase: the dispatcher sees it as still-Running on the next
                // tick and either retries the persistence or surfaces the
                // failure for human review.
                if !matches!(&result.outcome, PhaseExecutionOutcome::ManualPending { .. }) {
                    if let Err(persist_err) = persist_phase_output(
                        &params.project_root,
                        &workflow.id,
                        &phase_id,
                        phase_attempt,
                        &result.outcome,
                    ) {
                        // The phase completed but the durable output
                        // marker did not. Advancing the workflow to the
                        // next phase here would leave the workflow ahead
                        // of its persisted completion oracle — exactly
                        // the crash-replay hazard we are fixing. We mark
                        // the phase as failed (via `fail_current_phase`)
                        // so the workflow's status becomes terminal Failed:
                        // downstream daemon reconciliation surfaces the
                        // failure correctly, orphan recovery skips it,
                        // and an operator can inspect the run dir and
                        // `animus workflow retry` after fixing the I/O
                        // condition (vs pre-fix, where `let _ = persist`
                        // silently dropped the error and advanced the
                        // workflow into the next phase).
                        let fail_msg = format!(
                            "phase '{}' completed but persist_phase_output failed: {}; failing phase to preserve crash-replay invariant — operator must inspect run dir and retry workflow after resolving I/O",
                            phase_id, persist_err
                        );
                        workflow = hub.workflows().fail_current_phase(&workflow.id, fail_msg.clone()).await?;
                        reported_workflow_status = workflow.status;
                        emit(PhaseEvent::Completed {
                            phase_id: &phase_id,
                            duration: phase_elapsed,
                            success: false,
                            error: Some(fail_msg.clone()),
                            model: result.model.clone(),
                            tool: result.tool.clone(),
                        });
                        emit_runtime(
                            RuntimeWorkflowEventKind::PhaseFailed,
                            serde_json::json!({
                                "phase_id": phase_id,
                                "phase_status": "persist_failed",
                                "error": fail_msg,
                            }),
                        );
                        results.push(serde_json::json!({
                            "phase_id": phase_id,
                            "status": "persist_failed",
                            "duration_secs": phase_elapsed.as_secs(),
                            "workflow_status": format!("{:?}", workflow.status).to_ascii_lowercase(),
                            "error": fail_msg,
                        }));
                        break;
                    }
                }

                match &result.outcome {
                    PhaseExecutionOutcome::Completed { phase_decision, .. } => {
                        let decision = phase_decision.clone();
                        let updated = hub
                            .workflows()
                            .complete_current_phase_with_decision(&workflow.id, decision.clone())
                            .await?;
                        let next_status = updated.status;
                        let next_phase_index = updated.current_phase_index;
                        let next_phase_id = updated.current_phase.clone().or_else(|| {
                            updated.phases.get(updated.current_phase_index).map(|phase| phase.phase_id.clone())
                        });
                        let maybe_context = phase_rework_context(&result.outcome);
                        workflow = updated;
                        reported_workflow_status = next_status;
                        phases_to_run = workflow.phases.iter().map(|phase| phase.phase_id.clone()).collect();

                        let status = phase_result_status(&result.outcome).to_string();
                        let next_success = !matches!(next_status, WorkflowStatus::Failed | WorkflowStatus::Escalated);
                        emit(PhaseEvent::Completed {
                            phase_id: &phase_id,
                            duration: phase_elapsed,
                            success: next_success,
                            error: None,
                            model: result.model.clone(),
                            tool: result.tool.clone(),
                        });
                        emit_runtime(
                            RuntimeWorkflowEventKind::PhaseCompleted,
                            serde_json::json!({
                                "phase_id": phase_id,
                                "phase_status": status,
                            }),
                        );
                        let mut result_value = serde_json::json!({
                            "phase_id": phase_id,
                            "status": status,
                            "duration_secs": phase_elapsed.as_secs(),
                            "workflow_status": format!("{:?}", next_status).to_ascii_lowercase(),
                            "outcome": result.outcome,
                            "metadata": result.metadata,
                        });
                        if let Some(next_phase_id) = next_phase_id {
                            result_value["next_phase_id"] = serde_json::json!(next_phase_id);
                        }
                        if matches!(decision.as_ref().map(|value| value.verdict), Some(PhaseDecisionVerdict::Skip)) {
                            result_value["close_reason"] = serde_json::json!(decision
                                .as_ref()
                                .map(|value| value.reason.clone())
                                .unwrap_or_default());
                        }
                        if let Some(summary) = eval_summary.take() {
                            result_value["evals"] = summary;
                        }
                        results.push(result_value);

                        if matches!(
                            workflow.status,
                            WorkflowStatus::Failed | WorkflowStatus::Escalated | WorkflowStatus::Cancelled
                        ) {
                            break;
                        }

                        rework_context = maybe_context;
                        phase_idx = next_phase_index;
                        continue;
                    }
                    PhaseExecutionOutcome::ManualPending { .. } => {
                        let outcome = dispatch_workflow_event(
                            hub.clone(),
                            &params.project_root,
                            WorkflowEvent::Pause { workflow_id: workflow.id.clone(), reason_detail: None },
                        )
                        .await?;
                        workflow = outcome
                            .workflow
                            .ok_or_else(|| anyhow!("workflow '{}' not found for manual pause", workflow.id))?;
                        reported_workflow_status = workflow.status;
                        emit(PhaseEvent::Completed {
                            phase_id: &phase_id,
                            duration: phase_elapsed,
                            success: true,
                            error: None,
                            model: None,
                            tool: None,
                        });
                        emit_runtime(
                            RuntimeWorkflowEventKind::PhaseCompleted,
                            serde_json::json!({
                                "phase_id": phase_id,
                                "phase_status": "manual_pending",
                            }),
                        );
                        let mut manual_result = serde_json::json!({
                            "phase_id": phase_id,
                            "status": "manual_pending",
                            "duration_secs": phase_elapsed.as_secs(),
                            "workflow_status": format!("{:?}", workflow.status).to_ascii_lowercase(),
                            "outcome": result.outcome,
                            "metadata": result.metadata,
                        });
                        if let Some(summary) = eval_summary.take() {
                            manual_result["evals"] = summary;
                        }
                        results.push(manual_result);
                        break;
                    }
                }
            }
            Err(err) => {
                // DispatchRetryableError marks the case where a pre-runner
                // checkpoint write failed: no side-effecting work happened
                // yet. Pre-fix this was silently swallowed and the runner
                // would dispatch anyway, breaking the crash-replay
                // invariant. Within this PR's scope (no changes to
                // daemon_run.rs / orphan-recovery / scheduler semantics),
                // the safest terminal disposition is to fail the phase:
                // downstream reconciliation surfaces the failure
                // correctly, orphan recovery skips it, and an operator
                // can `animus workflow retry` after resolving the I/O
                // condition. The `phase_status: dispatch_retry`
                // discriminator on the emitted event lets operators
                // distinguish a transient checkpoint-write failure from
                // a real phase failure when triaging. (Automatic next-tick
                // retry would require scheduler changes outside this PR.)
                if err.downcast_ref::<crate::phase_executor::DispatchRetryableError>().is_some() {
                    workflow = hub.workflows().fail_current_phase(&workflow.id, err.to_string()).await?;
                    reported_workflow_status = workflow.status;
                    emit(PhaseEvent::Completed {
                        phase_id: &phase_id,
                        duration: phase_elapsed,
                        success: false,
                        error: Some(err.to_string()),
                        model: None,
                        tool: None,
                    });
                    emit_runtime(
                        RuntimeWorkflowEventKind::PhaseFailed,
                        serde_json::json!({
                            "phase_id": phase_id,
                            "phase_status": "dispatch_retry",
                            "error": err.to_string(),
                        }),
                    );
                    results.push(serde_json::json!({
                        "phase_id": phase_id,
                        "status": "dispatch_retry",
                        "duration_secs": phase_elapsed.as_secs(),
                        "workflow_status": format!("{:?}", workflow.status).to_ascii_lowercase(),
                        "error": err.to_string(),
                    }));
                    break;
                }
                workflow = hub.workflows().fail_current_phase(&workflow.id, err.to_string()).await?;
                reported_workflow_status = workflow.status;
                emit(PhaseEvent::Completed {
                    phase_id: &phase_id,
                    duration: phase_elapsed,
                    success: false,
                    error: Some(err.to_string()),
                    model: None,
                    tool: None,
                });
                emit_runtime(
                    RuntimeWorkflowEventKind::PhaseFailed,
                    serde_json::json!({
                        "phase_id": phase_id,
                        "phase_status": "failed",
                        "error": err.to_string(),
                    }),
                );
                results.push(serde_json::json!({
                    "phase_id": phase_id,
                    "status": "failed",
                    "duration_secs": phase_elapsed.as_secs(),
                    "workflow_status": format!("{:?}", workflow.status).to_ascii_lowercase(),
                    "error": err.to_string(),
                }));
                break;
            }
        }
    }

    let total_duration = workflow_start.elapsed();
    let mut post_success = serde_json::json!({
        "status": "skipped",
        "reason": "workflow did not complete all phases",
    });
    if workflow.status == WorkflowStatus::Completed {
        project_requirement_success_status(hub.clone(), &workflow.subject, &workflow_ref).await?;
        post_success = if let Some(ref t) = task {
            execute_post_success_actions(&params.project_root, t, &workflow, &workflow_config, &execution_cwd).await
        } else {
            serde_json::json!({
                "status": "skipped",
                "reason": "post-success actions require a task subject",
            })
        };

        match post_success["status"].as_str() {
            Some("conflict") => {
                let reason = post_success_failure_reason(&post_success)
                    .unwrap_or_else(|| "post-success merge conflict".to_string());
                workflow = hub.workflows().mark_merge_conflict(&workflow.id, reason).await?;
                reported_workflow_status = workflow.status;
            }
            Some("failed") => {
                let reason = post_success_failure_reason(&post_success)
                    .unwrap_or_else(|| "post-success action failed".to_string());
                workflow = hub.workflows().mark_completed_failed(&workflow.id, reason).await?;
                reported_workflow_status = workflow.status;
            }
            _ => {}
        }
    }

    match reported_workflow_status {
        WorkflowStatus::Completed => emit_runtime(
            RuntimeWorkflowEventKind::WorkflowCompleted,
            serde_json::json!({ "final_status": "completed" }),
        ),
        WorkflowStatus::Failed | WorkflowStatus::Escalated => emit_runtime(
            RuntimeWorkflowEventKind::WorkflowFailed,
            serde_json::json!({
                "final_status": format!("{:?}", reported_workflow_status).to_ascii_lowercase(),
            }),
        ),
        _ => {}
    }

    Ok(WorkflowExecuteResult {
        success: workflow_exit_success(reported_workflow_status),
        workflow_id: workflow.id.clone(),
        workflow_ref,
        workflow_status: reported_workflow_status,
        subject_id: subject_id_str,
        execution_cwd,
        phases_requested: phases_to_run.clone(),
        phases_completed: workflow.phases.iter().filter(|phase| phase.completed_at.is_some()).count(),
        phases_total: phases_to_run.len(),
        total_duration,
        phase_results: results,
        post_success,
    })
}

async fn load_existing_workflow(
    hub: Arc<dyn ServiceHub>,
    workflow_id: &str,
    params: &WorkflowExecuteParams,
) -> Result<OrchestratorWorkflow> {
    let workflow =
        hub.workflows().get(workflow_id).await.with_context(|| format!("workflow '{}' not found", workflow_id))?;

    if workflow.status != WorkflowStatus::Running {
        return Err(anyhow!(
            "workflow '{}' is not runnable (status: {})",
            workflow_id,
            format!("{:?}", workflow.status).to_ascii_lowercase()
        ));
    }

    validate_existing_workflow_subject(&workflow, params)?;
    Ok(workflow)
}

fn validate_existing_workflow_subject(workflow: &OrchestratorWorkflow, params: &WorkflowExecuteParams) -> Result<()> {
    if let Some(task_id) = params.task_id.as_deref() {
        let workflow_task_id = workflow.subject.task_id().unwrap_or(workflow.task_id.as_str());
        if workflow_task_id != task_id {
            return Err(anyhow!("workflow '{}' is for task '{}' not '{}'", workflow.id, workflow_task_id, task_id));
        }
    }

    if let Some(requirement_id) = params.requirement_id.as_deref() {
        match workflow.subject.requirement_id() {
            Some(id) if id == requirement_id => {}
            Some(id) => {
                return Err(anyhow!("workflow '{}' is for requirement '{}' not '{}'", workflow.id, id, requirement_id));
            }
            None => {
                return Err(anyhow!("workflow '{}' is not a requirement workflow", workflow.id));
            }
        }
    }

    if let Some(title) = params.title.as_deref() {
        if !workflow.subject.kind().eq_ignore_ascii_case(SUBJECT_KIND_CUSTOM) {
            return Err(anyhow!("workflow '{}' is not a custom workflow", workflow.id));
        }
        let actual = workflow.subject.title.as_deref().unwrap_or_else(|| workflow.subject.id());
        if actual != title {
            return Err(anyhow!("workflow '{}' is for custom subject '{}' not '{}'", workflow.id, actual, title));
        }
    }

    Ok(())
}

fn resolve_input(params: &WorkflowExecuteParams) -> Result<WorkflowRunInput> {
    let workflow_ref = params.workflow_ref.clone();
    match (&params.task_id, &params.requirement_id, &params.title) {
        (Some(task_id), _, _) => Ok(WorkflowRunInput::for_task(task_id.clone(), workflow_ref)
            .with_input(params.input.clone())
            .with_vars(params.vars.clone())),
        (None, Some(req_id), _) => Ok(WorkflowRunInput::for_requirement(req_id.clone(), workflow_ref)
            .with_input(params.input.clone())
            .with_vars(params.vars.clone())),
        (None, None, Some(title)) => Ok(WorkflowRunInput::for_custom(
            title.clone(),
            params.description.clone().unwrap_or_default(),
            workflow_ref,
        )
        .with_input(params.input.clone())
        .with_vars(params.vars.clone())),
        _ => Err(anyhow!("one of --task-id, --requirement-id, or --title must be provided")),
    }
}

async fn resolve_execution_subject_context(
    hub: Arc<dyn ServiceHub>,
    subject: &SubjectRef,
    fallback_title: Option<&str>,
    fallback_description: Option<&str>,
) -> Result<SubjectContext> {
    hub.subject_resolver()
        .resolve_subject_context(subject, fallback_title, fallback_description)
        .await
        .with_context(|| format!("failed to resolve subject context for '{}'", subject.id()))
}

async fn project_requirement_success_status(
    hub: Arc<dyn ServiceHub>,
    subject: &SubjectRef,
    workflow_ref: &str,
) -> Result<()> {
    let Some(id) = subject.requirement_id() else {
        return Ok(());
    };

    project_requirement_workflow_status(hub, id, workflow_ref).await
}

/// Build the phase-output context string handed to an `llm_judge` eval
/// check. Prefers the structured `result_payload`, falling back to the
/// decision reason; bounded so a large payload does not blow up the judge
/// prompt.
fn phase_eval_context(outcome: &PhaseExecutionOutcome) -> String {
    const MAX_CONTEXT_CHARS: usize = 8_000;
    let raw = match outcome {
        PhaseExecutionOutcome::Completed { result_payload: Some(payload), .. } => {
            serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string())
        }
        PhaseExecutionOutcome::Completed { phase_decision: Some(decision), .. } => decision.reason.clone(),
        _ => String::new(),
    };
    if raw.chars().count() > MAX_CONTEXT_CHARS {
        raw.chars().take(MAX_CONTEXT_CHARS).collect::<String>() + "\n... [truncated]"
    } else {
        raw
    }
}

fn phase_rework_context(outcome: &PhaseExecutionOutcome) -> Option<String> {
    match outcome {
        PhaseExecutionOutcome::Completed { phase_decision: Some(decision), .. }
            if matches!(decision.verdict, PhaseDecisionVerdict::Rework) =>
        {
            Some(decision.reason.clone())
        }
        _ => None,
    }
}

fn is_terminal_workflow_status(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Escalated | WorkflowStatus::Cancelled
    )
}

fn workflow_exit_success(status: WorkflowStatus) -> bool {
    !matches!(status, WorkflowStatus::Failed | WorkflowStatus::Escalated | WorkflowStatus::Cancelled)
}

fn phase_result_status(outcome: &PhaseExecutionOutcome) -> &'static str {
    match outcome {
        PhaseExecutionOutcome::Completed { phase_decision: Some(decision), .. } => match decision.verdict {
            PhaseDecisionVerdict::Advance | PhaseDecisionVerdict::Unknown => "completed",
            PhaseDecisionVerdict::Rework => "rework",
            PhaseDecisionVerdict::Fail => "failed",
            PhaseDecisionVerdict::Skip => "closed",
        },
        PhaseExecutionOutcome::Completed { phase_decision: None, .. } => "completed",
        PhaseExecutionOutcome::ManualPending { .. } => "manual_pending",
    }
}

// Codex round 8 P2 #1: single-phase (`--phase` filter) write path. Persists
// the phase JSON output for inspection but deliberately does NOT write the
// `<phase>.attempt-N.completed` marker. The completion marker is consulted
// by crash-recovery in full-workflow runs to decide whether to skip a phase
// (codex round-4 P1); writing it from an ad-hoc single-phase invocation
// would poison the next full run by tricking it into skipping a phase the
// workflow state machine never actually completed. The JSON shape mirrors
// `persist_phase_output` exactly so any reader that does open the file gets
// the same fields, just without the recovery breadcrumb.
fn persist_phase_output_without_marker(
    project_root: &str,
    workflow_id: &str,
    phase_id: &str,
    outcome: &PhaseExecutionOutcome,
) -> anyhow::Result<()> {
    let dir = phase_output_dir(project_root, workflow_id);
    std::fs::create_dir_all(&dir)?;

    let (verdict, confidence, reason, commit_message, evidence, guardrail_violations, payload) = match outcome {
        PhaseExecutionOutcome::Completed { commit_message, phase_decision, result_payload } => {
            let (v, c, r, ev, gv) = match phase_decision {
                Some(decision) => (
                    Some(format!("{:?}", decision.verdict).to_ascii_lowercase()),
                    Some(decision.confidence),
                    if decision.reason.is_empty() { None } else { Some(decision.reason.clone()) },
                    decision.evidence.clone(),
                    decision.guardrail_violations.clone(),
                ),
                None => (Some("advance".to_string()), None, None, Vec::new(), Vec::new()),
            };
            (v, c, r, commit_message.clone(), ev, gv, result_payload.clone())
        }
        PhaseExecutionOutcome::ManualPending { instructions, .. } => {
            (Some("manual_pending".to_string()), None, Some(instructions.clone()), None, Vec::new(), Vec::new(), None)
        }
    };

    let output = PersistedPhaseOutput {
        phase_id: phase_id.to_string(),
        completed_at: chrono::Utc::now().to_rfc3339(),
        verdict,
        confidence,
        reason,
        commit_message,
        evidence,
        guardrail_violations,
        payload,
    };

    let serialized = serde_json::to_string_pretty(&output)?;
    let file_path = dir.join(format!("{phase_id}.json"));
    let tmp_path = file_path.with_file_name(format!("{phase_id}.{}.tmp", uuid::Uuid::new_v4()));
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(serialized.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, &file_path)?;
    Ok(())
}

fn post_success_failure_reason(post_success: &Value) -> Option<String> {
    post_success
        .get("error")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| post_success.get("reason").and_then(Value::as_str).map(ToOwned::to_owned))
        .or_else(|| {
            post_success.get("actions").and_then(Value::as_object).and_then(|actions| {
                actions.values().find_map(|action| {
                    if action.get("status").and_then(Value::as_str) == Some("failed")
                        || action.get("status").and_then(Value::as_str) == Some("conflict")
                    {
                        action.get("error").and_then(Value::as_str).map(ToOwned::to_owned)
                    } else {
                        None
                    }
                })
            })
        })
}

async fn execute_post_success_actions(
    project_root: &str,
    task: &OrchestratorTask,
    workflow: &OrchestratorWorkflow,
    workflow_config: &orchestrator_core::WorkflowConfig,
    execution_cwd: &str,
) -> Value {
    let workflow_ref = workflow.workflow_ref.as_deref().unwrap_or(workflow_config.default_workflow_ref.as_str());
    let workflow_def = workflow_config
        .workflows
        .iter()
        .find(|p| p.id.eq_ignore_ascii_case(workflow_ref))
        .or_else(|| workflow_config.workflows.iter().find(|p| p.id.eq_ignore_ascii_case("standard")))
        .or_else(|| {
            workflow_config.workflows.iter().find(|p| p.id.eq_ignore_ascii_case(&workflow_config.default_workflow_ref))
        })
        .cloned();

    let workflow_ref_id = workflow_def.map(|def| def.id).unwrap_or_else(|| workflow_ref.to_string());

    // `post_success.merge` (auto-push / auto-PR / auto-merge / worktree
    // cleanup) was removed from Animus in v0.5.x: the kernel's
    // `WorkflowDefinition` no longer carries a `post_success` block, the
    // `MergeStrategy` config type and the in-kernel `BuiltinGitProvider`
    // were deleted, and the YAML parser now rejects `post_success.merge`
    // outright. Merge/PR behavior is no longer a workflow-runner
    // responsibility, so post-success actions are a no-op.
    let _ = (project_root, task, execution_cwd);
    serde_json::json!({
        "status": "skipped",
        "reason": "post_success actions removed in v0.5.x (merge/PR no longer a workflow-runner responsibility)",
        "workflow_ref": workflow_ref_id,
    })
}
