//! Per-phase eval gates. Evals are quality checks declared on a phase
//! (`phases.<id>.evals`) that run AFTER the phase produces an advancing
//! decision. Each check is either a `command` (spawn a process, compare its
//! exit code to `expected_exit`) or an `llm_judge` (dispatch the named agent
//! with a prompt + the phase output and read a PASS/FAIL verdict). The pass
//! rate (`passed / total`) is compared to `pass_threshold`; on a miss the
//! `on_fail` policy decides whether to rework the phase (bounded by
//! `max_reworks`) or block it for human intervention.
//!
//! Schema (read-only) lives in `animus-config-protocol::agent_types`
//! (`EvalsConfig`, `EvalCheck`, `EvalKind`, `EvalOnFail`); validation of the
//! cross-kind field contract is performed at config-compile time by
//! `orchestrator_config::agent_runtime_config::validate_evals_block_runtime`,
//! so this runtime path trusts the declared shape and focuses on execution.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::process::Command as TokioCommand;
use tokio::time::timeout;

use animus_actor::Actor;
use animus_session_backend::session::{SessionEvent, SessionRequest};
use orchestrator_config::agent_runtime_config::{EvalCheck, EvalKind, EvalOnFail, EvalsConfig};
use orchestrator_plugin_host::session::SessionBackendResolver;

use crate::config_context::RuntimeConfigContext;
use crate::phase_executor::PhaseExecutionOutcome;

/// Bounded default applied to a command eval check that omits `timeout_secs`,
/// so a hung command cannot freeze the workflow before the `on_fail` gate
/// fires. (10 minutes — generous enough for slow test/lint suites.)
const DEFAULT_COMMAND_CHECK_TIMEOUT_SECS: u64 = 600;

/// Result of running a single eval check.
#[derive(Debug, Clone)]
pub struct EvalCheckResult {
    pub id: String,
    pub kind: &'static str,
    pub passed: bool,
    pub detail: String,
}

/// Aggregate report for all checks in a phase's eval gate.
#[derive(Debug, Clone)]
pub struct EvalRunReport {
    pub results: Vec<EvalCheckResult>,
    pub passed: usize,
    pub total: usize,
    pub pass_rate: f32,
}

impl EvalRunReport {
    fn from_results(results: Vec<EvalCheckResult>) -> Self {
        let total = results.len();
        let passed = results.iter().filter(|r| r.passed).count();
        // An empty check list is treated as a vacuous pass (callers only
        // invoke the gate when `checks` is non-empty, so this is defensive).
        let pass_rate = if total == 0 { 1.0 } else { passed as f32 / total as f32 };
        Self { results, passed, total, pass_rate }
    }

    /// JSON summary recorded on the phase result so the eval outcome is
    /// observable in `phase_results` + the durable workflow-events JSONL.
    pub fn to_json(&self) -> Value {
        json!({
            "passed": self.passed,
            "total": self.total,
            "pass_rate": self.pass_rate,
            "checks": self
                .results
                .iter()
                .map(|r| {
                    json!({
                        "id": r.id,
                        "kind": r.kind,
                        "passed": r.passed,
                        "detail": r.detail,
                    })
                })
                .collect::<Vec<_>>(),
        })
    }

    fn failed_summary(&self) -> String {
        let failed: Vec<String> =
            self.results.iter().filter(|r| !r.passed).map(|r| format!("{} ({})", r.id, r.detail)).collect();
        if failed.is_empty() {
            "no individual check failures recorded".to_string()
        } else {
            failed.join("; ")
        }
    }
}

/// Decision produced by the eval gate once a report is in hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalGateDecision {
    /// Pass-rate met the threshold; the phase advances normally.
    Pass,
    /// Pass-rate missed; rework the current phase with the failure context.
    Rework { reason: String },
    /// Pass-rate missed and rework is unavailable/exhausted; block for a human.
    Block { reason: String },
}

/// Pure gate decision. `eval_reworks_used` is the number of eval-driven
/// reworks ALREADY consumed for this phase (distinct from the workflow state
/// machine's own rework budget). When `on_fail = rework` and the eval rework
/// budget (`max_reworks`) is exhausted, the gate falls through to `Block`.
pub fn decide_eval_gate(evals: &EvalsConfig, report: &EvalRunReport, eval_reworks_used: u32) -> EvalGateDecision {
    // Comparison mirrors the schema contract: advance when
    // `pass_rate >= pass_threshold`. Both sides are f32 so a 3/3 == 1.0
    // exact pass holds for the default threshold of 1.0.
    if report.pass_rate >= evals.pass_threshold {
        return EvalGateDecision::Pass;
    }

    let reason = format!(
        "eval gate failed: pass_rate {:.2} < threshold {:.2} ({}/{} checks passed) — {}",
        report.pass_rate,
        evals.pass_threshold,
        report.passed,
        report.total,
        report.failed_summary()
    );

    match evals.on_fail {
        EvalOnFail::Rework if eval_reworks_used < evals.max_reworks => EvalGateDecision::Rework { reason },
        EvalOnFail::Rework => EvalGateDecision::Block {
            reason: format!("{reason}; eval rework budget exhausted ({eval_reworks_used}/{} used)", evals.max_reworks),
        },
        EvalOnFail::Block => EvalGateDecision::Block { reason },
    }
}

/// Mutate a `Completed` outcome's decision into a `Rework` verdict carrying
/// the eval failure `reason`, so the workflow state machine reworks the
/// current phase (and the reason is injected into the next attempt's prompt
/// via `phase_rework_context`). A no-op for non-`Completed` outcomes.
pub fn force_rework(outcome: &mut PhaseExecutionOutcome, phase_id: &str, reason: String) {
    if let PhaseExecutionOutcome::Completed { phase_decision, .. } = outcome {
        match phase_decision {
            Some(decision) => {
                decision.verdict = orchestrator_core::PhaseDecisionVerdict::Rework;
                decision.reason = reason;
                // Clear any agent-selected `target_phase` from the original
                // advancing decision: an eval-driven rework must re-run THIS
                // phase, not whatever forward target the agent proposed. In
                // routed workflows that allow agent-selected rework targets a
                // stale target would otherwise redirect the rework elsewhere.
                decision.target_phase = None;
            }
            None => {
                *phase_decision =
                    Some(synthetic_eval_decision(phase_id, orchestrator_core::PhaseDecisionVerdict::Rework, reason));
            }
        }
    }
}

fn synthetic_eval_decision(
    phase_id: &str,
    verdict: orchestrator_core::PhaseDecisionVerdict,
    reason: String,
) -> orchestrator_core::PhaseDecision {
    orchestrator_core::PhaseDecision {
        kind: "phase_decision".to_string(),
        phase_id: phase_id.to_string(),
        verdict,
        verdict_key: None,
        confidence: 1.0,
        risk: orchestrator_core::WorkflowDecisionRisk::Medium,
        reason,
        evidence: vec![orchestrator_core::PhaseEvidence {
            kind: orchestrator_core::PhaseEvidenceKind::Custom,
            description: "eval gate verdict".to_string(),
            file_path: None,
            value: None,
        }],
        guardrail_violations: vec![],
        commit_message: None,
        target_phase: None,
    }
}

/// Run every check declared on the phase's eval gate and aggregate the
/// outcome. Checks run sequentially in declaration order. A check that
/// cannot execute (spawn failure, judge dispatch failure, timeout) is
/// recorded as FAILED so an inoperable gate never silently passes.
pub async fn run_phase_evals(
    project_root: &str,
    execution_cwd: &str,
    ctx: &RuntimeConfigContext,
    evals: &EvalsConfig,
    phase_context: &str,
    actor: Option<&Actor>,
) -> EvalRunReport {
    let mut results = Vec::with_capacity(evals.checks.len());
    for check in &evals.checks {
        let result = match check.kind {
            EvalKind::Command => run_command_check(project_root, ctx, execution_cwd, check).await,
            EvalKind::LlmJudge => {
                run_llm_judge_check(project_root, execution_cwd, ctx, check, phase_context, actor).await
            }
        };
        results.push(result);
    }
    EvalRunReport::from_results(results)
}

fn program_is_allowlisted(program: &str, allowlist: &[String]) -> bool {
    // Empty allowlist means "no restriction" — matches the config validator,
    // which only enforces the allowlist when it is non-empty.
    if allowlist.is_empty() {
        return true;
    }
    let command =
        Path::new(program).file_name().and_then(|value| value.to_str()).unwrap_or(program).trim().to_ascii_lowercase();
    if command.is_empty() {
        return false;
    }
    allowlist
        .iter()
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .any(|candidate| candidate.eq_ignore_ascii_case(command.as_str()))
}

/// Resolve a command check's working directory. `working_dir` is optional;
/// when absent the phase execution cwd is used. `{{name}}` template tokens
/// (`{{project_root}}`, `{{repo_root}}`, `{{execution_cwd}}`) are expanded
/// first via `orchestrator_config::expand_variables` — the same `{{...}}`
/// templating the command-phase `cwd_path` honors — so a `{{project_root}}`
/// form lands at the project root rather than a literal relative path. A
/// relative result is joined onto the execution cwd; an absolute one is used
/// verbatim.
fn resolve_check_cwd(project_root: &str, execution_cwd: &str, working_dir: Option<&str>) -> PathBuf {
    let template_vars = std::collections::HashMap::from([
        ("project_root".to_string(), project_root.to_string()),
        ("repo_root".to_string(), project_root.to_string()),
        ("execution_cwd".to_string(), execution_cwd.to_string()),
    ]);
    match working_dir.map(str::trim).filter(|value| !value.is_empty()) {
        Some(dir) => {
            let expanded = orchestrator_config::expand_variables(dir, &template_vars);
            let expanded = expanded.trim();
            let candidate = Path::new(expanded);
            if candidate.is_absolute() {
                candidate.to_path_buf()
            } else {
                Path::new(execution_cwd).join(candidate)
            }
        }
        None => PathBuf::from(execution_cwd),
    }
}

async fn run_command_check(
    project_root: &str,
    ctx: &RuntimeConfigContext,
    execution_cwd: &str,
    check: &EvalCheck,
) -> EvalCheckResult {
    let id = check.id.clone();
    let program = check.command.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let Some(program) = program else {
        return EvalCheckResult {
            id,
            kind: "command",
            passed: false,
            detail: "command check is missing a command program".to_string(),
        };
    };

    if !program_is_allowlisted(program, &ctx.agent_runtime_config.tools_allowlist) {
        return EvalCheckResult {
            id,
            kind: "command",
            passed: false,
            detail: format!("command '{program}' is not in tools_allowlist"),
        };
    }

    // TODO(codex-p2): pack-relative eval command assets (e.g. `assets/check.sh`
    // supplied by an installed workflow pack) are not rewritten by the pack
    // asset resolver here — only phase command/tool assets are resolved
    // upstream. Such a relative program is spawned as-is from the execution
    // cwd. The common case (allowlisted programs on PATH like `cargo`/`sh`, or
    // absolute paths) works; wiring pack-asset resolution into eval commands is
    // deferred since it requires threading the per-phase resolved pack root.
    let cwd = resolve_check_cwd(project_root, execution_cwd, check.working_dir.as_deref());
    let mut command = TokioCommand::new(program);
    command
        .args(&check.args)
        .current_dir(&cwd)
        .env_remove("CLAUDECODE")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            return EvalCheckResult {
                id,
                kind: "command",
                passed: false,
                detail: format!("failed to spawn '{program}': {err}"),
            };
        }
    };

    // A command check with no (or zero) `timeout_secs` still gets a bounded
    // default so a hung test/linter can never freeze the workflow before the
    // `on_fail` gate (rework/block) gets a chance to fire.
    let secs = check.timeout_secs.filter(|s| *s > 0).unwrap_or(DEFAULT_COMMAND_CHECK_TIMEOUT_SECS);
    let status = match timeout(Duration::from_secs(secs), child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            let _ = child.kill().await;
            return EvalCheckResult {
                id,
                kind: "command",
                passed: false,
                detail: format!("command '{program}' timed out after {secs}s"),
            };
        }
    };

    match status {
        Ok(status) => {
            let exit_code = status.code().unwrap_or(-1);
            let passed = exit_code == check.expected_exit;
            EvalCheckResult {
                id,
                kind: "command",
                passed,
                detail: format!("exit {exit_code} (expected {})", check.expected_exit),
            }
        }
        Err(err) => EvalCheckResult {
            id,
            kind: "command",
            passed: false,
            detail: format!("failed to await '{program}': {err}"),
        },
    }
}

/// First non-empty line's leading token is exactly `PASS` (case-insensitive,
/// ignoring trailing punctuation) => pass. Token-based so verdicts like
/// `PASSIVE` or `PASSAGE` do NOT count as a pass.
pub fn judge_text_passes(text: &str) -> bool {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .and_then(|line| line.split(|c: char| c.is_whitespace() || c == ':' || c == '.' || c == ',' || c == '-').next())
        .map(|token| token.eq_ignore_ascii_case("PASS"))
        .unwrap_or(false)
}

async fn run_llm_judge_check(
    project_root: &str,
    execution_cwd: &str,
    ctx: &RuntimeConfigContext,
    check: &EvalCheck,
    phase_context: &str,
    actor: Option<&Actor>,
) -> EvalCheckResult {
    let id = check.id.clone();
    let agent_id = check.agent.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let Some(agent_id) = agent_id else {
        return EvalCheckResult {
            id,
            kind: "llm_judge",
            passed: false,
            detail: "llm_judge check is missing an agent".to_string(),
        };
    };
    let prompt_template = check.prompt.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let Some(prompt_template) = prompt_template else {
        return EvalCheckResult {
            id,
            kind: "llm_judge",
            passed: false,
            detail: "llm_judge check is missing a prompt".to_string(),
        };
    };

    let profile = ctx.agent_runtime_config.agent_profile(agent_id);
    let tool = profile
        .and_then(|p| p.tool.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("claude")
        .to_string();
    let model = profile
        .and_then(|p| p.model.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("")
        .to_string();

    let prompt = format!(
        "{prompt_template}\n\n--- Phase output under review ---\n{phase_context}\n--- End phase output ---\n\nRespond with PASS or FAIL on the first line, then a one-line justification."
    );

    let request = SessionRequest {
        tool,
        model,
        prompt,
        cwd: PathBuf::from(execution_cwd),
        project_root: Some(PathBuf::from(project_root)),
        // Relay the transport-asserted actor verbatim so the judge session
        // runs as the same user as the phase under review. The runner never
        // interprets it.
        actor: actor.cloned(),
        mcp_endpoint: None,
        // llm_judge runs a self-contained PASS/FAIL review with no tool access.
        mcp_servers: None,
        permission_mode: None,
        // llm_judge checks carry no command-style timeout; fall back to the
        // judge agent profile's `timeout_secs` so a stalled judge session is
        // bounded rather than hanging indefinitely.
        timeout_secs: check.timeout_secs.or_else(|| profile.and_then(|p| p.timeout_secs)),
        env_vars: Vec::new(),
        extras: Value::Object(serde_json::Map::new()),
    };

    let mut session_run =
        match SessionBackendResolver::with_plugin_discovery(Path::new(project_root)).start_session(request).await {
            Ok(run) => run,
            Err(err) => {
                return EvalCheckResult {
                    id,
                    kind: "llm_judge",
                    passed: false,
                    detail: format!("failed to start judge agent '{agent_id}': {err}"),
                };
            }
        };

    let mut accumulated = String::new();
    let mut errored: Option<String> = None;
    while let Some(event) = session_run.events.recv().await {
        match event {
            SessionEvent::TextDelta { text } | SessionEvent::FinalText { text } => accumulated.push_str(&text),
            SessionEvent::Error { message, recoverable } => {
                if !recoverable {
                    errored = Some(message);
                    break;
                }
            }
            SessionEvent::Finished { .. } => break,
            _ => {}
        }
    }

    if let Some(message) = errored {
        return EvalCheckResult {
            id,
            kind: "llm_judge",
            passed: false,
            detail: format!("judge agent '{agent_id}' errored: {message}"),
        };
    }

    let passed = judge_text_passes(&accumulated);
    let verdict_line = accumulated.lines().map(str::trim).find(|line| !line.is_empty()).unwrap_or("<no output>");
    EvalCheckResult { id, kind: "llm_judge", passed, detail: format!("judge '{agent_id}' verdict: {verdict_line}") }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_config::agent_runtime_config::EvalCheck;

    fn command_check(id: &str, program: &str, args: &[&str], expected_exit: i32) -> EvalCheck {
        EvalCheck {
            id: id.to_string(),
            kind: EvalKind::Command,
            command: Some(program.to_string()),
            args: args.iter().map(|a| a.to_string()).collect(),
            working_dir: None,
            timeout_secs: None,
            expected_exit,
            agent: None,
            prompt: None,
        }
    }

    fn evals(on_fail: EvalOnFail, max_reworks: u32, pass_threshold: f32, checks: Vec<EvalCheck>) -> EvalsConfig {
        EvalsConfig { pass_threshold, on_fail, max_reworks, checks }
    }

    #[test]
    fn judge_text_passes_reads_first_nonempty_line() {
        assert!(judge_text_passes("PASS\nlooks good"));
        assert!(judge_text_passes("\n\n  pass: nice work"));
        assert!(!judge_text_passes("FAIL\nmissing tests"));
        assert!(!judge_text_passes("the verdict is PASS")); // PASS not at line start
        assert!(!judge_text_passes("PASSIVE voice detected")); // token must be exactly PASS
        assert!(!judge_text_passes("PASSAGE looks fine"));
        assert!(!judge_text_passes(""));
    }

    #[test]
    fn decide_pass_when_threshold_met() {
        let cfg = evals(EvalOnFail::Block, 0, 1.0, vec![command_check("c", "true", &[], 0)]);
        let report = EvalRunReport::from_results(vec![EvalCheckResult {
            id: "c".into(),
            kind: "command",
            passed: true,
            detail: "exit 0".into(),
        }]);
        assert_eq!(decide_eval_gate(&cfg, &report, 0), EvalGateDecision::Pass);
    }

    #[test]
    fn decide_rework_then_block_on_exhaustion() {
        let cfg = evals(EvalOnFail::Rework, 1, 1.0, vec![command_check("c", "false", &[], 0)]);
        let report = EvalRunReport::from_results(vec![EvalCheckResult {
            id: "c".into(),
            kind: "command",
            passed: false,
            detail: "exit 1 (expected 0)".into(),
        }]);
        // First miss with budget remaining => rework.
        assert!(matches!(decide_eval_gate(&cfg, &report, 0), EvalGateDecision::Rework { .. }));
        // Budget exhausted (1/1 used) => block.
        assert!(matches!(decide_eval_gate(&cfg, &report, 1), EvalGateDecision::Block { .. }));
    }

    #[test]
    fn decide_block_when_on_fail_block() {
        let cfg = evals(EvalOnFail::Block, 0, 1.0, vec![command_check("c", "false", &[], 0)]);
        let report = EvalRunReport::from_results(vec![EvalCheckResult {
            id: "c".into(),
            kind: "command",
            passed: false,
            detail: "exit 1".into(),
        }]);
        assert!(matches!(decide_eval_gate(&cfg, &report, 0), EvalGateDecision::Block { .. }));
    }

    #[test]
    fn pass_rate_threshold_partial() {
        // 1 of 2 checks passed => pass_rate 0.5.
        let report = EvalRunReport::from_results(vec![
            EvalCheckResult { id: "a".into(), kind: "command", passed: true, detail: String::new() },
            EvalCheckResult { id: "b".into(), kind: "command", passed: false, detail: String::new() },
        ]);
        assert!((report.pass_rate - 0.5).abs() < f32::EPSILON);
        // Threshold 0.5 => pass; threshold 0.75 => fail.
        let pass_cfg = evals(EvalOnFail::Block, 0, 0.5, vec![]);
        let fail_cfg = evals(EvalOnFail::Block, 0, 0.75, vec![]);
        assert_eq!(decide_eval_gate(&pass_cfg, &report, 0), EvalGateDecision::Pass);
        assert!(matches!(decide_eval_gate(&fail_cfg, &report, 0), EvalGateDecision::Block { .. }));
    }

    #[tokio::test]
    async fn command_check_passes_on_expected_exit() {
        let ctx = crate::config_context::tests_support::ctx_with_allowlist(&["true", "false"]);
        let report = run_phase_evals(
            "/",
            "/",
            &ctx,
            &evals(EvalOnFail::Block, 0, 1.0, vec![command_check("ok", "true", &[], 0)]),
            "",
            None,
        )
        .await;
        assert_eq!(report.passed, 1);
        assert!((report.pass_rate - 1.0).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn command_check_fails_on_unexpected_exit() {
        let ctx = crate::config_context::tests_support::ctx_with_allowlist(&["true", "false"]);
        let report = run_phase_evals(
            "/",
            "/",
            &ctx,
            &evals(EvalOnFail::Rework, 1, 1.0, vec![command_check("bad", "false", &[], 0)]),
            "",
            None,
        )
        .await;
        assert_eq!(report.passed, 0);
        assert!((report.pass_rate - 0.0).abs() < f32::EPSILON);
        assert!(matches!(
            decide_eval_gate(&evals(EvalOnFail::Rework, 1, 1.0, vec![]), &report, 0),
            EvalGateDecision::Rework { .. }
        ));
    }

    #[test]
    fn force_rework_mutates_existing_decision() {
        let mut decision =
            synthetic_eval_decision("impl", orchestrator_core::PhaseDecisionVerdict::Advance, "advance".into());
        decision.target_phase = Some("review".into());
        let mut outcome = PhaseExecutionOutcome::Completed {
            commit_message: None,
            phase_decision: Some(decision),
            result_payload: None,
        };
        force_rework(&mut outcome, "impl", "needs more tests".into());
        match outcome {
            PhaseExecutionOutcome::Completed { phase_decision: Some(decision), .. } => {
                assert_eq!(decision.verdict, orchestrator_core::PhaseDecisionVerdict::Rework);
                assert_eq!(decision.reason, "needs more tests");
                assert!(decision.target_phase.is_none(), "eval rework must clear stale target_phase");
            }
            _ => panic!("expected Completed with a decision"),
        }
    }

    #[test]
    fn force_rework_synthesizes_decision_when_absent() {
        let mut outcome =
            PhaseExecutionOutcome::Completed { commit_message: None, phase_decision: None, result_payload: None };
        force_rework(&mut outcome, "impl", "gate failed".into());
        match outcome {
            PhaseExecutionOutcome::Completed { phase_decision: Some(decision), .. } => {
                assert_eq!(decision.verdict, orchestrator_core::PhaseDecisionVerdict::Rework);
                assert_eq!(decision.phase_id, "impl");
            }
            _ => panic!("expected a synthesized rework decision"),
        }
    }

    #[tokio::test]
    async fn command_check_blocked_by_allowlist() {
        let ctx = crate::config_context::tests_support::ctx_with_allowlist(&["cargo"]);
        let report = run_phase_evals(
            "/",
            "/",
            &ctx,
            &evals(EvalOnFail::Block, 0, 1.0, vec![command_check("x", "true", &[], 0)]),
            "",
            None,
        )
        .await;
        assert_eq!(report.passed, 0);
        assert!(report.results[0].detail.contains("tools_allowlist"));
    }
}
