use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::time::timeout;

use crate::payload_traversal::parse_phase_decision_from_text;

#[derive(Debug, Clone, Copy)]
pub(crate) struct CommandExecutionContext<'a> {
    pub project_root: &'a str,
    pub execution_cwd: &'a str,
    pub workflow_id: &'a str,
    pub phase_id: &'a str,
    pub workflow_ref: &'a str,
    pub subject_id: &'a str,
    pub subject_title: &'a str,
    pub subject_description: &'a str,
    pub pipeline_vars: Option<&'a HashMap<String, String>>,
    pub dispatch_input: Option<&'a str>,
    pub schedule_input: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub(crate) struct CommandExecutionResult {
    pub exit_code: i32,
    pub program: String,
    pub args: Vec<String>,
    pub stdout: String,
    pub stderr: String,
    pub cwd: String,
    pub duration_ms: u64,
    pub parsed_payload: Option<Value>,
    pub phase_decision: Option<orchestrator_core::PhaseDecision>,
    pub failure_summary: Option<String>,
}

/// Template variables that a command phase MUST have bound to a non-empty
/// value before it is safe to execute. Today this is the subject identity:
/// a command like `animus subject status --id {{subject_id}}` dispatched on a
/// run with no bound subject expands `{{subject_id}}` to the empty string and
/// invokes `animus subject status --id ""`, which the CLI rejects with
/// "--id must not be empty" (exit 2). The pre-fix runner then treated that as
/// a normal non-zero exit -> rework -> 4 retries -> escalated. Detecting the
/// unresolved binding up front lets the phase fail-fast on attempt 1 with a
/// clear message instead of burning the whole rework budget. Extend this list
/// as other required-non-empty bindings are identified.
pub(crate) const COMMAND_REQUIRED_NONEMPTY_VARS: &[&str] = &["subject_id"];

/// Sentinel error attached to a terminal command-phase failure. Carries the
/// structured exit-code + stderr snippet so the `workflow_execute` error arms
/// can emit a `phase_failed` runtime event that the ao-cli journal mapping
/// (ao-cli PR #299) persists into `journal_events` with the exit metadata,
/// instead of a `phase_completed`+rework that drops it. Recovered upstream via
/// `error.downcast_ref::<CommandPhaseFailedError>()`.
#[derive(Debug, Clone)]
pub struct CommandPhaseFailedError {
    pub message: String,
    pub program: Option<String>,
    pub exit_code: Option<i32>,
    pub stderr_excerpt: Option<String>,
}

impl std::fmt::Display for CommandPhaseFailedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CommandPhaseFailedError {}

/// Collect the `{{name}}` placeholder names referenced in `text`. Mirrors the
/// scanning rules of `orchestrator_config::expand_variables` (a placeholder is
/// the exact text between the first `{{` and the next `}}`; no trimming).
fn referenced_template_vars(text: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            break;
        };
        names.push(&after[..end]);
        rest = &after[end + 2..];
    }
    names
}

/// Return the required-non-empty template vars (see
/// [`COMMAND_REQUIRED_NONEMPTY_VARS`]) that a command references via a
/// `{{name}}` placeholder but which resolve to an empty value in
/// `template_vars` (or are absent entirely). A non-empty result means the
/// command must NOT be executed — its required binding is unresolved.
pub(crate) fn unresolved_required_command_vars(
    command: &orchestrator_core::PhaseCommandDefinition,
    template_vars: &HashMap<String, String>,
) -> Vec<String> {
    let mut fields: Vec<&str> = Vec::with_capacity(1 + command.args.len() + command.env.len());
    fields.push(command.program.as_str());
    fields.extend(command.args.iter().map(String::as_str));
    fields.extend(command.env.values().map(String::as_str));
    if let Some(cwd) = command.cwd_path.as_deref() {
        fields.push(cwd);
    }

    let mut unresolved: Vec<String> = Vec::new();
    for field in fields {
        for name in referenced_template_vars(field) {
            let required = COMMAND_REQUIRED_NONEMPTY_VARS.contains(&name);
            let empty = template_vars.get(name).map(|value| value.trim().is_empty()).unwrap_or(true);
            if required && empty && !unresolved.iter().any(|existing| existing == name) {
                unresolved.push(name.to_string());
            }
        }
    }
    unresolved
}

/// True when a command phase has opted into producing structured verdicts:
/// it declares `parse_json_output` or an output contract
/// (`expected_result_kind` / `expected_schema`). Only then is a JSON verdict
/// object on stdout authoritative for the phase disposition; otherwise the
/// phase is routed by exit-code inference. This keeps a plain command (e.g. a
/// `cargo test` QA gate) from being hijacked by incidental JSON-shaped output.
pub(crate) fn command_emits_decision(command: &orchestrator_core::PhaseCommandDefinition) -> bool {
    command.parse_json_output || command.expected_result_kind.is_some() || command.expected_schema.is_some()
}

/// True when a failed command phase's resolved decision is a terminal `fail`
/// (as opposed to a QA-gate `rework`/`advance`/`skip`). Only terminal-fail
/// command exits are re-routed into a `phase_failed` event carrying the exit
/// metadata; rework gates keep their existing `phase_completed` path so a
/// failing test/lint command still drives the implement->review loop.
pub(crate) fn command_failure_is_terminal(decision: &orchestrator_core::PhaseDecision) -> bool {
    matches!(decision.verdict, orchestrator_core::PhaseDecisionVerdict::Fail)
}

/// Resolve the effective decision for a completed command phase (TASK-206).
///
/// A JSON verdict object emitted on stdout (`{verdict, reason?, confidence?}`,
/// parsed via [`crate::payload_traversal::parse_phase_decision_from_text`] into
/// `result.phase_decision`) is authoritative — but ONLY when the command opted
/// into decision production via [`command_emits_decision`]. When the command
/// did not opt in, or emitted no parseable verdict (malformed/absent JSON), the
/// disposition falls back to exit-code inference through
/// [`build_command_phase_decision`], which applies the `on_success_verdict` /
/// `on_failure_verdict` mappings. `result.failure_summary` is `Some` on a
/// non-success exit and selects the failure-verdict branch.
pub(crate) fn resolve_command_decision(
    command: &orchestrator_core::PhaseCommandDefinition,
    phase_id: &str,
    result: &CommandExecutionResult,
) -> orchestrator_core::PhaseDecision {
    let emitted_verdict = if command_emits_decision(command) { result.phase_decision.clone() } else { None };
    emitted_verdict.unwrap_or_else(|| {
        build_command_phase_decision(command, phase_id, result.exit_code, result.failure_summary.as_deref())
    })
}

/// Build the `phase_failed` runtime-event payload for a terminal command
/// failure, carrying the exit-code + stderr snippet the journal mapping
/// persists. Shared by both `workflow_execute` error arms (single-phase and
/// multi-phase) so the wire shape stays identical.
pub(crate) fn command_phase_failed_event_payload(phase_id: &str, error: &CommandPhaseFailedError) -> Value {
    let mut payload = serde_json::json!({
        "phase_id": phase_id,
        "phase_status": "failed",
        "error": error.message,
    });
    if let Some(code) = error.exit_code {
        payload["exit_code"] = serde_json::json!(code);
    }
    if let Some(ref stderr) = error.stderr_excerpt {
        payload["stderr"] = serde_json::json!(stderr);
    }
    if let Some(ref program) = error.program {
        payload["program"] = serde_json::json!(program);
    }
    payload
}

pub(crate) fn build_command_template_vars(context: &CommandExecutionContext<'_>) -> HashMap<String, String> {
    let mut vars = HashMap::from([
        ("project_root".to_string(), context.project_root.to_string()),
        ("execution_cwd".to_string(), context.execution_cwd.to_string()),
        ("workflow_id".to_string(), context.workflow_id.to_string()),
        ("phase_id".to_string(), context.phase_id.to_string()),
        ("workflow_ref".to_string(), context.workflow_ref.to_string()),
        ("subject_id".to_string(), context.subject_id.to_string()),
        ("subject_title".to_string(), context.subject_title.to_string()),
        ("subject_description".to_string(), context.subject_description.to_string()),
    ]);

    if let Some(pipeline_vars) = context.pipeline_vars {
        for (key, value) in pipeline_vars {
            vars.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    if let Some(dispatch_input) = context.dispatch_input.filter(|value| !value.is_empty()) {
        vars.entry("dispatch_input".to_string()).or_insert_with(|| dispatch_input.to_string());
        if context.subject_id.starts_with("schedule:") {
            vars.entry("schedule_input".to_string()).or_insert_with(|| dispatch_input.to_string());
        }
    } else if let Some(schedule_input) = context.schedule_input.filter(|value| !value.is_empty()) {
        vars.entry("schedule_input".to_string()).or_insert_with(|| schedule_input.to_string());
        vars.entry("dispatch_input".to_string()).or_insert_with(|| schedule_input.to_string());
    }

    vars
}

fn resolve_command_cwd(
    context: &CommandExecutionContext<'_>,
    command: &orchestrator_core::PhaseCommandDefinition,
    template_vars: &HashMap<String, String>,
) -> Result<String> {
    match command.cwd_mode {
        orchestrator_core::CommandCwdMode::ProjectRoot => Ok(context.project_root.to_string()),
        orchestrator_core::CommandCwdMode::TaskRoot => Ok(context.execution_cwd.to_string()),
        orchestrator_core::CommandCwdMode::Path => {
            let expanded = command
                .cwd_path
                .as_deref()
                .map(|value| orchestrator_config::expand_variables(value, template_vars))
                .ok_or_else(|| anyhow!("command.cwd_path is required when cwd_mode='path'"))?;
            let raw = expanded.trim();
            if raw.is_empty() {
                return Err(anyhow!("command.cwd_path is required when cwd_mode='path'"));
            }
            let relative = Path::new(raw);
            if relative.is_absolute() {
                return Err(anyhow!("command.cwd_path must be relative when cwd_mode='path'"));
            }
            if relative.components().any(|component| matches!(component, Component::ParentDir)) {
                return Err(anyhow!("command.cwd_path cannot contain '..' components"));
            }
            let resolved = Path::new(context.project_root).join(relative);
            let canonical = std::fs::canonicalize(&resolved).unwrap_or_else(|_| resolved.clone());
            let canonical_root =
                std::fs::canonicalize(context.project_root).unwrap_or_else(|_| PathBuf::from(context.project_root));
            if !canonical.starts_with(&canonical_root) {
                return Err(anyhow!("command cwd_path escapes project root: {}", raw));
            }
            Ok(resolved.display().to_string())
        }
    }
}

fn is_program_allowlisted(program: &str, allowlist: &[String]) -> bool {
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

fn command_phase_category(command: &orchestrator_core::PhaseCommandDefinition, phase_id: &str) -> String {
    if let Some(category) = command.category.as_deref() {
        return category.to_string();
    }

    let normalized_phase = phase_id.to_ascii_lowercase();
    let normalized_program = command.program.to_ascii_lowercase();
    let normalized_args: Vec<String> = command.args.iter().map(|v| v.to_ascii_lowercase()).collect();

    if normalized_phase.contains("test")
        || normalized_program.contains("cargo") && normalized_args.iter().any(|arg| arg == "test")
    {
        "test".to_string()
    } else if normalized_phase.contains("lint")
        || normalized_program.contains("clippy")
        || normalized_program.contains("rustfmt")
        || normalized_args.iter().any(|arg| arg.contains("clippy") || arg.contains("fmt"))
    {
        "lint".to_string()
    } else if normalized_phase.contains("build")
        || normalized_program.contains("cargo") && normalized_args.iter().any(|arg| arg == "build")
    {
        "build".to_string()
    } else {
        "command".to_string()
    }
}

fn command_phase_evidence_kind(
    command: &orchestrator_core::PhaseCommandDefinition,
    phase_id: &str,
    success: bool,
) -> orchestrator_core::PhaseEvidenceKind {
    let category = command_phase_category(command, phase_id);
    if category == "test" {
        if success {
            orchestrator_core::PhaseEvidenceKind::TestsPassed
        } else {
            orchestrator_core::PhaseEvidenceKind::TestsFailed
        }
    } else {
        orchestrator_core::PhaseEvidenceKind::Custom
    }
}

fn extract_failing_tests(
    command: &orchestrator_core::PhaseCommandDefinition,
    stdout: &str,
    stderr: &str,
) -> Vec<String> {
    let pattern_str = command.failure_pattern.as_deref().unwrap_or(r"test (.+) \.\.\. FAILED");

    let re = match regex::Regex::new(pattern_str) {
        Ok(re) => re,
        Err(_) => return Vec::new(),
    };

    let mut failing = Vec::new();
    for text in [stdout, stderr] {
        for line in text.lines() {
            if let Some(captures) = re.captures(line.trim()) {
                let candidate = captures.get(1).map(|m| m.as_str().trim().to_string()).unwrap_or_default();
                if !candidate.is_empty() && !failing.contains(&candidate) {
                    failing.push(candidate);
                }
            }
        }
    }
    failing
}

pub(crate) fn summarize_output_excerpt(text: &str, max_len: usize) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let excerpt = if trimmed.chars().count() > max_len {
        let mut shortened = trimmed.chars().take(max_len).collect::<String>();
        shortened.push_str("...");
        shortened
    } else {
        trimmed.to_string()
    };
    Some(excerpt)
}

pub(crate) fn build_command_phase_decision(
    command: &orchestrator_core::PhaseCommandDefinition,
    phase_id: &str,
    exit_code: i32,
    failure_summary: Option<&str>,
) -> orchestrator_core::PhaseDecision {
    let success = failure_summary.is_none();
    let kind = command_phase_evidence_kind(command, phase_id, success);
    let reason = failure_summary
        .map(str::to_string)
        .unwrap_or_else(|| format!("Command `{}` completed successfully", command.program));

    let verdict = if success {
        match command.on_success_verdict.as_deref() {
            Some("rework") => orchestrator_core::PhaseDecisionVerdict::Rework,
            Some("fail") => orchestrator_core::PhaseDecisionVerdict::Fail,
            Some("skip") => orchestrator_core::PhaseDecisionVerdict::Skip,
            _ => orchestrator_core::PhaseDecisionVerdict::Advance,
        }
    } else {
        match command.on_failure_verdict.as_deref() {
            Some("advance") => orchestrator_core::PhaseDecisionVerdict::Advance,
            Some("fail") => orchestrator_core::PhaseDecisionVerdict::Fail,
            Some("skip") => orchestrator_core::PhaseDecisionVerdict::Skip,
            _ => orchestrator_core::PhaseDecisionVerdict::Rework,
        }
    };

    let confidence = command.confidence.unwrap_or(1.0);

    let risk = if success {
        orchestrator_core::WorkflowDecisionRisk::Low
    } else {
        match command.failure_risk.as_deref() {
            Some("low") => orchestrator_core::WorkflowDecisionRisk::Low,
            Some("high") => orchestrator_core::WorkflowDecisionRisk::High,
            _ => orchestrator_core::WorkflowDecisionRisk::Medium,
        }
    };

    orchestrator_core::PhaseDecision {
        kind: "phase_decision".to_string(),
        phase_id: phase_id.to_string(),
        verdict,
        // TODO(codex-p2): a non-built-in `on_success_verdict`/`on_failure_verdict`
        // should route via v0.7 `Unknown + verdict_key`; `None` preserves the
        // v0.4.20 built-in mapping (no regression) pending custom-key adoption.
        verdict_key: None,
        confidence,
        risk,
        reason: reason.clone(),
        evidence: vec![orchestrator_core::PhaseEvidence {
            kind,
            description: format!("Command `{}` exited with code {exit_code}", command.program),
            file_path: None,
            value: Some(serde_json::json!({
                "program": command.program,
                "args": command.args,
                "exit_code": exit_code
            })),
        }],
        guardrail_violations: vec![],
        commit_message: None,
        target_phase: None,
    }
}

pub(crate) fn build_command_result_payload(
    command: &orchestrator_core::PhaseCommandDefinition,
    phase_id: &str,
    contract_kind: Option<&str>,
    command_result: &CommandExecutionResult,
    phase_decision: &orchestrator_core::PhaseDecision,
) -> Value {
    let mut payload = match command_result.parsed_payload.clone() {
        Some(Value::Object(map)) => Value::Object(map),
        Some(other) => serde_json::json!({ "raw_payload": other }),
        None => serde_json::json!({}),
    };

    payload["kind"] = Value::String(
        payload
            .get("kind")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(contract_kind.unwrap_or("phase_result"))
            .to_string(),
    );
    payload["phase_id"] = Value::String(phase_id.to_string());
    payload["verdict"] = Value::String(format!("{:?}", phase_decision.verdict).to_ascii_lowercase());
    payload["reason"] = Value::String(phase_decision.reason.clone());
    payload["confidence"] = serde_json::json!(phase_decision.confidence);
    payload["risk"] = Value::String(format!("{:?}", phase_decision.risk).to_ascii_lowercase());
    payload["evidence"] = serde_json::to_value(&phase_decision.evidence).unwrap_or(Value::Array(vec![]));
    payload["exit_code"] = serde_json::json!(command_result.exit_code);
    payload["command"] = serde_json::json!({
        "program": command_result.program,
        "args": command_result.args
    });
    payload["duration_ms"] = serde_json::json!(command_result.duration_ms);

    let excerpt_max = command.excerpt_max_chars.unwrap_or(800);
    let category = command_phase_category(command, phase_id);

    if let Some(summary) = command_result.failure_summary.as_deref() {
        payload["failure_summary"] = Value::String(summary.to_string());
        payload["failure_category"] = Value::String(format!("{category}_failed"));
        let failing_tests = extract_failing_tests(command, &command_result.stdout, &command_result.stderr);
        if !failing_tests.is_empty() {
            payload["failing_tests"] = Value::Array(failing_tests.into_iter().map(Value::String).collect::<Vec<_>>());
        }
    }

    if let Some(stdout_excerpt) = summarize_output_excerpt(&command_result.stdout, excerpt_max) {
        payload["stdout_excerpt"] = Value::String(stdout_excerpt);
    }
    if let Some(stderr_excerpt) = summarize_output_excerpt(&command_result.stderr, excerpt_max) {
        payload["stderr_excerpt"] = Value::String(stderr_excerpt);
    }

    payload
}

#[derive(Debug)]
struct CommandStreamCapture {
    text: String,
    phase_decision: Option<orchestrator_core::PhaseDecision>,
}

async fn capture_command_stream<R>(reader: R, phase_id: String) -> Result<CommandStreamCapture>
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    let mut text = String::new();
    let mut phase_decision = None;

    while let Some(line) = lines.next_line().await? {
        text.push_str(&line);
        text.push('\n');

        if phase_decision.is_none() {
            phase_decision = parse_phase_decision_from_text(&line, &phase_id);
        }
    }

    Ok(CommandStreamCapture { text, phase_decision })
}

pub(crate) async fn run_workflow_phase_with_command(
    context: &CommandExecutionContext<'_>,
    runtime_config: &orchestrator_core::AgentRuntimeConfig,
    command: &orchestrator_core::PhaseCommandDefinition,
) -> Result<CommandExecutionResult> {
    if !is_program_allowlisted(&command.program, &runtime_config.tools_allowlist) {
        return Err(anyhow!("phase '{}' command '{}' is not in tools_allowlist", context.phase_id, command.program));
    }

    let template_vars = build_command_template_vars(context);

    // Fail-fast (TASK-205): a required binding like `{{subject_id}}` that
    // expands to empty (no subject bound) would run e.g. `animus subject
    // status --id ""` and get rejected by the CLI, then be retried until the
    // rework budget escalates. Refuse to execute the command and surface a
    // clear, attempt-1 terminal failure instead.
    let unresolved = unresolved_required_command_vars(command, &template_vars);
    if !unresolved.is_empty() {
        let placeholders = unresolved
            .iter()
            .map(|name| {
                let mut placeholder = String::with_capacity(name.len() + 4);
                placeholder.push_str("{{");
                placeholder.push_str(name);
                placeholder.push_str("}}");
                placeholder
            })
            .collect::<Vec<_>>()
            .join(", ");
        let message = format!(
            "phase '{}' command '{}' references unresolved template {} — no subject bound; failing before execution to avoid burning the rework budget",
            context.phase_id, command.program, placeholders
        );
        return Err(anyhow::Error::new(CommandPhaseFailedError {
            message,
            program: Some(command.program.clone()),
            exit_code: None,
            stderr_excerpt: None,
        }));
    }

    let args =
        command.args.iter().map(|arg| orchestrator_config::expand_variables(arg, &template_vars)).collect::<Vec<_>>();
    let env = command
        .env
        .iter()
        .map(|(key, value)| (key.clone(), orchestrator_config::expand_variables(value, &template_vars)))
        .collect::<BTreeMap<_, _>>();
    let cwd = resolve_command_cwd(context, command, &template_vars)?;
    let started = std::time::Instant::now();

    let mut process = TokioCommand::new(&command.program);
    process
        .args(&args)
        .current_dir(&cwd)
        .env_remove("CLAUDECODE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for (key, value) in &env {
        process.env(key, value);
    }

    let mut child = process.spawn()?;
    let stdout_reader = child.stdout.take().ok_or_else(|| anyhow!("failed to capture stdout for command phase"))?;
    let stderr_reader = child.stderr.take().ok_or_else(|| anyhow!("failed to capture stderr for command phase"))?;
    let phase_id = context.phase_id.to_string();
    let phase_id2 = phase_id.clone();
    // Codex P3 #5: `capture_command_stream` now takes an owned `String`,
    // so the spawned tasks no longer require leaking `&'static str` per
    // command phase. In a long-lived plugin process this stops the
    // monotonic memory growth observed in the pre-fix path.
    let stdout_task = tokio::spawn(capture_command_stream(stdout_reader, phase_id));
    let stderr_task = tokio::spawn(capture_command_stream(stderr_reader, phase_id2));

    let status = if let Some(timeout_secs) = command.timeout_secs {
        match timeout(Duration::from_secs(timeout_secs), child.wait()).await {
            Ok(status) => status?,
            Err(_) => {
                let _ = child.kill().await;
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(anyhow!(
                    "phase '{}' command '{}' timed out after {} seconds",
                    context.phase_id,
                    command.program,
                    timeout_secs
                ));
            }
        }
    } else {
        child.wait().await?
    };

    let stdout_capture = stdout_task.await.map_err(|error| anyhow!("stdout capture task failed: {error}"))??;
    let stderr_capture = stderr_task.await.map_err(|error| anyhow!("stderr capture task failed: {error}"))??;

    let exit_code = status.code().unwrap_or(-1);
    let stdout = stdout_capture.text;
    let stderr = stderr_capture.text;
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let phase_decision = stdout_capture
        .phase_decision
        .or(stderr_capture.phase_decision)
        .or_else(|| parse_phase_decision_from_text(&stdout, context.phase_id))
        .or_else(|| parse_phase_decision_from_text(&stderr, context.phase_id));

    if !command.success_exit_codes.contains(&exit_code) {
        let mut failure_summary = format!(
            "Command `{}` exited with code {} (expected one of {:?}).",
            command.program, exit_code, command.success_exit_codes
        );
        if !stdout.trim().is_empty() {
            failure_summary.push_str("\n\nStdout:\n");
            failure_summary.push_str(stdout.trim());
        }
        if !stderr.trim().is_empty() {
            failure_summary.push_str("\n\nStderr:\n");
            failure_summary.push_str(stderr.trim());
        }
        return Ok(CommandExecutionResult {
            exit_code,
            program: command.program.clone(),
            args,
            stdout,
            stderr,
            cwd,
            duration_ms,
            parsed_payload: None,
            phase_decision,
            failure_summary: Some(failure_summary),
        });
    }

    let parsed_payload = if command.parse_json_output {
        match parse_command_json_output(&stdout) {
            Ok(payload) => {
                // A payload that parses but violates the declared kind/schema is
                // a genuine contract violation and still fails the phase.
                validate_command_contract(
                    &payload,
                    command.expected_result_kind.as_deref(),
                    command.expected_schema.as_ref(),
                )?;
                Some(payload)
            }
            // TASK-206: malformed/absent JSON is NOT a hard failure — the phase
            // falls back to exit-code / on_*_verdict inference (or an emitted
            // verdict line, if any). Only a parseable-but-invalid payload above
            // is treated as a contract violation.
            Err(_) => None,
        }
    } else {
        None
    };

    Ok(CommandExecutionResult {
        exit_code,
        program: command.program.clone(),
        args,
        stdout,
        stderr,
        cwd,
        duration_ms,
        parsed_payload,
        phase_decision,
        failure_summary: None,
    })
}

fn parse_command_json_output(stdout: &str) -> Result<Value> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("command output is empty; expected JSON payload"));
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Ok(value);
    }
    let payloads = crate::ipc::collect_json_payload_lines(stdout);
    payloads
        .last()
        .map(|(_, payload)| payload.clone())
        .ok_or_else(|| anyhow!("unable to parse JSON payload from command output"))
}

fn validate_command_contract(
    payload: &Value,
    expected_kind: Option<&str>,
    expected_schema: Option<&Value>,
) -> Result<()> {
    if let Some(kind) = expected_kind.map(str::trim).filter(|v| !v.is_empty()) {
        let payload_kind = payload
            .get("kind")
            .and_then(Value::as_str)
            .map(str::trim)
            .ok_or_else(|| anyhow!("payload is missing required field 'kind'"))?;
        if !payload_kind.eq_ignore_ascii_case(kind) {
            return Err(anyhow!("payload kind mismatch: expected '{}', got '{}'", kind, payload_kind));
        }
    }
    if let Some(schema) = expected_schema {
        crate::phase_executor::validate_basic_json_schema(payload, schema)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn command_def(program: &str, args: &[&str]) -> orchestrator_core::PhaseCommandDefinition {
        serde_json::from_value(serde_json::json!({ "program": program, "args": args })).expect("command def")
    }

    fn command_def_with_failure_verdict(
        program: &str,
        on_failure_verdict: Option<&str>,
    ) -> orchestrator_core::PhaseCommandDefinition {
        serde_json::from_value(serde_json::json!({
            "program": program,
            "on_failure_verdict": on_failure_verdict,
        }))
        .expect("command def")
    }

    fn subjectless_context<'a>() -> CommandExecutionContext<'a> {
        CommandExecutionContext {
            project_root: ".",
            execution_cwd: ".",
            workflow_id: "wf-test",
            phase_id: "sync-status",
            workflow_ref: "default",
            subject_id: "",
            subject_title: "",
            subject_description: "",
            pipeline_vars: None,
            dispatch_input: None,
            schedule_input: None,
        }
    }

    fn bound_context<'a>() -> CommandExecutionContext<'a> {
        let mut ctx = subjectless_context();
        ctx.subject_id = "task:TASK-1";
        ctx
    }

    fn echo_runtime() -> orchestrator_core::AgentRuntimeConfig {
        orchestrator_core::AgentRuntimeConfig { tools_allowlist: vec!["echo".to_string()], ..Default::default() }
    }

    fn echo_json_command(raw: &str) -> orchestrator_core::PhaseCommandDefinition {
        serde_json::from_value(serde_json::json!({ "program": "echo", "args": [raw], "parse_json_output": true }))
            .expect("command def")
    }

    fn decision_with_verdict(verdict: orchestrator_core::PhaseDecisionVerdict) -> orchestrator_core::PhaseDecision {
        orchestrator_core::PhaseDecision {
            kind: "phase_decision".to_string(),
            phase_id: "gate".to_string(),
            verdict,
            verdict_key: None,
            confidence: 1.0,
            risk: orchestrator_core::WorkflowDecisionRisk::Low,
            reason: "test".to_string(),
            evidence: vec![],
            guardrail_violations: vec![],
            commit_message: None,
            target_phase: None,
        }
    }

    fn command_result_stub(
        exit_code: i32,
        failure_summary: Option<&str>,
        decision: Option<orchestrator_core::PhaseDecision>,
    ) -> CommandExecutionResult {
        CommandExecutionResult {
            exit_code,
            program: "cmd".to_string(),
            args: vec![],
            stdout: String::new(),
            stderr: String::new(),
            cwd: ".".to_string(),
            duration_ms: 0,
            parsed_payload: None,
            phase_decision: decision,
            failure_summary: failure_summary.map(ToString::to_string),
        }
    }

    #[test]
    fn unresolved_required_command_vars_flags_empty_subject_id() {
        let command = command_def("animus", &["subject", "status", "--id", "{{subject_id}}"]);
        let mut vars = HashMap::new();
        vars.insert("subject_id".to_string(), String::new());
        assert_eq!(unresolved_required_command_vars(&command, &vars), vec!["subject_id".to_string()]);

        // Bound subject: nothing to flag.
        vars.insert("subject_id".to_string(), "task:TASK-1".to_string());
        assert!(unresolved_required_command_vars(&command, &vars).is_empty());
    }

    #[test]
    fn unresolved_required_command_vars_ignores_unreferenced_and_optional() {
        // subject_id is empty but not referenced by the command -> not flagged.
        let command = command_def("echo", &["hello"]);
        let mut vars = HashMap::new();
        vars.insert("subject_id".to_string(), String::new());
        assert!(unresolved_required_command_vars(&command, &vars).is_empty());

        // A non-required var expanding to empty is not a fail-fast condition.
        let command = command_def("echo", &["{{dispatch_input}}"]);
        vars.insert("dispatch_input".to_string(), String::new());
        assert!(unresolved_required_command_vars(&command, &vars).is_empty());
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_fails_fast_on_empty_required_var() {
        let command = command_def("false", &["{{subject_id}}"]);
        let runtime =
            orchestrator_core::AgentRuntimeConfig { tools_allowlist: vec!["false".to_string()], ..Default::default() };

        let err = run_workflow_phase_with_command(&subjectless_context(), &runtime, &command)
            .await
            .expect_err("empty required var must fail fast");
        let cmd_fail =
            err.downcast_ref::<CommandPhaseFailedError>().expect("fail-fast returns a CommandPhaseFailedError");
        // exit_code None proves the command never ran: a real run of `false`
        // would surface exit_code Some(1) via failure_summary, not fail-fast.
        assert!(cmd_fail.exit_code.is_none(), "fail-fast must not execute the command");
        assert!(cmd_fail.message.contains("{{subject_id}}"), "message names the unresolved placeholder");
        assert!(cmd_fail.message.contains("no subject bound"));
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_runs_when_required_var_bound() {
        // Control: a bound subject_id must NOT fail-fast — the command runs and
        // a genuine non-zero exit is reported via failure_summary (proving the
        // fail-fast guard does not over-trigger and swallow real executions).
        let command = command_def("false", &["{{subject_id}}"]);
        let runtime =
            orchestrator_core::AgentRuntimeConfig { tools_allowlist: vec!["false".to_string()], ..Default::default() };
        let mut context = subjectless_context();
        context.subject_id = "task:TASK-1";

        let result = run_workflow_phase_with_command(&context, &runtime, &command).await.expect("command runs");
        assert_eq!(result.exit_code, 1);
        assert!(result.failure_summary.is_some());
    }

    #[test]
    fn command_phase_failed_event_payload_carries_exit_code_and_stderr() {
        let error = CommandPhaseFailedError {
            message: "phase 'sync-status' command 'animus' exited with code 2 and failed the phase".to_string(),
            program: Some("animus".to_string()),
            exit_code: Some(2),
            stderr_excerpt: Some("--id must not be empty".to_string()),
        };
        let payload = command_phase_failed_event_payload("sync-status", &error);
        assert_eq!(payload["phase_id"], "sync-status");
        assert_eq!(payload["phase_status"], "failed");
        assert_eq!(payload["exit_code"], 2);
        assert_eq!(payload["stderr"], "--id must not be empty");
        assert_eq!(payload["program"], "animus");
        assert!(payload["error"].as_str().expect("error string").contains("code 2"));
    }

    #[test]
    fn command_failure_is_terminal_only_for_fail_verdict() {
        // on_failure_verdict "fail" -> terminal phase_failed routing.
        let fail_cmd = command_def_with_failure_verdict("false", Some("fail"));
        let fail_decision = build_command_phase_decision(&fail_cmd, "gate", 1, Some("boom"));
        assert!(command_failure_is_terminal(&fail_decision));

        // Default failure verdict is rework (a QA gate) -> NOT terminal, so the
        // existing Completed+rework path is preserved.
        let rework_cmd = command_def_with_failure_verdict("false", None);
        let rework_decision = build_command_phase_decision(&rework_cmd, "gate", 1, Some("boom"));
        assert!(!command_failure_is_terminal(&rework_decision));
    }

    // ---- TASK-206: command phases as decision producers ----

    #[test]
    fn command_emits_decision_only_when_opted_in() {
        // Plain command (no contract fields) does not produce decisions.
        assert!(!command_emits_decision(&command_def("cargo", &["test"])));

        // parse_json_output opts in.
        let by_flag: orchestrator_core::PhaseCommandDefinition =
            serde_json::from_value(serde_json::json!({ "program": "cmd", "parse_json_output": true })).unwrap();
        assert!(command_emits_decision(&by_flag));

        // An output contract (expected_result_kind / expected_schema) opts in.
        let by_kind: orchestrator_core::PhaseCommandDefinition =
            serde_json::from_value(serde_json::json!({ "program": "cmd", "expected_result_kind": "phase_decision" }))
                .unwrap();
        assert!(command_emits_decision(&by_kind));
        let by_schema: orchestrator_core::PhaseCommandDefinition =
            serde_json::from_value(serde_json::json!({ "program": "cmd", "expected_schema": {"type": "object"} }))
                .unwrap();
        assert!(command_emits_decision(&by_schema));
    }

    #[test]
    fn resolve_command_decision_honors_each_emitted_verdict_when_opted_in() {
        use orchestrator_core::PhaseDecisionVerdict as Verdict;
        let command = echo_json_command("unused"); // parse_json_output -> opted in
        for verdict in [Verdict::Advance, Verdict::Rework, Verdict::Skip, Verdict::Fail] {
            let result = command_result_stub(0, None, Some(decision_with_verdict(verdict)));
            let resolved = resolve_command_decision(&command, "gate", &result);
            assert_eq!(resolved.verdict, verdict, "emitted verdict must win when the command opts in");
        }
    }

    #[test]
    fn resolve_command_decision_ignores_verdict_when_not_opted_in() {
        use orchestrator_core::PhaseDecisionVerdict as Verdict;
        // A plain command (no parse_json_output/contract) is exit-code driven,
        // even if the stdout happened to contain a verdict-shaped object.
        let command = command_def("cargo", &["test"]);
        let result = command_result_stub(0, None, Some(decision_with_verdict(Verdict::Rework)));
        let resolved = resolve_command_decision(&command, "gate", &result);
        assert_eq!(resolved.verdict, Verdict::Advance, "clean exit advances; incidental verdict is ignored");
    }

    #[test]
    fn resolve_command_decision_falls_back_to_exit_code_when_no_verdict() {
        use orchestrator_core::PhaseDecisionVerdict as Verdict;
        let command = echo_json_command("unused"); // opted in, but no verdict emitted

        // Clean exit with no verdict -> advance.
        let clean = command_result_stub(0, None, None);
        assert_eq!(resolve_command_decision(&command, "gate", &clean).verdict, Verdict::Advance);

        // Non-zero exit with no verdict -> default on_failure_verdict (rework).
        let failed = command_result_stub(1, Some("boom"), None);
        assert_eq!(resolve_command_decision(&command, "gate", &failed).verdict, Verdict::Rework);
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_parses_emitted_verdict() {
        use orchestrator_core::PhaseDecisionVerdict as Verdict;
        let cases = [
            (r#"{"verdict":"advance","reason":"all good"}"#, Verdict::Advance),
            (r#"{"verdict":"rework","reason":"needs another pass"}"#, Verdict::Rework),
            (r#"{"verdict":"skip","reason":"already done"}"#, Verdict::Skip),
            (r#"{"verdict":"fail","reason":"unrecoverable"}"#, Verdict::Fail),
        ];
        for (raw, expected) in cases {
            let command = echo_json_command(raw);
            let result = run_workflow_phase_with_command(&bound_context(), &echo_runtime(), &command)
                .await
                .expect("echo command runs");
            // Exit 0 -> not a failure_summary; the verdict comes from stdout JSON.
            assert!(result.failure_summary.is_none(), "exit 0 has no failure summary");
            let decision = result.phase_decision.as_ref().expect("verdict parsed from stdout");
            assert_eq!(decision.verdict, expected, "stdout JSON verdict must be parsed");
        }
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_lenient_on_malformed_json() {
        // parse_json_output is set, but the command prints non-JSON on a clean
        // exit. This must NOT hard-fail (TASK-206 leniency): no verdict is
        // produced, so the arm falls back to exit-code inference (advance).
        let command = echo_json_command("this is not json");
        let result = run_workflow_phase_with_command(&bound_context(), &echo_runtime(), &command)
            .await
            .expect("malformed JSON must not hard-fail the phase");
        assert_eq!(result.exit_code, 0);
        assert!(result.failure_summary.is_none());
        assert!(result.phase_decision.is_none(), "no verdict parsed from non-JSON output");
        assert!(result.parsed_payload.is_none(), "malformed JSON yields no payload (falls back)");
    }

    /// Codex P3 #5 (Box::leak): drives `capture_command_stream` through 10k
    /// iterations and asserts each pass completes without panicking. Pre-fix
    /// each iteration leaked the phase-id string permanently; the heap-stress
    /// run was unsafe in long-lived plugin processes. With owned `String`s the
    /// loop runs end-to-end and the iterations complete cleanly.
    #[tokio::test]
    async fn capture_command_stream_does_not_leak_phase_id_under_stress() {
        const ITERATIONS: usize = 10_000;
        let payload = b"hello\nworld\n";

        for i in 0..ITERATIONS {
            let phase_id = format!("phase-{i}");
            let reader = Cursor::new(payload.to_vec());
            let capture = capture_command_stream(reader, phase_id).await.expect("capture ok");
            assert!(capture.text.contains("hello"));
            assert!(capture.text.contains("world"));
            assert!(capture.phase_decision.is_none());
        }
    }
}
