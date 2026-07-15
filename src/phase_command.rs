use anyhow::{anyhow, Result};
use orchestrator_plugin_host::PluginRegistry;
use serde_json::{json, Value};
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
    /// Subject kind for the bound subject (e.g. `animus.task`, `custom`, or a
    /// dynamic backend kind like `transcript`). Used to render `{{subject_id}}`
    /// in the kind-qualified `<kind>:<native>` form (see
    /// [`qualified_subject_id`]) so subject verbs like `animus subject status`
    /// resolve the right backend without a `default_subject_kind` fallback.
    pub subject_kind: &'a str,
    pub subject_title: &'a str,
    pub subject_description: &'a str,
    pub pipeline_vars: Option<&'a HashMap<String, String>>,
    pub dispatch_input: Option<&'a str>,
    pub schedule_input: Option<&'a str>,
    /// The bound subject's CUSTOM fields (e.g. a portal subject's `git_repo`),
    /// exposed as bare, lowercased `{{name}}` template variables so a command
    /// like `git clone {{git_repo}} .` renders (REQUIREMENT-048). Populated in
    /// [`run_workflow_phase_with_command`] from the raw `subject/get` record,
    /// since the in-tree `OrchestratorTask` value carries no arbitrary custom-
    /// field map. `None` when no subject record was resolved.
    pub custom_fields: Option<&'a HashMap<String, String>>,
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

/// Upper bound on the best-effort subject/get enrichment run before every
/// command phase. On timeout the phase degrades to scalar context rather than
/// stalling on a slow subject_backend.
const SUBJECT_FETCH_TIMEOUT_SECS: u64 = 5;

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

/// Map a subject kind to the bare kind alias the CLI subject backends dispatch
/// on. The built-in kinds are namespaced on the wire (`animus.task`,
/// `animus.requirement`) but the default `subject_backend` plugins advertise —
/// and route — the bare `task` / `requirement` methods, so the qualified id
/// prefix must use the bare alias. Returns `None` for the `custom` kind
/// (ad-hoc / schedule-driven subjects are never kind-qualified — their bare id,
/// e.g. `schedule:...`, already carries a prefix the schedule-input binding
/// depends on) and for an empty kind (no kind information available).
fn subject_kind_command_alias(kind: &str) -> Option<&str> {
    let kind = kind.trim();
    if kind.is_empty() || kind.eq_ignore_ascii_case(orchestrator_core::SUBJECT_KIND_CUSTOM) {
        return None;
    }
    if kind.eq_ignore_ascii_case(orchestrator_core::SUBJECT_KIND_TASK) {
        return Some("task");
    }
    if kind.eq_ignore_ascii_case(orchestrator_core::SUBJECT_KIND_REQUIREMENT) {
        return Some("requirement");
    }
    // Dynamic backend kinds (`transcript`, `blog`, ...) already dispatch on
    // their bare name, so use them verbatim.
    Some(kind)
}

/// Render a subject id in the kind-qualified `<kind>:<native>` form the CLI
/// resolves without a `default_subject_kind` fallback.
///
/// A workflow's `mark-running` command is `animus subject status --id
/// {{subject_id}} --status in-progress` with no `--kind`. A BARE native id
/// (`TRANSCRIPT-001`) forces the CLI onto the `default_subject_kind` (=`task`)
/// path, which the backend rejects for any non-task subject
/// (`id 'TRANSCRIPT-001' is a 'transcript' subject, not 'task'`). Emitting the
/// qualified id (`transcript:TRANSCRIPT-001`) lets the CLI derive the kind from
/// the prefix. Tasks stay backward compatible: `task:TASK-1` still resolves to
/// kind `task`, the same backend the bare-id + default-task path hit.
///
/// Pass-through cases (returned unchanged) so we never break an existing
/// consumer or double-prefix:
/// - an empty id (subjectless run — the fail-fast guard handles it), and
/// - an already-qualified id (contains `:`) — either a `<kind>:<native>` id the
///   caller supplied, or a `custom` schedule id (`schedule:...`) whose bare form
///   the schedule-input binding relies on.
pub(crate) fn qualified_subject_id(kind: &str, id: &str) -> String {
    let trimmed = id.trim();
    if trimmed.is_empty() || trimmed.contains(':') {
        return id.to_string();
    }
    match subject_kind_command_alias(kind) {
        Some(alias) => format!("{alias}:{trimmed}"),
        None => id.to_string(),
    }
}

pub(crate) fn build_command_template_vars(context: &CommandExecutionContext<'_>) -> HashMap<String, String> {
    let mut vars = HashMap::from([
        ("project_root".to_string(), context.project_root.to_string()),
        ("execution_cwd".to_string(), context.execution_cwd.to_string()),
        ("workflow_id".to_string(), context.workflow_id.to_string()),
        ("phase_id".to_string(), context.phase_id.to_string()),
        ("workflow_ref".to_string(), context.workflow_ref.to_string()),
        // `{{subject_id}}` renders kind-qualified so subject verbs resolve the
        // right backend; `{{subject_native_id}}` preserves the BARE native id
        // for any consumer that genuinely needs it.
        ("subject_id".to_string(), qualified_subject_id(context.subject_kind, context.subject_id)),
        ("subject_native_id".to_string(), context.subject_id.to_string()),
        ("subject_kind".to_string(), context.subject_kind.to_string()),
        ("subject_title".to_string(), context.subject_title.to_string()),
        ("subject_description".to_string(), context.subject_description.to_string()),
    ]);

    if let Some(pipeline_vars) = context.pipeline_vars {
        for (key, value) in pipeline_vars {
            vars.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    // The bound subject's custom fields (e.g. `git_repo`) as bare, lowercased
    // template vars. Merged last with `or_insert` so a built-in var or an
    // explicit pipeline var is never shadowed by a same-named custom field.
    if let Some(custom_fields) = context.custom_fields {
        for (key, value) in custom_fields {
            vars.entry(key.to_ascii_lowercase()).or_insert_with(|| value.clone());
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

/// RAII guard that deletes the per-phase context temp file on drop, so the
/// `ANIMUS_CONTEXT_FILE` never leaks across phases — it is removed on the
/// success, failure, and timeout return paths alike.
struct TempContextFile {
    path: PathBuf,
}

impl Drop for TempContextFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn sanitize_path_component(value: &str) -> String {
    value.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' }).collect()
}

/// Absolute temp path for this phase run's context file, scoped by run id +
/// phase id + a random suffix so concurrent phases never collide.
fn command_context_file_path(context: &CommandExecutionContext<'_>) -> PathBuf {
    let name = format!(
        "animus-ctx-{}-{}-{}.json",
        sanitize_path_component(context.workflow_id),
        sanitize_path_component(context.phase_id),
        uuid::Uuid::new_v4().simple()
    );
    std::env::temp_dir().join(name)
}

/// Write the context JSON with owner-only (0600) permissions — it carries the
/// full `subject.data`, which may contain sensitive fields. On unix the file is
/// created 0600 up front so it is never briefly world-readable.
#[cfg(unix)]
fn write_context_file_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
    file.write_all(bytes)
}

#[cfg(not(unix))]
fn write_context_file_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

/// The `ANIMUS_*` scalar env catalog promoted onto the command subprocess. A
/// superset of the `{{var}}` template catalog (which keeps working — this is
/// additive) plus the subject status from the fetched record.
pub(crate) fn build_animus_context_env(
    context: &CommandExecutionContext<'_>,
    template_vars: &HashMap<String, String>,
    subject_status: &str,
) -> Vec<(&'static str, String)> {
    let qualified_subject_id =
        template_vars.get("subject_id").cloned().unwrap_or_else(|| context.subject_id.to_string());
    let dispatch_input = template_vars.get("dispatch_input").cloned().unwrap_or_default();
    vec![
        ("ANIMUS_SUBJECT_ID", qualified_subject_id),
        ("ANIMUS_SUBJECT_NATIVE_ID", context.subject_id.to_string()),
        ("ANIMUS_SUBJECT_KIND", context.subject_kind.to_string()),
        ("ANIMUS_SUBJECT_TITLE", context.subject_title.to_string()),
        ("ANIMUS_SUBJECT_STATUS", subject_status.to_string()),
        ("ANIMUS_WORKFLOW_REF", context.workflow_ref.to_string()),
        ("ANIMUS_WORKFLOW_ID", context.workflow_id.to_string()),
        ("ANIMUS_PHASE_ID", context.phase_id.to_string()),
        ("ANIMUS_PROJECT_ROOT", context.project_root.to_string()),
        ("ANIMUS_EXECUTION_CWD", context.execution_cwd.to_string()),
        ("ANIMUS_DISPATCH_INPUT", dispatch_input),
    ]
}

/// Build the structured context JSON written to `ANIMUS_CONTEXT_FILE`. Subject
/// identity/title/description come from the (always-present) execution context;
/// `status`/`priority`/`data`/`labels`/`dependencies` are enriched from the
/// best-effort `subject/get` record (null when the fetch degraded). `phases`
/// carries prior completed phases this run (id + verdict + persisted outputs).
pub(crate) fn build_command_context_json(
    context: &CommandExecutionContext<'_>,
    template_vars: &HashMap<String, String>,
    subject_record: Option<&Value>,
    prior_phases: &[crate::phase_output::PersistedPhaseOutput],
) -> Value {
    let field = |key: &str| subject_record.and_then(|rec| rec.get(key)).cloned();
    // `data` prefers an explicit `data` object, then the backend `attributes`
    // bag; `labels` prefers `labels`, then `tags`. Absent fields stay `null`.
    let data = field("data").or_else(|| field("attributes")).unwrap_or(Value::Null);
    let labels = field("labels").or_else(|| field("tags")).unwrap_or(Value::Null);

    let qualified_subject_id =
        template_vars.get("subject_id").cloned().unwrap_or_else(|| context.subject_id.to_string());

    let subject = json!({
        "id": qualified_subject_id,
        "native_id": context.subject_id,
        "kind": context.subject_kind,
        "title": context.subject_title,
        "status": field("status").unwrap_or(Value::Null),
        "priority": field("priority").unwrap_or(Value::Null),
        "description": context.subject_description,
        "data": data,
        "labels": labels,
        "dependencies": field("dependencies").unwrap_or(Value::Null),
    });

    let phases: Vec<Value> = prior_phases
        .iter()
        .map(|output| {
            json!({
                "id": output.phase_id,
                "verdict": output.verdict,
                "outputs": output.payload.clone().unwrap_or(Value::Null),
            })
        })
        .collect();

    // dispatch_input is surfaced as parsed JSON when it is valid JSON, else as
    // the raw string; absent -> null.
    let dispatch_input = template_vars
        .get("dispatch_input")
        .map(|raw| serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.clone())))
        .unwrap_or(Value::Null);

    json!({
        "subject": subject,
        "workflow": {
            "ref": context.workflow_ref,
            "run_id": context.workflow_id,
            "phase_id": context.phase_id,
        },
        "phases": phases,
        "dispatch_input": dispatch_input,
    })
}

/// Best-effort fetch of the FULL subject record via a `subject_backend`
/// plugin's generic `subject/get` (`{kind, id}`), falling back to the per-kind
/// `<kind>/get` (`{id}`) — the same discovery the runner uses for dynamic-kind
/// context builds. Returns `None` (never errors) on any failure so a command
/// phase degrades gracefully to the scalar env + minimal context file.
async fn fetch_subject_record(project_root: &str, kind: &str, native_id: &str) -> Option<Value> {
    let kind = kind.trim();
    let native_id = native_id.trim();
    if kind.is_empty() || native_id.is_empty() {
        return None;
    }

    let mut registry = PluginRegistry::discover(project_root).ok()?;
    let kind_method = format!("{kind}/get");
    let candidates: Vec<(String, String, Value)> = registry
        .list_plugins()
        .filter(|plugin| crate::workflow_execute::is_subject_backend(plugin))
        .filter_map(|plugin| {
            let capabilities = &plugin.manifest.capabilities;
            if capabilities.iter().any(|cap| cap.as_str() == "subject/get") {
                Some((plugin.name.clone(), "subject/get".to_string(), json!({ "kind": kind, "id": native_id })))
            } else if capabilities.iter().any(|cap| cap.as_str() == kind_method.as_str()) {
                Some((plugin.name.clone(), kind_method.clone(), json!({ "id": native_id })))
            } else {
                None
            }
        })
        .collect();

    for (name, method, params) in &candidates {
        if let Ok(host) = registry.get_plugin(name).await {
            if let Ok(value) = host.request(method.clone(), Some(params.clone())).await {
                return Some(value);
            }
        }
    }
    None
}

/// Extract the bound subject's CUSTOM fields from a raw `subject/get` record as
/// string values, preferring the explicit `data` object then the backend
/// `attributes` bag (the same precedence [`build_command_context_json`] uses).
/// String values pass through and other scalars stringify; `null` and nested
/// object/array values are skipped (they are not sensible template scalars).
/// These become bare, lowercased `{{name}}` template vars so a command like
/// `git clone {{git_repo}} .` renders (REQUIREMENT-048).
fn subject_custom_fields(record: Option<&Value>) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    // Custom fields land under `data`/`attributes` on some backends, but the
    // consolidated animus-postgres subject/get surfaces them under `custom`
    // (verified live) — read all three so a `git_repo` set via
    // `animus subject update --data` resolves as `{{git_repo}}`.
    let Some(bag) = record.and_then(|record| {
        record
            .get("data")
            .and_then(Value::as_object)
            .or_else(|| record.get("attributes").and_then(Value::as_object))
            .or_else(|| record.get("custom").and_then(Value::as_object))
    }) else {
        return fields;
    };
    for (key, value) in bag {
        let rendered = match value {
            Value::String(text) => Some(text.clone()),
            Value::Bool(_) | Value::Number(_) => Some(value.to_string()),
            Value::Null | Value::Array(_) | Value::Object(_) => None,
        };
        if let Some(rendered) = rendered {
            fields.insert(key.clone(), rendered);
        }
    }
    fields
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

/// Map a configured `on_success_verdict` / `on_failure_verdict` string to a
/// `(verdict, verdict_key)` pair. A built-in verdict maps to its enum variant
/// with no key; a non-empty non-built-in string is carried as `Unknown` + the
/// raw key so the executor routes it through the phase `on_verdict` map (custom
/// verdict routing). An absent/empty configuration falls back to `default`.
fn map_configured_command_verdict(
    configured: Option<&str>,
    default: orchestrator_core::PhaseDecisionVerdict,
) -> (orchestrator_core::PhaseDecisionVerdict, Option<String>) {
    match configured.map(str::trim).filter(|value| !value.is_empty()) {
        None => (default, None),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "advance" => (orchestrator_core::PhaseDecisionVerdict::Advance, None),
            "rework" => (orchestrator_core::PhaseDecisionVerdict::Rework, None),
            "skip" => (orchestrator_core::PhaseDecisionVerdict::Skip, None),
            "fail" => (orchestrator_core::PhaseDecisionVerdict::Fail, None),
            _ => (orchestrator_core::PhaseDecisionVerdict::Unknown, Some(value.to_string())),
        },
    }
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

    let (verdict, verdict_key) = if success {
        map_configured_command_verdict(
            command.on_success_verdict.as_deref(),
            orchestrator_core::PhaseDecisionVerdict::Advance,
        )
    } else {
        map_configured_command_verdict(
            command.on_failure_verdict.as_deref(),
            orchestrator_core::PhaseDecisionVerdict::Rework,
        )
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
        // A non-built-in `on_success_verdict`/`on_failure_verdict` routes via
        // `Unknown` + the raw key; built-in mappings leave it `None`.
        verdict_key,
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
    // REQUIREMENT-048: the per-run environment node, when the run is env-routed.
    // `Some` -> exec the command INSIDE the shared node (so a `git clone` /
    // `git commit` operates on the SAME workspace the agent phases edit); `None`
    // -> the default local `TokioCommand` path (byte-for-byte unchanged).
    held_environment: Option<&dyn crate::phase_environment::HeldEnvironment>,
) -> Result<CommandExecutionResult> {
    if !is_program_allowlisted(&command.program, &runtime_config.tools_allowlist) {
        return Err(anyhow!("phase '{}' command '{}' is not in tools_allowlist", context.phase_id, command.program));
    }

    // TASK-292 + REQUIREMENT-048: best-effort fetch of the FULL subject record
    // via the subject_backend's generic `subject/get` BEFORE templating, so the
    // subject's custom fields (e.g. `git_repo`) can be exposed as `{{name}}`
    // template vars AND the context blob carries status/priority/data/labels the
    // runner does not otherwise track. A fetch failure must NOT crash the phase —
    // degrade to the scalar env + minimal context file. Bounded because it
    // spawns/connects a subject_backend plugin host on EVERY command phase (incl.
    // trivial mark-running/mark-done); a slow/hung backend must not stall it.
    let subject_record = match tokio::time::timeout(
        std::time::Duration::from_secs(SUBJECT_FETCH_TIMEOUT_SECS),
        fetch_subject_record(context.project_root, context.subject_kind, context.subject_id),
    )
    .await
    {
        Ok(record) => record,
        Err(_) => {
            tracing::warn!(
                phase_id = context.phase_id,
                subject_id = context.subject_id,
                timeout_secs = SUBJECT_FETCH_TIMEOUT_SECS,
                "command-phase context: subject/get timed out; degrading to scalar context"
            );
            None
        }
    };
    if subject_record.is_none() && !context.subject_id.trim().is_empty() {
        tracing::debug!(
            phase_id = context.phase_id,
            subject_id = context.subject_id,
            subject_kind = context.subject_kind,
            "command-phase context: subject/get returned no record; degrading to scalar context"
        );
    }

    // Expose the subject's custom fields (from the raw record's `data` /
    // `attributes` bag) as bare `{{name}}` template vars — the in-tree
    // `OrchestratorTask` value carries no arbitrary custom-field map, so the raw
    // subject fetch above is the source.
    let custom_fields = subject_custom_fields(subject_record.as_ref());
    let context = &CommandExecutionContext { custom_fields: Some(&custom_fields), ..*context };

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

    // REQUIREMENT-048: exec the templated command INSIDE the per-run
    // environment node when the run is env-routed, so a `git clone` / `git
    // commit` / `gh pr create` operates on the SAME shared workspace the agent
    // phases edit. The rendered program/args/env/cwd are identical to the local
    // path; only WHERE the process runs changes. The shared tail below
    // (success_exit_codes check, verdict/JSON parsing, result assembly) operates
    // on the stdout/stderr strings + exit_code, so it is execution-path-agnostic.
    let (exit_code, stdout, stderr, duration_ms, phase_decision) = match held_environment {
        Some(held) => {
            let started = std::time::Instant::now();
            let output = held.exec_command(
                Path::new(context.project_root),
                &command.program,
                &args,
                &env,
                Some(cwd.as_str()),
                None, // command phases run with stdin closed (mirrors Stdio::null())
                command.timeout_secs.map(Duration::from_secs),
            )?;
            let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            if output.timed_out {
                return Err(anyhow!(
                    "phase '{}' command '{}' timed out{}",
                    context.phase_id,
                    command.program,
                    command.timeout_secs.map(|secs| format!(" after {secs} seconds")).unwrap_or_default()
                ));
            }
            let phase_decision = parse_phase_decision_from_text(&output.stdout, context.phase_id)
                .or_else(|| parse_phase_decision_from_text(&output.stderr, context.phase_id));
            (output.exit_code, output.stdout, output.stderr, duration_ms, phase_decision)
        }
        None => {
            let prior_phases = crate::phase_output::list_prior_phase_outputs(
                context.project_root,
                context.workflow_id,
                context.phase_id,
            );
            let context_json =
                build_command_context_json(context, &template_vars, subject_record.as_ref(), &prior_phases);
            let subject_status =
                subject_record.as_ref().and_then(|rec| rec.get("status")).and_then(Value::as_str).unwrap_or_default();
            let animus_env = build_animus_context_env(context, &template_vars, subject_status);

            // Write the structured context JSON to a temp file scoped to THIS phase
            // run. The RAII guard removes it on every return path so it never leaks
            // across phases; stdin stays null.
            let context_file = command_context_file_path(context);
            let context_bytes = serde_json::to_vec(&context_json).unwrap_or_default();
            // Own the path with the RAII guard BEFORE writing so a partial file left by a
            // mid-write error is still cleaned up (the guard drops on the Err arm). The
            // file is written 0600 — it embeds subject.data which may carry secrets,
            // unlike the scalar ANIMUS_* env.
            let context_file_guard = {
                let guard = TempContextFile { path: context_file.clone() };
                match write_context_file_private(&context_file, &context_bytes) {
                    Ok(()) => Some(guard),
                    Err(error) => {
                        tracing::warn!(
                            phase_id = context.phase_id,
                            path = %context_file.display(),
                            %error,
                            "command-phase context: failed to write ANIMUS_CONTEXT_FILE; continuing with scalar env only"
                        );
                        None
                    }
                }
            };

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

            // TASK-292: promote the context scalars to real env vars (additive — the
            // `{{var}}` templating above is unchanged). Set AFTER the user's
            // command.env so the runner-provided ANIMUS_* contract is authoritative.
            for (key, value) in &animus_env {
                process.env(key, value);
            }
            if let Some(guard) = context_file_guard.as_ref() {
                process.env("ANIMUS_CONTEXT_FILE", &guard.path);
            }

            let mut child = process.spawn()?;
            let stdout_reader =
                child.stdout.take().ok_or_else(|| anyhow!("failed to capture stdout for command phase"))?;
            let stderr_reader =
                child.stderr.take().ok_or_else(|| anyhow!("failed to capture stderr for command phase"))?;
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

            let stdout_capture =
                stdout_task.await.map_err(|error| anyhow!("stdout capture task failed: {error}"))??;
            let stderr_capture =
                stderr_task.await.map_err(|error| anyhow!("stderr capture task failed: {error}"))??;

            let exit_code = status.code().unwrap_or(-1);
            let stdout = stdout_capture.text;
            let stderr = stderr_capture.text;
            let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            let phase_decision = stdout_capture
                .phase_decision
                .or(stderr_capture.phase_decision)
                .or_else(|| parse_phase_decision_from_text(&stdout, context.phase_id))
                .or_else(|| parse_phase_decision_from_text(&stderr, context.phase_id));
            (exit_code, stdout, stderr, duration_ms, phase_decision)
        }
    };

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
            subject_kind: "",
            subject_title: "",
            subject_description: "",
            pipeline_vars: None,
            dispatch_input: None,
            schedule_input: None,
            custom_fields: None,
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

    #[test]
    fn qualified_subject_id_qualifies_dynamic_and_builtin_kinds() {
        // Dynamic (custom-declared) kind: `mark-running` must render the
        // kind-qualified id so the CLI derives kind `transcript` from the
        // prefix instead of falling back to `default_subject_kind` (=task).
        assert_eq!(qualified_subject_id("transcript", "TRANSCRIPT-001"), "transcript:TRANSCRIPT-001");
        assert_eq!(qualified_subject_id("blog", "BLOG-42"), "blog:BLOG-42");

        // Built-in task: the wire kind is namespaced (`animus.task`) but the
        // CLI backend dispatches on the bare `task` alias — and `task:TASK-1`
        // still resolves to kind `task`, so this stays backward compatible with
        // the old bare-id + default-task path.
        assert_eq!(qualified_subject_id(orchestrator_core::SUBJECT_KIND_TASK, "TASK-1"), "task:TASK-1");
        assert_eq!(qualified_subject_id("task", "TASK-1"), "task:TASK-1");
        assert_eq!(qualified_subject_id(orchestrator_core::SUBJECT_KIND_REQUIREMENT, "REQ-9"), "requirement:REQ-9");
        assert_eq!(qualified_subject_id("requirement", "REQ-9"), "requirement:REQ-9");
    }

    #[test]
    fn qualified_subject_id_passes_through_special_cases() {
        // Empty (subjectless) id is untouched — the fail-fast guard handles it.
        assert_eq!(qualified_subject_id("transcript", ""), "");
        // Already-qualified ids are never double-prefixed.
        assert_eq!(qualified_subject_id("transcript", "transcript:TRANSCRIPT-001"), "transcript:TRANSCRIPT-001");
        assert_eq!(qualified_subject_id(orchestrator_core::SUBJECT_KIND_TASK, "task:TASK-1"), "task:TASK-1");
        // Custom / schedule-driven subjects keep their bare id (their prefix is
        // meaningful to the schedule-input binding, not a kind qualifier).
        assert_eq!(
            qualified_subject_id(orchestrator_core::SUBJECT_KIND_CUSTOM, "schedule:nightly"),
            "schedule:nightly"
        );
        // No kind information -> leave the id bare (non-regression).
        assert_eq!(qualified_subject_id("", "TASK-1"), "TASK-1");
    }

    #[test]
    fn build_command_template_vars_exposes_qualified_and_native_subject_id() {
        // A dynamic-kind command context: `{{subject_id}}` is qualified,
        // `{{subject_native_id}}` keeps the bare native id, and `{{subject_kind}}`
        // is exposed.
        let mut ctx = subjectless_context();
        ctx.subject_id = "TRANSCRIPT-001";
        ctx.subject_kind = "transcript";
        let vars = build_command_template_vars(&ctx);
        assert_eq!(vars.get("subject_id").map(String::as_str), Some("transcript:TRANSCRIPT-001"));
        assert_eq!(vars.get("subject_native_id").map(String::as_str), Some("TRANSCRIPT-001"));
        assert_eq!(vars.get("subject_kind").map(String::as_str), Some("transcript"));

        // A built-in task context: qualified as `task:...`, native id preserved.
        let mut task_ctx = subjectless_context();
        task_ctx.subject_id = "TASK-254";
        task_ctx.subject_kind = orchestrator_core::SUBJECT_KIND_TASK;
        let task_vars = build_command_template_vars(&task_ctx);
        assert_eq!(task_vars.get("subject_id").map(String::as_str), Some("task:TASK-254"));
        assert_eq!(task_vars.get("subject_native_id").map(String::as_str), Some("TASK-254"));
    }

    #[test]
    fn build_command_template_vars_exposes_subject_custom_fields() {
        // REQUIREMENT-048: a subject custom field like `git_repo` renders as a
        // bare `{{git_repo}}` template var so `git clone {{git_repo}} .` works.
        let custom = HashMap::from([
            ("git_repo".to_string(), "https://github.com/acme/app.git".to_string()),
            ("GitRef".to_string(), "main".to_string()),
        ]);
        let mut ctx = bound_context();
        ctx.custom_fields = Some(&custom);
        let vars = build_command_template_vars(&ctx);
        assert_eq!(vars.get("git_repo").map(String::as_str), Some("https://github.com/acme/app.git"));
        // Keys are lowercased at merge time.
        assert_eq!(vars.get("gitref").map(String::as_str), Some("main"));

        // A custom field NEVER shadows a built-in var (e.g. subject_id).
        let shadow = HashMap::from([("subject_id".to_string(), "HACKED".to_string())]);
        ctx.custom_fields = Some(&shadow);
        let vars = build_command_template_vars(&ctx);
        assert_eq!(vars.get("subject_id").map(String::as_str), Some("task:TASK-1"), "built-in subject_id wins");
    }

    #[test]
    fn subject_custom_fields_extracts_from_data_then_attributes() {
        // `data` object is preferred; string values pass through, other scalars
        // stringify, and null / nested containers are skipped.
        let record = serde_json::json!({
            "data": {
                "git_repo": "https://github.com/acme/app.git",
                "count": 3,
                "flag": true,
                "nothing": null,
                "nested": { "a": 1 },
                "list": [1, 2]
            }
        });
        let fields = subject_custom_fields(Some(&record));
        assert_eq!(fields.get("git_repo").map(String::as_str), Some("https://github.com/acme/app.git"));
        assert_eq!(fields.get("count").map(String::as_str), Some("3"));
        assert_eq!(fields.get("flag").map(String::as_str), Some("true"));
        assert!(!fields.contains_key("nothing"), "null skipped");
        assert!(!fields.contains_key("nested"), "nested object skipped");
        assert!(!fields.contains_key("list"), "array skipped");

        // Falls back to the backend `attributes` bag when there is no `data`.
        let attrs = serde_json::json!({ "attributes": { "git_repo": "git@x:y.git" } });
        assert_eq!(subject_custom_fields(Some(&attrs)).get("git_repo").map(String::as_str), Some("git@x:y.git"));

        // Falls back to `custom` — the shape animus-postgres subject/get returns.
        let custom = serde_json::json!({ "custom": { "git_repo": "https://github.com/launchapp-dev/animus-cli.git" } });
        assert_eq!(
            subject_custom_fields(Some(&custom)).get("git_repo").map(String::as_str),
            Some("https://github.com/launchapp-dev/animus-cli.git")
        );

        // No record / no bag -> empty.
        assert!(subject_custom_fields(None).is_empty());
        assert!(subject_custom_fields(Some(&serde_json::json!({ "status": "open" }))).is_empty());
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_fails_fast_on_empty_required_var() {
        let command = command_def("false", &["{{subject_id}}"]);
        let runtime =
            orchestrator_core::AgentRuntimeConfig { tools_allowlist: vec!["false".to_string()], ..Default::default() };

        let err = run_workflow_phase_with_command(&subjectless_context(), &runtime, &command, None)
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

        let result = run_workflow_phase_with_command(&context, &runtime, &command, None).await.expect("command runs");
        assert_eq!(result.exit_code, 1);
        assert!(result.failure_summary.is_some());
    }

    /// A fake [`crate::phase_environment::HeldEnvironment`] that records the raw
    /// command it is handed and returns a canned buffered output, WITHOUT
    /// running anything on the host. Used to assert the command phase routes
    /// through the node instead of spawning a local process.
    /// One recorded `exec_command` call: `(program, args, cwd)`.
    type RecordedEnvCall = (String, Vec<String>, Option<String>);

    struct FakeHeldEnvironment {
        calls: std::sync::Mutex<Vec<RecordedEnvCall>>,
        stdout: String,
        exit_code: i32,
    }

    impl crate::phase_environment::HeldEnvironment for FakeHeldEnvironment {
        fn id(&self) -> &str {
            "fake"
        }

        fn exec_session(
            &self,
            _project_root: &Path,
            _request: &animus_session_backend::session::SessionRequest,
        ) -> Result<animus_session_backend::session::SessionRun> {
            unreachable!("command phases route through exec_command, never exec_session")
        }

        fn exec_command(
            &self,
            _project_root: &Path,
            program: &str,
            args: &[String],
            _env: &BTreeMap<String, String>,
            cwd: Option<&str>,
            _stdin: Option<String>,
            _timeout: Option<Duration>,
        ) -> Result<crate::phase_environment::EnvCommandOutput> {
            self.calls.lock().unwrap().push((program.to_string(), args.to_vec(), cwd.map(str::to_string)));
            Ok(crate::phase_environment::EnvCommandOutput {
                exit_code: self.exit_code,
                stdout: self.stdout.clone(),
                stderr: String::new(),
                timed_out: false,
            })
        }
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_routes_through_held_environment() {
        // The command is sent to the held environment (NOT spawned locally); its
        // stdout + exit code flow back into the CommandExecutionResult, and the
        // shared verdict-parsing tail still lifts an emitted verdict from stdout.
        let fake = FakeHeldEnvironment {
            calls: std::sync::Mutex::new(Vec::new()),
            stdout: r#"{"verdict":"advance","reason":"clone ok"}"#.to_string(),
            exit_code: 0,
        };
        // `echo` is allowlisted + parse_json_output opts the phase into verdicts.
        let command = echo_json_command("unused-when-env-routed");

        let result = run_workflow_phase_with_command(&bound_context(), &echo_runtime(), &command, Some(&fake))
            .await
            .expect("env-routed command runs");

        // The command was dispatched to the environment exactly once.
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "command must be sent to the held environment");
        assert_eq!(calls[0].0, "echo", "program forwarded to the environment");

        // stdout + exit code flow back from the environment.
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("advance"), "env stdout surfaced: {}", result.stdout);
        assert!(result.failure_summary.is_none(), "exit 0 is not a failure");

        // Verdict parsing (shared execution-path-agnostic tail) still works.
        let decision = result.phase_decision.as_ref().expect("verdict parsed from env stdout");
        assert_eq!(decision.verdict, orchestrator_core::PhaseDecisionVerdict::Advance);
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_env_routed_nonzero_exit_reports_failure() {
        // A non-zero exit from the environment flows through the SAME failure
        // path as the local spawn (failure_summary set, exit code preserved).
        let fake =
            FakeHeldEnvironment { calls: std::sync::Mutex::new(Vec::new()), stdout: String::new(), exit_code: 2 };
        let command = command_def("echo", &["hi"]);

        let result = run_workflow_phase_with_command(&bound_context(), &echo_runtime(), &command, Some(&fake))
            .await
            .expect("env-routed command runs");
        assert_eq!(result.exit_code, 2);
        assert!(result.failure_summary.is_some(), "non-zero env exit sets failure_summary");
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
            let result = run_workflow_phase_with_command(&bound_context(), &echo_runtime(), &command, None)
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
        let result = run_workflow_phase_with_command(&bound_context(), &echo_runtime(), &command, None)
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

    // ---- TASK-293: custom verdict keys on command decisions ----

    #[test]
    fn map_configured_command_verdict_maps_builtin_and_custom() {
        use orchestrator_core::PhaseDecisionVerdict as V;
        assert_eq!(map_configured_command_verdict(None, V::Advance), (V::Advance, None));
        assert_eq!(map_configured_command_verdict(Some("rework"), V::Advance), (V::Rework, None));
        assert_eq!(map_configured_command_verdict(Some("SKIP"), V::Rework), (V::Skip, None));
        assert_eq!(map_configured_command_verdict(Some("  fail "), V::Advance), (V::Fail, None));
        // A non-built-in verdict carries the raw key verbatim as Unknown.
        let (verdict, key) = map_configured_command_verdict(Some("needs-research"), V::Advance);
        assert_eq!(verdict, V::Unknown);
        assert_eq!(key.as_deref(), Some("needs-research"));
    }

    #[test]
    fn build_command_phase_decision_custom_failure_verdict_carries_key() {
        let cmd = command_def_with_failure_verdict("false", Some("needs-research"));
        let decision = build_command_phase_decision(&cmd, "gate", 1, Some("boom"));
        assert_eq!(decision.verdict, orchestrator_core::PhaseDecisionVerdict::Unknown);
        assert_eq!(decision.verdict_key.as_deref(), Some("needs-research"));
        // A custom verdict is not a terminal fail, so the QA-gate Completed path
        // (not the phase_failed path) carries it — routing happens downstream.
        assert!(!command_failure_is_terminal(&decision));
    }

    // ---- TASK-292: rich context injection ----

    #[test]
    fn build_command_context_json_enriches_from_record_and_prior_phases() {
        let mut ctx = subjectless_context();
        ctx.subject_id = "TRANSCRIPT-001";
        ctx.subject_kind = "transcript";
        ctx.subject_title = "Standup";
        ctx.subject_description = "notes";
        let vars = build_command_template_vars(&ctx);

        let record = serde_json::json!({
            "id": "transcript:TRANSCRIPT-001",
            "kind": "transcript",
            "status": "captured",
            "priority": "p1",
            "data": { "source": "krisp" },
            "labels": ["standup"],
            "dependencies": ["TASK-1"],
        });
        let prior = vec![crate::phase_output::PersistedPhaseOutput {
            phase_id: "research".to_string(),
            completed_at: "2026-03-01T00:00:00Z".to_string(),
            verdict: Some("advance".to_string()),
            confidence: Some(0.9),
            reason: Some("done".to_string()),
            commit_message: None,
            evidence: vec![],
            guardrail_violations: vec![],
            payload: Some(serde_json::json!({ "findings": ["a"] })),
        }];

        let json = build_command_context_json(&ctx, &vars, Some(&record), &prior);
        assert_eq!(json["subject"]["id"], "transcript:TRANSCRIPT-001");
        assert_eq!(json["subject"]["native_id"], "TRANSCRIPT-001");
        assert_eq!(json["subject"]["kind"], "transcript");
        assert_eq!(json["subject"]["status"], "captured");
        assert_eq!(json["subject"]["priority"], "p1");
        assert_eq!(json["subject"]["data"]["source"], "krisp");
        assert_eq!(json["subject"]["labels"][0], "standup");
        assert_eq!(json["subject"]["dependencies"][0], "TASK-1");
        assert_eq!(json["workflow"]["run_id"], ctx.workflow_id);
        assert_eq!(json["workflow"]["phase_id"], ctx.phase_id);
        assert_eq!(json["phases"][0]["id"], "research");
        assert_eq!(json["phases"][0]["verdict"], "advance");
        assert_eq!(json["phases"][0]["outputs"]["findings"][0], "a");
    }

    #[test]
    fn build_command_context_json_degrades_without_record() {
        let ctx = bound_context();
        let vars = build_command_template_vars(&ctx);
        let json = build_command_context_json(&ctx, &vars, None, &[]);
        assert_eq!(json["subject"]["native_id"], "task:TASK-1");
        // Keys stay present but null when the subject fetch degraded.
        assert!(json["subject"]["data"].is_null());
        assert!(json["subject"]["status"].is_null());
        assert!(json["subject"]["dependencies"].is_null());
        assert!(json["phases"].as_array().expect("phases is array").is_empty());
    }

    #[test]
    fn build_animus_context_env_exposes_full_catalog() {
        let mut ctx = subjectless_context();
        ctx.subject_id = "TASK-9";
        ctx.subject_kind = orchestrator_core::SUBJECT_KIND_TASK;
        ctx.subject_title = "Do it";
        let vars = build_command_template_vars(&ctx);
        let env = build_animus_context_env(&ctx, &vars, "in-progress");
        let map: HashMap<_, _> = env.into_iter().collect();
        assert_eq!(map.get("ANIMUS_SUBJECT_ID").map(String::as_str), Some("task:TASK-9"));
        assert_eq!(map.get("ANIMUS_SUBJECT_NATIVE_ID").map(String::as_str), Some("TASK-9"));
        assert_eq!(map.get("ANIMUS_SUBJECT_KIND").map(String::as_str), Some(orchestrator_core::SUBJECT_KIND_TASK));
        assert_eq!(map.get("ANIMUS_SUBJECT_TITLE").map(String::as_str), Some("Do it"));
        assert_eq!(map.get("ANIMUS_SUBJECT_STATUS").map(String::as_str), Some("in-progress"));
        assert_eq!(map.get("ANIMUS_PHASE_ID").map(String::as_str), Some(ctx.phase_id));
        for key in [
            "ANIMUS_WORKFLOW_REF",
            "ANIMUS_WORKFLOW_ID",
            "ANIMUS_PROJECT_ROOT",
            "ANIMUS_EXECUTION_CWD",
            "ANIMUS_DISPATCH_INPUT",
        ] {
            assert!(map.contains_key(key), "missing {key}");
        }
    }

    #[tokio::test]
    async fn run_workflow_phase_with_command_injects_animus_env_and_context_file() {
        // The command subprocess must receive the ANIMUS_* env catalog and a
        // readable ANIMUS_CONTEXT_FILE whose JSON carries the subject context;
        // the temp file must be cleaned up after the phase returns.
        let script = r#"printf 'SID=%s\nKIND=%s\nCTX=%s\n' "$ANIMUS_SUBJECT_ID" "$ANIMUS_SUBJECT_KIND" "$ANIMUS_CONTEXT_FILE"; cat "$ANIMUS_CONTEXT_FILE""#;
        let command: orchestrator_core::PhaseCommandDefinition =
            serde_json::from_value(serde_json::json!({ "program": "sh", "args": ["-c", script] }))
                .expect("command def");
        let runtime =
            orchestrator_core::AgentRuntimeConfig { tools_allowlist: vec!["sh".to_string()], ..Default::default() };
        let mut context = subjectless_context();
        context.subject_id = "TRANSCRIPT-001";
        context.subject_kind = "transcript";
        context.subject_title = "Standup";

        let result = run_workflow_phase_with_command(&context, &runtime, &command, None).await.expect("command runs");
        assert_eq!(result.exit_code, 0);
        let stdout = result.stdout;

        // ANIMUS_* env promoted onto the child (kind-qualified id + kind).
        assert!(stdout.contains("SID=transcript:TRANSCRIPT-001"), "qualified subject id in env: {stdout}");
        assert!(stdout.contains("KIND=transcript"), "subject kind in env: {stdout}");

        let ctx_path =
            stdout.lines().find_map(|line| line.strip_prefix("CTX=")).expect("CTX= line present").to_string();
        assert!(!ctx_path.is_empty(), "ANIMUS_CONTEXT_FILE must be set");

        // The JSON blob (everything from the first `{`) parses and carries the
        // subject identity + a stable key set (subject.data present, null with
        // no backend available in the test env).
        let json_start = stdout.find('{').expect("context JSON present in stdout");
        let ctx: serde_json::Value =
            serde_json::from_str(stdout[json_start..].trim()).expect("context file is valid JSON");
        assert_eq!(ctx["subject"]["native_id"], "TRANSCRIPT-001");
        assert_eq!(ctx["subject"]["kind"], "transcript");
        assert_eq!(ctx["subject"]["id"], "transcript:TRANSCRIPT-001");
        assert!(ctx["subject"].get("data").is_some(), "subject.data key always present");
        assert!(ctx["workflow"]["run_id"].is_string());
        assert!(ctx["phases"].is_array());

        // Cleanup: the temp context file must not leak past the phase run.
        assert!(!std::path::Path::new(&ctx_path).exists(), "ANIMUS_CONTEXT_FILE must be cleaned up after the phase");
    }
}
