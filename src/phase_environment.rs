//! REQUIREMENT-048 Phase B: route a workflow PHASE's harness execution through
//! a resolved `environment` plugin instead of running it on the local host.
//!
//! This is the runner-side twin of the kernel's ad-hoc `animus agent run`
//! environment seam (`orchestrator-cli` `environment_exec.rs`, TASK-166 Phase
//! 2). When a phase's harness resolves to a NON-LOCAL environment (a container
//! / remote sandbox plugin), the harness command that would have been spawned
//! on the host is instead prepared + executed inside that environment via
//! [`EnvironmentClient`] (`prepare` → `exec_stream` → `teardown`), with the
//! streamed stdout/stderr surfaced through the SAME [`SessionEvent`] channel a
//! local [`SessionRun`] yields — so the runner's existing
//! `process_phase_event_stream` consumer drives it UNCHANGED.
//!
//! ## Gate (default = byte-for-byte the current local path)
//!
//! [`resolve_exec_environment`] decides whether a phase is env-routed:
//!
//! - `ANIMUS_ENVIRONMENT_EXEC` unset → consult the compiled config via
//!   [`orchestrator_config::workflow_config::resolve_environment`], honoring the
//!   phase/workflow-level `environment:` overrides plus the phase's subject kind
//!   and harness against the `environment_routing:` table.
//! - `ANIMUS_ENVIRONMENT_EXEC` set to a falsy token (`0` / `false` / `no` /
//!   `off` / empty) → hard kill-switch: local execution even when config routes.
//! - `ANIMUS_ENVIRONMENT_EXEC` set to any other value → that value is the
//!   environment plugin id (dev/test override).
//!
//! A resolution of `None`, a LOCAL environment id (`local` / `worktree`), or a
//! resolved environment that declares NO repos to materialize leaves the
//! existing local path untouched.
//!
//! ## Adaptation vs. the kernel seam (the railway-plugin constraint)
//!
//! The kernel seam hardcodes the run's repo to `[project_root]` (a LOCAL path).
//! A remote environment plugin (e.g. railway) REJECTS a local path — it can
//! only clone remote urls. So here the [`EnvironmentSpec::repos`] are resolved
//! from (in precedence order): a referenced `workspace:` (a named [`Workspace`]
//! repo set from `phase.workspace` / `workflow.workspace`), else the matched
//! routing rule's inline `spec.repos` — each a list of `{url, git_ref?, name?,
//! primary?}`. An environment with no repos from either source is treated as
//! "not routable" and falls through to local execution rather than sending a
//! local path the plugin rejects.
//!
//! ## Failure posture
//!
//! Once a non-local environment IS resolved, the phase never silently falls
//! back to local execution — a missing plugin, a failed `prepare`, or a failed
//! exec fails the phase with an actionable error. The single transparent
//! fallback is protocol-level: a plugin that does not implement
//! `environment/exec_stream` (METHOD_NOT_SUPPORTED) is retried with the buffered
//! `environment/exec`, which is safe because METHOD_NOT_SUPPORTED means the
//! command never started.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use animus_environment_protocol::{
    EnvironmentHandle, EnvironmentSpec, ExecResponse, ExecStream, HarnessCommand, RepoRef,
};
use animus_plugin_protocol::error_codes::METHOD_NOT_SUPPORTED;
use animus_session_backend::session::{SessionEvent, SessionRequest, SessionRun};
use anyhow::{anyhow, Result};
use orchestrator_config::workflow_config::{resolve_environment, Workspace, WorkspaceRepo};
use orchestrator_core::EnvironmentClient;
use orchestrator_plugin_host::HostError;
use serde_json::Value;
use tokio::sync::mpsc;

/// Environment ids that mean "run on the host": the resolver treats them as
/// no-routing so the existing local spawn path stays byte-for-byte unchanged.
const LOCAL_ENVIRONMENT_IDS: &[&str] = &["local", "worktree"];

/// Default hard cap for an env-routed phase when the request sets no timeout.
/// Matches the kernel path's `DEFAULT_PLUGIN_RUN_TIMEOUT_SECS` (30 min) so
/// routed and local phases share the same default bound.
const DEFAULT_ENVIRONMENT_RUN_TIMEOUT_SECS: u64 = 1_800;

/// Whether `environment_id` names a LOCAL environment (host execution).
fn is_local_environment(environment_id: &str) -> bool {
    let normalized = environment_id.trim().to_ascii_lowercase();
    LOCAL_ENVIRONMENT_IDS.contains(&normalized.as_str())
}

/// A resolved non-local environment for a phase: the plugin id, the repos to
/// materialize into [`EnvironmentSpec::repos`] (from the referenced `workspace:`
/// or the matched routing rule), plus any opaque `spec` overrides the matched
/// rule carried.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedEnvironment {
    pub(crate) id: String,
    pub(crate) repos: Vec<RepoRef>,
    pub(crate) spec_overrides: Option<BTreeMap<String, Value>>,
}

/// The phase/workflow-level routing context threaded from the dispatch site into
/// [`resolve_exec_environment`]: the phase's subject kind, the phase- and
/// workflow-level `environment:` overrides, and the resolved `workspace:`
/// reference (`phase.workspace` falling back to `workflow.workspace`). All
/// `None` reproduces the pre-override behavior (routing-table-only resolution).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PhaseRouting<'a> {
    pub(crate) subject_kind: Option<&'a str>,
    pub(crate) phase_env: Option<&'a str>,
    pub(crate) workflow_env: Option<&'a str>,
    pub(crate) workspace: Option<&'a str>,
}

/// Resolve the NON-LOCAL environment (if any) that should execute this phase's
/// harness. Returns `None` for the default local path — see the module docs for
/// the full gate semantics. A resolved environment with no repos to materialize
/// also returns `None` (falls through to local) rather than sending a local
/// path a remote plugin would reject.
pub(crate) fn resolve_exec_environment(
    project_root: &Path,
    harness: &str,
    routing_ctx: PhaseRouting,
) -> Option<ResolvedEnvironment> {
    let resolved = match std::env::var("ANIMUS_ENVIRONMENT_EXEC") {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" | "0" | "false" | "no" | "off" => return None,
            _ => ResolvedEnvironment { id: raw.trim().to_string(), repos: Vec::new(), spec_overrides: None },
        },
        Err(_) => {
            let config = orchestrator_core::load_workflow_config_or_default(project_root).config;
            let routing = config.environment_routing.as_ref();
            // Precedence (see `resolve_environment`): phase `environment:` >
            // first matching routing rule > workflow `environment:` > routing
            // default. So a WorkflowDefinition `environment:` routes every phase,
            // and a phase-level `environment:` wins for that phase.
            let id = resolve_environment(
                routing_ctx.subject_kind,
                Some(harness),
                routing_ctx.phase_env,
                routing_ctx.workflow_env,
                routing,
            )?;
            // Re-find the routing rule that SELECTED this id so its repos +
            // opaque `spec` overrides ride the prepared EnvironmentSpec. A
            // phase-level `environment:` override wins BEFORE rules are consulted
            // (see `resolve_environment`), so when `phase_env` is set the id did
            // NOT come from a rule — attaching a coincidental same-id rule's
            // repos/spec would contradict the documented "override + no workspace
            // -> local" behavior. Otherwise the FIRST rule matching this phase's
            // (kind, harness) is exactly the one `resolve_environment` returned,
            // so its spec is the selected rule's. When no rule matches (id came
            // from `workflow_env` / the routing default), repos come from the
            // `workspace:` ref alone.
            let rule_spec = if routing_ctx.phase_env.is_some() {
                None
            } else {
                routing.and_then(|routing| {
                    routing
                        .rules
                        .iter()
                        .find(|rule| {
                            rule.match_on
                                .kind
                                .as_deref()
                                .is_none_or(|rule_kind| routing_ctx.subject_kind == Some(rule_kind))
                                && rule.match_on.harness.as_deref().is_none_or(|rule_harness| rule_harness == harness)
                        })
                        .filter(|rule| rule.environment == id)
                        .and_then(|rule| rule.spec.clone())
                })
            };
            // Repo precedence: a referenced `workspace:` (named repo set) wins;
            // else the matched routing rule's inline `spec.repos`.
            let workspace_repos = routing_ctx
                .workspace
                .and_then(|name| config.workspaces.get(name))
                .map(repos_from_workspace)
                .unwrap_or_default();
            let repos = if workspace_repos.is_empty() {
                rule_spec.as_ref().map(repos_from_spec).unwrap_or_default()
            } else {
                workspace_repos
            };
            ResolvedEnvironment { id, repos, spec_overrides: rule_spec }
        }
    };
    if is_local_environment(&resolved.id) || resolved.repos.is_empty() {
        return None;
    }
    Some(resolved)
}

/// Build the `repos` list from a named [`Workspace`] (repo set) referenced by
/// `phase.workspace` / `workflow.workspace`. Entries without a non-empty `url`
/// are skipped (a remote environment plugin can only clone from a url).
fn repos_from_workspace(workspace: &Workspace) -> Vec<RepoRef> {
    workspace.repos.iter().filter_map(repo_ref_from_workspace_repo).collect()
}

/// Convert a config [`WorkspaceRepo`] into the wire [`RepoRef`]. Returns `None`
/// when the entry carries no non-empty `url`.
fn repo_ref_from_workspace_repo(repo: &WorkspaceRepo) -> Option<RepoRef> {
    let url = repo.url.trim();
    if url.is_empty() {
        return None;
    }
    Some(RepoRef {
        url: url.to_string(),
        name: repo.name.as_deref().map(str::trim).filter(|name| !name.is_empty()).map(ToOwned::to_owned),
        git_ref: repo.git_ref.as_deref().map(str::trim).filter(|git_ref| !git_ref.is_empty()).map(ToOwned::to_owned),
        primary: repo.primary,
    })
}

/// Build the `repos` list from a routing rule's `spec.repos` entry: a JSON array
/// of `{url, git_ref?, name?, primary?}` objects. Entries without a non-empty
/// `url` are skipped (a remote environment plugin can only clone from a url).
fn repos_from_spec(spec: &BTreeMap<String, Value>) -> Vec<RepoRef> {
    let Some(entries) = spec.get("repos").and_then(Value::as_array) else {
        return Vec::new();
    };
    entries.iter().filter_map(repo_ref_from_value).collect()
}

/// Parse a single `{url, git_ref?, name?, primary?}` object into a [`RepoRef`].
/// Returns `None` when the entry carries no non-empty `url`.
fn repo_ref_from_value(value: &Value) -> Option<RepoRef> {
    let url = value.get("url").and_then(Value::as_str).map(str::trim).filter(|url| !url.is_empty())?;
    Some(RepoRef {
        url: url.to_string(),
        name: value
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned),
        git_ref: value
            .get("git_ref")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|git_ref| !git_ref.is_empty())
            .map(ToOwned::to_owned),
        primary: value.get("primary").and_then(Value::as_bool).unwrap_or(false),
    })
}

/// Start the phase inside the resolved non-local environment: resolve the
/// plugin, build the harness command + spec, and hand the pipeline to a
/// background thread that streams [`SessionEvent`]s into the returned
/// [`SessionRun`] — the same handle shape `start_session` produces, so the
/// runner's event loop is unchanged.
pub(crate) fn start_environment_session(
    project_root: &Path,
    environment: &ResolvedEnvironment,
    request: &SessionRequest,
) -> Result<SessionRun> {
    let environment_id = environment.id.as_str();
    let client = EnvironmentClient::resolve(project_root, environment_id).map_err(|err| {
        anyhow!(
            "workflow phase is routed to environment '{environment_id}' but no usable environment plugin was resolved \
             (the phase is NOT executed locally when an environment is requested): {err}"
        )
    })?;
    let (command, stdin) = harness_command_for_request(project_root, request)?;
    let mut spec = environment_spec_for_run(environment_id, environment.repos.clone(), request);
    if let Some(overrides) = environment.spec_overrides.clone() {
        apply_spec_overrides(&mut spec, overrides);
    }
    // Mirror the local path's default run cap: an un-timed env exec would
    // otherwise be unbounded, so a hung provider command inside the environment
    // would drain forever.
    let timeout = Some(Duration::from_secs(request.timeout_secs.unwrap_or(DEFAULT_ENVIRONMENT_RUN_TIMEOUT_SECS)));
    let backend_label = format!("environment:{}", client.plugin_name());
    Ok(spawn_environment_run(Arc::new(client), spec, command, stdin, timeout, backend_label))
}

/// Merge a routing rule's opaque `spec` overrides into the compiled
/// [`EnvironmentSpec`]: the wire-typed keys (`image`, `resources`, `env`) land
/// on their typed fields; `repos` is already consumed into
/// [`EnvironmentSpec::repos`] (skipped here); everything else is carried
/// opaquely on `metadata`.
fn apply_spec_overrides(spec: &mut EnvironmentSpec, overrides: BTreeMap<String, Value>) {
    let mut metadata = serde_json::Map::new();
    for (key, value) in overrides {
        match key.as_str() {
            // Consumed into the typed `repos` field by `repos_from_spec`.
            "repos" => {}
            "image" => spec.image = value.as_str().map(ToOwned::to_owned),
            "resources" => spec.resources = Some(value),
            "env" => {
                if let Some(env) = value.as_object() {
                    for (env_key, env_value) in env {
                        if let Some(env_value) = env_value.as_str() {
                            spec.env.insert(env_key.clone(), env_value.to_string());
                        }
                    }
                }
            }
            other => {
                metadata.insert(other.to_string(), value);
            }
        }
    }
    if !metadata.is_empty() {
        spec.metadata = Value::Object(metadata);
    }
}

/// Build the [`HarnessCommand`] that mirrors what would have been spawned
/// locally. The request's assembled `runtime_contract.cli.launch` block wins
/// when present; otherwise the launch argv is built from the same
/// [`orchestrator_core::runtime_contract::build_cli_launch_contract`] table the
/// contract path uses. Either way the argv is normalized for the env-exec
/// transport:
///
/// - machine-output flags are stripped (see [`plain_text_launch_args`]): the
///   env path forwards raw stdout as text deltas rather than running a
///   provider-plugin stream parser;
/// - an explicit `permission_mode` is re-applied via
///   [`apply_permission_mode_to_launch`] (idempotent when the graft already
///   applied it), so a mode like `plan` is never silently dropped;
/// - kernel-mediated approvals (`extras.approvals`) FAIL CLOSED: their gate
///   rides transport-level wiring that does not reach inside an environment.
///
/// Returns the command plus the stdin payload for it: a contract launch with
/// `prompt_via_stdin: true` omits the prompt from the argv, so the prompt is fed
/// to the command's stdin instead.
fn harness_command_for_request(
    project_root: &Path,
    request: &SessionRequest,
) -> Result<(HarnessCommand, Option<String>)> {
    if request.extras.pointer("/approvals").and_then(Value::as_bool).unwrap_or(false) {
        return Err(anyhow!(
            "kernel-mediated approvals are not supported for environment-routed phases yet: the approval gate is \
             wired at the provider transport layer, which an environment exec bypasses. Drop the agent profile's \
             approval_policy, or run this phase locally."
        ));
    }

    let system_prompt = request
        .extras
        .pointer("/system_prompt")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|prompt| !prompt.is_empty());
    let is_claude = request.tool.trim().eq_ignore_ascii_case("claude");
    let contract_launch = request.extras.pointer("/runtime_contract/cli/launch").cloned();
    let effective_prompt = match system_prompt {
        Some(system) if !is_claude => format!("{system}\n\n{}", request.prompt),
        _ => request.prompt.clone(),
    };
    let prompt_via_stdin = contract_launch
        .as_ref()
        .and_then(|launch| launch.get("prompt_via_stdin"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if system_prompt.is_some() && contract_launch.is_some() && !is_claude && !prompt_via_stdin {
        return Err(anyhow!(
            "a system prompt cannot be applied to tool '{}' through its runtime contract launch inside an \
             environment (the contract embeds the prompt in its argv). Drop the system prompt / skill system \
             fragments, or run this phase locally.",
            request.tool
        ));
    }

    let launch = contract_launch.as_ref().and_then(launch_program_args).or_else(|| {
        orchestrator_core::runtime_contract::build_cli_launch_contract(
            &request.tool,
            &request.model,
            &effective_prompt,
            None,
            None,
        )
        .as_ref()
        .and_then(launch_program_args)
    });
    let Some((program, mut args)) = launch else {
        return Err(anyhow!(
            "cannot build a harness command for tool '{}' to run inside an environment plugin \
             (no runtime_contract cli.launch and no built-in launch table entry for this tool)",
            request.tool
        ));
    };
    if let (Some(system), true) = (system_prompt, is_claude) {
        args.push("--append-system-prompt".to_string());
        args.push(system.to_string());
    }
    let args = plain_text_launch_args(&request.tool, args);
    let args = apply_permission_mode(&request.tool, args, request.permission_mode.as_deref());
    let args = apply_reasoning_effort(
        &request.tool,
        args,
        request.extras.pointer("/reasoning_effort").and_then(Value::as_str),
    );

    let mut env: BTreeMap<String, String> = BTreeMap::new();
    if let Some(launch_env) = contract_launch.as_ref().and_then(|launch| launch.get("env")).and_then(Value::as_object) {
        for (key, value) in launch_env {
            if let Some(value) = value.as_str() {
                env.insert(key.clone(), value.to_string());
            }
        }
    }
    // Explicit request env wins over contract launch env on collision.
    for (key, value) in &request.env_vars {
        env.insert(key.clone(), value.clone());
    }

    let stdin = prompt_via_stdin.then(|| effective_prompt.clone());

    Ok((HarnessCommand { program, args, env, cwd: relative_cwd_for_run(project_root, request) }, stdin))
}

/// Map the request's (host) cwd into the environment. `HarnessCommand::cwd` is
/// defined relative to the prepared `workspace_root`, so a caller cwd BELOW the
/// project root maps to the same relative subpath; a cwd AT the project root
/// (the default) maps to `None`, letting the environment run in its primary repo
/// directory.
fn relative_cwd_for_run(project_root: &Path, request: &SessionRequest) -> Option<String> {
    let root = project_root.canonicalize().unwrap_or_else(|_| project_root.to_path_buf());
    let cwd = request.cwd.canonicalize().unwrap_or_else(|_| request.cwd.clone());
    let relative = cwd.strip_prefix(&root).ok()?;
    if relative.as_os_str().is_empty() {
        return None;
    }
    Some(relative.to_string_lossy().to_string())
}

/// Re-apply an explicit permission mode onto the env-exec argv via the same
/// transform the local transports/grafts use. Idempotent when a grafted contract
/// already applied the mode; a no-op when no mode is set.
fn apply_permission_mode(tool: &str, args: Vec<String>, permission_mode: Option<&str>) -> Vec<String> {
    if permission_mode.map(str::trim).is_none_or(str::is_empty) {
        return args;
    }
    let mut contract = serde_json::json!({ "cli": { "launch": { "args": args } } });
    apply_permission_mode_to_launch(&mut contract, tool, permission_mode);
    contract
        .pointer("/cli/launch/args")
        .and_then(Value::as_array)
        .map(|args| args.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect())
        .unwrap_or_default()
}

/// Re-apply an explicit permission mode onto a grafted launch block so the CLI
/// flag keeps winning once the transport's own argv assembly is replaced by the
/// contract launch. claude: swap the default `--dangerously-skip-permissions`
/// for `--permission-mode <mode>`; codex: upsert `-c approval_policy="<mode>"`.
/// Other tools pass through.
///
/// Inlined equivalent of the kernel's
/// `runtime_agent::provider_client::apply_permission_mode_to_launch`, which the
/// out-of-tree runner cannot import.
fn apply_permission_mode_to_launch(contract: &mut Value, tool: &str, permission_mode: Option<&str>) {
    let Some(mode) = permission_mode.map(str::trim).filter(|mode| !mode.is_empty()) else {
        return;
    };
    match tool.trim().to_ascii_lowercase().as_str() {
        "claude" => {
            if let Some(args) = contract.pointer_mut("/cli/launch/args").and_then(Value::as_array_mut) {
                args.retain(|arg| arg.as_str() != Some("--dangerously-skip-permissions"));
                if let Some(pos) = args.iter().position(|arg| arg.as_str() == Some("--permission-mode")) {
                    if pos + 1 < args.len() {
                        args[pos + 1] = Value::String(mode.to_string());
                    } else {
                        args.push(Value::String(mode.to_string()));
                    }
                } else {
                    let insert_at = 1.min(args.len());
                    args.insert(insert_at, Value::String("--permission-mode".to_string()));
                    args.insert(insert_at + 1, Value::String(mode.to_string()));
                }
            }
        }
        "codex" => {
            animus_runtime_shared::inject_codex_config_overrides_list(
                contract,
                "codex",
                &[format!("approval_policy=\"{mode}\"")],
            );
        }
        _ => {}
    }
}

/// Re-apply an explicit reasoning-effort request onto the env-exec argv: codex
/// carries it as a `model_reasoning_effort` config override; other tools do not
/// map a launch flag for it today, so they pass through.
fn apply_reasoning_effort(tool: &str, args: Vec<String>, reasoning_effort: Option<&str>) -> Vec<String> {
    let Some(effort) = reasoning_effort.map(str::trim).filter(|effort| !effort.is_empty()) else {
        return args;
    };
    if !tool.trim().eq_ignore_ascii_case("codex") {
        return args;
    }
    let mut contract = serde_json::json!({ "cli": { "launch": { "args": args } } });
    animus_runtime_shared::inject_codex_config_overrides_list(
        &mut contract,
        "codex",
        &[format!("model_reasoning_effort={}", effort.to_ascii_lowercase())],
    );
    contract
        .pointer("/cli/launch/args")
        .and_then(Value::as_array)
        .map(|args| args.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect())
        .unwrap_or_default()
}

/// Strip the machine-output flags from a launch argv so the command emits plain
/// text. The env-exec path has no provider stream parser — stdout is surfaced
/// directly as text deltas — so the structured-output mode would leak raw JSON
/// into rendered/persisted output.
fn plain_text_launch_args(tool: &str, args: Vec<String>) -> Vec<String> {
    let normalized = tool.trim().to_ascii_lowercase();
    let (strip_flags, strip_pairs): (&[&str], &[&str]) = match normalized.as_str() {
        "claude" => (&["--verbose"], &["--output-format"]),
        "codex" => (&["--json"], &[]),
        "gemini" => (&[], &["--output-format"]),
        "opencode" | "oai-runner" => (&[], &["--format"]),
        _ => (&[], &[]),
    };
    let mut plain = Vec::with_capacity(args.len());
    let mut iter = args.into_iter().peekable();
    while let Some(arg) = iter.next() {
        if strip_pairs.contains(&arg.as_str()) {
            let _ = iter.next();
            continue;
        }
        if strip_flags.contains(&arg.as_str()) {
            continue;
        }
        plain.push(arg);
    }
    plain
}

/// Extract `(command, args)` from a `cli.launch` JSON block. Returns `None` when
/// the block carries no non-empty `command`.
fn launch_program_args(launch: &Value) -> Option<(String, Vec<String>)> {
    let program = launch.get("command").and_then(Value::as_str).map(str::trim).filter(|cmd| !cmd.is_empty())?;
    let args = launch
        .get("args")
        .and_then(Value::as_array)
        .map(|args| args.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect())
        .unwrap_or_default();
    Some((program.to_string(), args))
}

/// Build the [`EnvironmentSpec`] for a phase run. `repos` come from the matched
/// routing rule (see the module docs); image/resources/metadata are left to the
/// plugin's defaults unless a rule `spec` override sets them.
fn environment_spec_for_run(environment_id: &str, repos: Vec<RepoRef>, request: &SessionRequest) -> EnvironmentSpec {
    let mut env = BTreeMap::new();
    for (key, value) in &request.env_vars {
        env.insert(key.clone(), value.clone());
    }
    EnvironmentSpec {
        kind: environment_id.to_string(),
        repos,
        image: None,
        resources: None,
        env,
        metadata: Value::Null,
    }
}

/// The prepare/exec/teardown surface the pipeline drives. Production wraps
/// [`EnvironmentClient`]; tests inject a fake so the pipeline's event sequencing
/// and teardown guarantees are exercised without a plugin binary.
pub(crate) trait EnvironmentExecBackend: Send + Sync + 'static {
    fn prepare(&self, spec: EnvironmentSpec) -> Result<EnvironmentHandle>;
    fn exec_stream(
        &self,
        handle: &EnvironmentHandle,
        command: HarnessCommand,
        stdin: Option<String>,
        timeout: Option<Duration>,
        on_output: &(dyn Fn(ExecStream, &str) + Send + Sync),
    ) -> Result<ExecResponse>;
    fn exec(
        &self,
        handle: &EnvironmentHandle,
        command: HarnessCommand,
        stdin: Option<String>,
        timeout: Option<Duration>,
    ) -> Result<ExecResponse>;
    fn teardown(&self, handle: &EnvironmentHandle) -> Result<()>;
}

impl EnvironmentExecBackend for EnvironmentClient {
    fn prepare(&self, spec: EnvironmentSpec) -> Result<EnvironmentHandle> {
        EnvironmentClient::prepare(self, spec)
    }

    fn exec_stream(
        &self,
        handle: &EnvironmentHandle,
        command: HarnessCommand,
        stdin: Option<String>,
        timeout: Option<Duration>,
        on_output: &(dyn Fn(ExecStream, &str) + Send + Sync),
    ) -> Result<ExecResponse> {
        EnvironmentClient::exec_stream(self, handle, command, BTreeMap::new(), stdin, timeout, on_output)
    }

    fn exec(
        &self,
        handle: &EnvironmentHandle,
        command: HarnessCommand,
        stdin: Option<String>,
        timeout: Option<Duration>,
    ) -> Result<ExecResponse> {
        EnvironmentClient::exec(self, handle, command, BTreeMap::new(), stdin, timeout)
    }

    fn teardown(&self, handle: &EnvironmentHandle) -> Result<()> {
        EnvironmentClient::teardown(self, handle)
    }
}

/// Spawn the environment pipeline on a dedicated OS thread (the
/// [`EnvironmentClient`] surface is blocking) and return a [`SessionRun`] whose
/// event stream mirrors the local provider path. Must be called from within a
/// tokio runtime (the forwarder task bridges the pipeline's unbounded sends onto
/// the bounded `SessionRun` channel).
pub(crate) fn spawn_environment_run(
    backend: Arc<dyn EnvironmentExecBackend>,
    spec: EnvironmentSpec,
    command: HarnessCommand,
    stdin: Option<String>,
    timeout: Option<Duration>,
    backend_label: String,
) -> SessionRun {
    let (events_tx, events_rx) = mpsc::channel::<SessionEvent>(256);
    // The pipeline thread sends through an unbounded channel: the exec_stream
    // output callback fires inside the client's internal runtime, where a
    // bounded blocking send is not allowed. The forwarder task applies the
    // bounded channel's backpressure on the async side.
    let (pipeline_tx, mut pipeline_rx) = mpsc::unbounded_channel::<SessionEvent>();
    tokio::spawn(async move {
        while let Some(event) = pipeline_rx.recv().await {
            if events_tx.send(event).await.is_err() {
                break;
            }
        }
    });

    let selected_backend = backend_label.clone();
    std::thread::spawn(move || {
        run_environment_pipeline(backend.as_ref(), spec, command, stdin, timeout, &backend_label, &pipeline_tx);
    });

    SessionRun { session_id: None, events: events_rx, selected_backend, fallback_reason: None, pid: None }
}

/// The blocking prepare → exec_stream → teardown pipeline. Emits the same event
/// grammar the provider path does: `Started`, stdout deltas as `TextDelta`,
/// stderr deltas as recoverable `Error` frames, then a terminal `Finished` (or
/// an unrecoverable `Error`). Teardown always runs once a handle exists.
fn run_environment_pipeline(
    backend: &dyn EnvironmentExecBackend,
    spec: EnvironmentSpec,
    command: HarnessCommand,
    stdin: Option<String>,
    timeout: Option<Duration>,
    backend_label: &str,
    events: &mpsc::UnboundedSender<SessionEvent>,
) {
    let send = |event: SessionEvent| {
        let _ = events.send(event);
    };
    send(SessionEvent::Started { backend: backend_label.to_string(), session_id: None, pid: None });

    let handle = match backend.prepare(spec) {
        Ok(handle) => handle,
        Err(err) => {
            send(SessionEvent::Error {
                message: format!("environment prepare failed for {backend_label}: {err:#}"),
                recoverable: false,
            });
            return;
        }
    };

    let stream_events = events.clone();
    let on_output = move |stream: ExecStream, text: &str| {
        let event = match stream {
            ExecStream::Stdout => SessionEvent::TextDelta { text: text.to_string() },
            ExecStream::Stderr => SessionEvent::Error { message: text.to_string(), recoverable: true },
        };
        let _ = stream_events.send(event);
    };

    let result = match backend.exec_stream(&handle, command.clone(), stdin.clone(), timeout, &on_output) {
        // A plugin without exec_stream support answers METHOD_NOT_SUPPORTED
        // before the command ever starts, so retrying with the buffered exec is
        // safe (no double-execution risk). The aggregated output is emitted
        // once, post-hoc — deltas were never streamed.
        Err(err) if is_method_not_supported(&err) => {
            backend.exec(&handle, command, stdin, timeout).inspect(|response| {
                if !response.stdout.is_empty() {
                    send(SessionEvent::FinalText { text: response.stdout.clone() });
                }
                if !response.stderr.is_empty() {
                    send(SessionEvent::Error { message: response.stderr.clone(), recoverable: true });
                }
            })
        }
        result => result,
    };

    // Teardown regardless of exec outcome; a teardown failure is surfaced as a
    // recoverable frame (the run's verdict is the exec result, not cleanup).
    if let Err(err) = backend.teardown(&handle) {
        send(SessionEvent::Error {
            message: format!("environment teardown failed for {backend_label} (handle {}): {err:#}", handle.id),
            recoverable: true,
        });
    }

    match result {
        Ok(response) if response.timed_out => send(SessionEvent::Error {
            message: format!(
                "environment exec timed out in {backend_label}{}",
                timeout.map(|t| format!(" after {}s", t.as_secs())).unwrap_or_default()
            ),
            recoverable: false,
        }),
        // A finished command with NO exit code was signal-killed / OOM-killed:
        // downstream status treats `exit_code: None` as success, so surface it
        // as a terminal error instead.
        Ok(ExecResponse { exit_code: None, .. }) => send(SessionEvent::Error {
            message: format!(
                "environment exec in {backend_label} ended without an exit code (terminated by a signal?)"
            ),
            recoverable: false,
        }),
        Ok(response) => send(SessionEvent::Finished { exit_code: response.exit_code }),
        Err(err) => send(SessionEvent::Error {
            message: format!("environment exec failed in {backend_label}: {err:#}"),
            recoverable: false,
        }),
    }
}

/// Whether an exec error is the plugin's structured METHOD_NOT_SUPPORTED
/// rejection of `environment/exec_stream` — the only error class where a
/// buffered-exec retry is guaranteed side-effect free.
fn is_method_not_supported(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<HostError>(),
            Some(HostError::Rpc(rpc)) if rpc.code == METHOD_NOT_SUPPORTED
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use animus_plugin_protocol::RpcError;
    use protocol::test_utils::EnvVarGuard;

    fn sample_request(tool: &str) -> SessionRequest {
        SessionRequest {
            tool: tool.to_string(),
            model: "some-model".to_string(),
            prompt: "do the thing".to_string(),
            cwd: std::path::PathBuf::from("."),
            project_root: None,
            mcp_endpoint: None,
            mcp_servers: None,
            permission_mode: None,
            timeout_secs: None,
            env_vars: Vec::new(),
            extras: serde_json::json!({}),
            actor: None,
        }
    }

    // -----------------------------------------------------------------
    // Gate: resolve_exec_environment
    // -----------------------------------------------------------------

    /// THE Phase-B regression pin: with no `ANIMUS_ENVIRONMENT_EXEC` and no
    /// `environment_routing:` in config, the gate resolves to `None`, so the
    /// dispatch takes the exact pre-existing `start_session` local path.
    #[test]
    fn default_resolves_to_no_environment_so_the_local_path_is_unchanged() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(resolve_exec_environment(tmp.path(), "claude", PhaseRouting::default()), None);
    }

    #[test]
    fn env_var_falsy_tokens_disable_routing() {
        let _lock = crate::test_env::scoped_state_serializer();
        let tmp = tempfile::tempdir().unwrap();
        for token in ["", "0", "false", "no", "off", "False", "OFF", "No"] {
            let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", Some(token));
            assert_eq!(
                resolve_exec_environment(tmp.path(), "claude", PhaseRouting::default()),
                None,
                "token {token:?} must disable"
            );
        }
    }

    /// The bare `ANIMUS_ENVIRONMENT_EXEC=<id>` override carries no repos, so the
    /// adapted gate falls through to local rather than sending a local path.
    #[test]
    fn env_var_explicit_id_without_repos_falls_through_to_local() {
        let _lock = crate::test_env::scoped_state_serializer();
        let tmp = tempfile::tempdir().unwrap();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", Some("container"));
        assert_eq!(resolve_exec_environment(tmp.path(), "claude", PhaseRouting::default()), None, "no repos -> local");
        for local in ["local", "worktree", "Worktree", " local "] {
            let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", Some(local));
            assert_eq!(
                resolve_exec_environment(tmp.path(), "claude", PhaseRouting::default()),
                None,
                "id {local:?} is local"
            );
        }
    }

    #[test]
    fn config_routing_selects_the_environment_and_carries_repos() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".animus")).unwrap();
        std::fs::write(
            root.join(".animus").join("workflows.yaml"),
            r#"
environment_routing:
  rules:
    - match:
        harness: codex
      environment: sandbox-env
      spec:
        repos:
          - url: https://github.com/acme/app.git
            git_ref: main
            primary: true
"#,
        )
        .unwrap();
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        let resolved = resolve_exec_environment(&root, "codex", PhaseRouting::default()).expect("rule routes");
        assert_eq!(resolved.id, "sandbox-env");
        assert_eq!(resolved.repos.len(), 1);
        assert_eq!(resolved.repos[0].url, "https://github.com/acme/app.git");
        assert_eq!(resolved.repos[0].git_ref.as_deref(), Some("main"));
        assert!(resolved.repos[0].primary);
        // Non-matching harness, no default -> local path.
        assert_eq!(resolve_exec_environment(&root, "claude", PhaseRouting::default()), None);
    }

    /// A routing rule that selects a non-local environment but declares NO repos
    /// falls through to local — a remote plugin would reject a local path.
    #[test]
    fn config_routing_without_repos_falls_through_to_local() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".animus")).unwrap();
        std::fs::write(
            root.join(".animus").join("workflows.yaml"),
            r#"
environment_routing:
  rules:
    - match:
        harness: codex
      environment: sandbox-env
"#,
        )
        .unwrap();
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        assert_eq!(
            resolve_exec_environment(&root, "codex", PhaseRouting::default()),
            None,
            "a rule with no repos must not route"
        );
    }

    #[test]
    fn config_routing_local_default_keeps_the_local_path() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".animus")).unwrap();
        std::fs::write(
            root.join(".animus").join("workflows.yaml"),
            r#"
environment_routing:
  default: local
"#,
        )
        .unwrap();
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        assert_eq!(
            resolve_exec_environment(&root, "claude", PhaseRouting::default()),
            None,
            "the documented `default: local` stays local"
        );
    }

    // -----------------------------------------------------------------
    // Gate: phase/workflow `environment:` overrides + `workspace:` repos
    // -----------------------------------------------------------------

    /// Write a config whose only routing input is a named `workspaces:` repo set
    /// (`app-set` with two repos, plus an empty `empty-set`). Environment routing
    /// then rides the phase/workflow `environment:` overrides passed via
    /// [`PhaseRouting`], NOT an `environment_routing:` rule.
    fn write_workspace_config(root: &Path) {
        std::fs::create_dir_all(root.join(".animus")).unwrap();
        std::fs::write(
            root.join(".animus").join("workflows.yaml"),
            r#"
workspaces:
  app-set:
    repos:
      - url: https://github.com/acme/app.git
        name: app
        git_ref: main
        primary: true
      - url: https://github.com/acme/lib.git
  empty-set:
    repos: []
"#,
        )
        .unwrap();
    }

    /// A WorkflowDefinition-level `environment:` routes every phase: with no
    /// routing rule, the workflow env id is resolved and the referenced
    /// `workspace:` supplies the repos.
    #[test]
    fn workflow_level_environment_routes_with_workspace_repos() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        write_workspace_config(&root);
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        let routing =
            PhaseRouting { workflow_env: Some("railway-env-123"), workspace: Some("app-set"), ..Default::default() };
        let resolved = resolve_exec_environment(&root, "claude", routing).expect("workflow env routes");
        assert_eq!(resolved.id, "railway-env-123");
        assert_eq!(resolved.repos.len(), 2, "the referenced workspace's repos populate the spec");
        assert_eq!(resolved.repos[0].url, "https://github.com/acme/app.git");
        assert!(resolved.repos[0].primary);
        assert_eq!(resolved.repos[1].url, "https://github.com/acme/lib.git");
    }

    /// A phase-level `environment:` wins over the workflow-level one for that
    /// phase (precedence: phase_env > rule > workflow_env > default).
    #[test]
    fn phase_level_environment_overrides_the_workflow_environment() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        write_workspace_config(&root);
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        let routing = PhaseRouting {
            phase_env: Some("phase-env"),
            workflow_env: Some("workflow-env"),
            workspace: Some("app-set"),
            ..Default::default()
        };
        let resolved = resolve_exec_environment(&root, "claude", routing).expect("phase env wins");
        assert_eq!(resolved.id, "phase-env", "phase-level environment overrides the workflow's");
        assert_eq!(resolved.repos.len(), 2);
    }

    /// A phase-level `environment:` override wins BEFORE routing rules are
    /// consulted, so a coincidental same-id rule must NOT lend its repos/spec to
    /// the override. With no `workspace:`, the override then has no repos and
    /// falls through to local — whereas the SAME rule DOES route when it (not a
    /// phase override) is what selected the environment.
    #[test]
    fn phase_override_does_not_inherit_a_coincidental_rules_repos() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".animus")).unwrap();
        std::fs::write(
            root.join(".animus").join("workflows.yaml"),
            r#"
environment_routing:
  rules:
    - match:
        harness: claude
      environment: sandbox-env
      spec:
        repos:
          - url: https://github.com/acme/app.git
            primary: true
"#,
        )
        .unwrap();
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        // Phase override to the SAME id, no workspace: the rule's repos must not
        // be borrowed -> no repos -> local execution.
        let routing = PhaseRouting { phase_env: Some("sandbox-env"), ..Default::default() };
        assert_eq!(
            resolve_exec_environment(&root, "claude", routing),
            None,
            "a phase override must not inherit a same-id routing rule's repos"
        );

        // The very same rule DOES route when it is the selector (no phase override).
        let resolved =
            resolve_exec_environment(&root, "claude", PhaseRouting::default()).expect("the rule itself still routes");
        assert_eq!(resolved.id, "sandbox-env");
        assert_eq!(resolved.repos.len(), 1);
    }

    /// The `workspace:` repos flow all the way into the built `EnvironmentSpec`,
    /// with no local path leaking into the repo list.
    #[test]
    fn workspace_repos_populate_the_environment_spec() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        write_workspace_config(&root);
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        let routing =
            PhaseRouting { workflow_env: Some("railway-env"), workspace: Some("app-set"), ..Default::default() };
        let resolved = resolve_exec_environment(&root, "claude", routing).expect("routes");
        let spec = environment_spec_for_run(&resolved.id, resolved.repos.clone(), &sample_request("claude"));
        assert_eq!(spec.repos.len(), 2);
        assert_eq!(spec.repos[0].name.as_deref(), Some("app"));
        assert_eq!(spec.repos[0].git_ref.as_deref(), Some("main"));
        assert!(spec.repos[0].primary);
        assert!(!spec.repos[1].primary, "primary defaults to false");
        assert!(!spec.repos.iter().any(|repo| repo.url.starts_with('/')), "no local path leaks into repos");
    }

    /// A phase/workflow `environment:` that is non-local but has NO repos —
    /// because the `workspace:` is missing/empty and no routing rule matches —
    /// falls through to local rather than sending a local path a plugin rejects.
    #[test]
    fn non_local_environment_without_repos_falls_through_to_local() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        write_workspace_config(&root);
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        // No workspace ref at all -> no repos -> local.
        assert_eq!(
            resolve_exec_environment(
                &root,
                "claude",
                PhaseRouting { workflow_env: Some("railway-env"), ..Default::default() }
            ),
            None,
            "a non-local env with no workspace and no rule repos must not route"
        );
        // Unknown workspace name -> no repos -> local.
        assert_eq!(
            resolve_exec_environment(
                &root,
                "claude",
                PhaseRouting {
                    workflow_env: Some("railway-env"),
                    workspace: Some("does-not-exist"),
                    ..Default::default()
                }
            ),
            None,
            "an unresolvable workspace ref must not route"
        );
        // Defined-but-empty workspace -> no repos -> local.
        assert_eq!(
            resolve_exec_environment(
                &root,
                "claude",
                PhaseRouting { workflow_env: Some("railway-env"), workspace: Some("empty-set"), ..Default::default() }
            ),
            None,
            "an empty workspace repo set must not route"
        );
    }

    // -----------------------------------------------------------------
    // Spec builder: repos from the routing rule
    // -----------------------------------------------------------------

    #[test]
    fn routing_rule_repos_and_spec_overrides_reach_the_environment_spec() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".animus")).unwrap();
        std::fs::write(
            root.join(".animus").join("workflows.yaml"),
            r#"
environment_routing:
  rules:
    - match:
        harness: claude
      environment: container-env
      spec:
        image: acme/dev:latest
        env:
          IN_CONTAINER: "1"
        network: none
        repos:
          - url: https://github.com/acme/app.git
            name: app
            git_ref: feature/x
            primary: true
          - url: https://github.com/acme/lib.git
"#,
        )
        .unwrap();
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        let resolved = resolve_exec_environment(&root, "claude", PhaseRouting::default()).expect("rule routes");
        assert_eq!(resolved.id, "container-env");

        let mut spec = environment_spec_for_run(&resolved.id, resolved.repos.clone(), &sample_request("claude"));
        apply_spec_overrides(&mut spec, resolved.spec_overrides.clone().expect("rule spec carried"));

        // Repos come from the RULE, not a local project path.
        assert_eq!(spec.repos.len(), 2, "both rule repos reach the spec");
        assert_eq!(spec.repos[0].url, "https://github.com/acme/app.git");
        assert_eq!(spec.repos[0].name.as_deref(), Some("app"));
        assert_eq!(spec.repos[0].git_ref.as_deref(), Some("feature/x"));
        assert!(spec.repos[0].primary);
        assert_eq!(spec.repos[1].url, "https://github.com/acme/lib.git");
        assert!(!spec.repos[1].primary, "primary defaults to false");
        assert!(!spec.repos.iter().any(|repo| repo.url.starts_with('/')), "no local path leaks into repos");

        // The rest of the rule spec still lands on its typed / opaque fields.
        assert_eq!(spec.image.as_deref(), Some("acme/dev:latest"));
        assert_eq!(spec.env.get("IN_CONTAINER").map(String::as_str), Some("1"));
        assert_eq!(spec.metadata.pointer("/network").and_then(Value::as_str), Some("none"));
        // `repos` was consumed into the typed field, not echoed onto metadata.
        assert!(spec.metadata.pointer("/repos").is_none(), "repos must not ride metadata opaquely");
    }

    #[test]
    fn repo_ref_from_value_skips_entries_without_a_url() {
        let spec: BTreeMap<String, Value> = serde_json::from_value(serde_json::json!({
            "repos": [
                { "git_ref": "main" },
                { "url": "  " },
                { "url": "https://github.com/acme/app.git" }
            ]
        }))
        .unwrap();
        let repos = repos_from_spec(&spec);
        assert_eq!(repos.len(), 1, "only the entry with a non-empty url survives");
        assert_eq!(repos[0].url, "https://github.com/acme/app.git");
    }

    // -----------------------------------------------------------------
    // Harness command construction
    // -----------------------------------------------------------------

    #[test]
    fn harness_command_builds_a_plain_text_launch_for_a_known_tool() {
        let request = sample_request("claude");
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("known tool builds");
        assert_eq!(command.program, "claude");
        assert!(command.args.contains(&"--print".to_string()), "launch args mirror the local argv: {:?}", command.args);
        assert_eq!(command.args.last().map(String::as_str), Some("do the thing"), "prompt rides the argv");
        assert!(
            !command.args.iter().any(|arg| arg == "--output-format" || arg == "stream-json" || arg == "--verbose"),
            "machine-output flags stripped for env exec: {:?}",
            command.args
        );
        assert!(command.cwd.is_none(), "cwd at the project root defaults to the environment's primary repo dir");
    }

    #[test]
    fn plain_text_launch_strips_json_mode_per_tool() {
        let codex = plain_text_launch_args(
            "codex",
            vec!["exec".to_string(), "--json".to_string(), "--full-auto".to_string(), "prompt".to_string()],
        );
        assert_eq!(codex, vec!["exec".to_string(), "--full-auto".to_string(), "prompt".to_string()]);

        let gemini = plain_text_launch_args(
            "gemini",
            vec!["--output-format".to_string(), "json".to_string(), "-p".to_string(), "prompt".to_string()],
        );
        assert_eq!(gemini, vec!["-p".to_string(), "prompt".to_string()]);

        let other = plain_text_launch_args("other", vec!["--json".to_string()]);
        assert_eq!(other, vec!["--json".to_string()]);
    }

    #[test]
    fn harness_command_maps_a_subdirectory_cwd_into_the_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("crates").join("web")).unwrap();

        let mut request = sample_request("claude");
        request.cwd = root.join("crates").join("web");
        let (command, _stdin) = harness_command_for_request(&root, &request).expect("builds");
        assert_eq!(command.cwd.as_deref(), Some("crates/web"), "a caller cwd below the root maps to the subpath");

        request.cwd = root.clone();
        let (command, _stdin) = harness_command_for_request(&root, &request).expect("builds");
        assert!(command.cwd.is_none(), "a cwd at the project root maps to the environment default");
    }

    #[test]
    fn harness_command_prefers_the_assembled_contract_launch() {
        let mut request = sample_request("claude");
        request.extras = serde_json::json!({
            "runtime_contract": {
                "cli": {
                    "name": "claude",
                    "launch": {
                        "command": "custom-claude",
                        "args": ["--flag", "value"],
                        "env": { "FROM_LAUNCH": "launch", "SHARED": "launch" }
                    }
                }
            }
        });
        request.env_vars = vec![("SHARED".to_string(), "request".to_string())];
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("contract launch builds");
        assert_eq!(command.program, "custom-claude");
        assert_eq!(command.args, vec!["--flag".to_string(), "value".to_string()]);
        assert_eq!(command.env.get("FROM_LAUNCH").map(String::as_str), Some("launch"));
        assert_eq!(command.env.get("SHARED").map(String::as_str), Some("request"), "request env wins on collision");
    }

    #[test]
    fn harness_command_strips_machine_output_from_a_contract_launch_too() {
        let mut request = sample_request("claude");
        request.extras = serde_json::json!({
            "runtime_contract": {
                "cli": {
                    "name": "claude",
                    "launch": {
                        "command": "claude",
                        "args": ["--print", "--verbose", "--output-format", "stream-json", "prompt"]
                    }
                }
            }
        });
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("contract launch builds");
        assert_eq!(command.args, vec!["--print".to_string(), "prompt".to_string()]);
    }

    #[test]
    fn harness_command_feeds_the_prompt_via_stdin_when_the_contract_says_so() {
        let mut request = sample_request("claude");
        request.extras = serde_json::json!({
            "runtime_contract": {
                "cli": {
                    "name": "claude",
                    "launch": {
                        "command": "claude",
                        "args": ["--print"],
                        "prompt_via_stdin": true
                    }
                }
            }
        });
        let (command, stdin) = harness_command_for_request(Path::new("."), &request).expect("builds");
        assert_eq!(stdin.as_deref(), Some("do the thing"), "prompt rides stdin, mirroring the local transport");
        assert!(!command.args.iter().any(|arg| arg == "do the thing"), "prompt stays off the argv");

        let (_, stdin) = harness_command_for_request(Path::new("."), &sample_request("claude")).expect("builds");
        assert!(stdin.is_none());
    }

    #[test]
    fn harness_command_applies_an_explicit_permission_mode() {
        let mut request = sample_request("claude");
        request.permission_mode = Some("plan".to_string());
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("builds");
        assert!(
            !command.args.iter().any(|arg| arg == "--dangerously-skip-permissions"),
            "the explicit mode replaces the skip-permissions default: {:?}",
            command.args
        );
        let pos = command
            .args
            .iter()
            .position(|arg| arg == "--permission-mode")
            .expect("permission mode flag applied to the env launch");
        assert_eq!(command.args.get(pos + 1).map(String::as_str), Some("plan"));
    }

    #[test]
    fn harness_command_maps_the_system_prompt_per_tool() {
        let mut request = sample_request("claude");
        request.extras = serde_json::json!({ "system_prompt": "You are guided." });
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("builds");
        let pos = command
            .args
            .iter()
            .position(|arg| arg == "--append-system-prompt")
            .expect("claude system prompt rides the flag");
        assert_eq!(command.args.get(pos + 1).map(String::as_str), Some("You are guided."));

        let mut request = sample_request("codex");
        request.extras = serde_json::json!({ "system_prompt": "You are guided." });
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("builds");
        assert_eq!(
            command.args.last().map(String::as_str),
            Some("You are guided.\n\ndo the thing"),
            "system prompt prepended to the prompt argv"
        );

        let mut request = sample_request("codex");
        request.extras = serde_json::json!({
            "system_prompt": "You are guided.",
            "runtime_contract": { "cli": { "launch": { "command": "codex", "args": ["exec", "prompt"] } } }
        });
        let err = harness_command_for_request(Path::new("."), &request).expect_err("must fail closed");
        assert!(format!("{err}").contains("system prompt"), "error names the dropped input: {err}");
    }

    #[test]
    fn harness_command_applies_codex_reasoning_effort() {
        let mut request = sample_request("codex");
        request.extras = serde_json::json!({ "reasoning_effort": "High" });
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("builds");
        let pos = command
            .args
            .iter()
            .position(|arg| arg == "model_reasoning_effort=high")
            .expect("codex reasoning effort override applied");
        assert_eq!(command.args.get(pos - 1).map(String::as_str), Some("-c"), "rides a -c config override");
    }

    #[test]
    fn harness_command_fails_closed_when_kernel_approvals_are_requested() {
        let mut request = sample_request("claude");
        request.extras = serde_json::json!({ "approvals": true });
        let err: anyhow::Error = harness_command_for_request(Path::new("."), &request)
            .expect_err("approvals must not silently fail open inside an environment");
        assert!(format!("{err}").contains("approvals"), "error explains the approvals gate: {err}");
    }

    #[test]
    fn harness_command_errors_for_an_unknown_tool_without_a_contract() {
        let request = sample_request("definitely-not-a-tool");
        let err: anyhow::Error =
            harness_command_for_request(Path::new("."), &request).expect_err("unknown tool cannot build");
        assert!(format!("{err}").contains("definitely-not-a-tool"), "error names the tool: {err}");
    }

    // -----------------------------------------------------------------
    // Pipeline event sequencing (fake backend)
    // -----------------------------------------------------------------

    struct FakeBackend {
        prepare_result: Mutex<Option<Result<EnvironmentHandle>>>,
        exec_stream_outcome: Mutex<Option<StreamOutcome>>,
        exec_result: Mutex<Option<Result<ExecResponse>>>,
        exec_calls: AtomicUsize,
        torn_down: AtomicBool,
        last_command: Mutex<Option<HarnessCommand>>,
        last_stdin: Mutex<Option<String>>,
    }

    enum StreamOutcome {
        Deltas(Vec<(ExecStream, String)>, Result<ExecResponse>),
        Fail(anyhow::Error),
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                prepare_result: Mutex::new(Some(Ok(sample_handle()))),
                exec_stream_outcome: Mutex::new(None),
                exec_result: Mutex::new(None),
                exec_calls: AtomicUsize::new(0),
                torn_down: AtomicBool::new(false),
                last_command: Mutex::new(None),
                last_stdin: Mutex::new(None),
            }
        }
    }

    fn sample_handle() -> EnvironmentHandle {
        EnvironmentHandle { id: "env-1".to_string(), workspace_root: "/work".to_string(), metadata: Value::Null }
    }

    fn ok_response(exit_code: i32) -> ExecResponse {
        ExecResponse { exit_code: Some(exit_code), stdout: String::new(), stderr: String::new(), timed_out: false }
    }

    impl EnvironmentExecBackend for FakeBackend {
        fn prepare(&self, _spec: EnvironmentSpec) -> Result<EnvironmentHandle> {
            self.prepare_result.lock().unwrap().take().expect("prepare stubbed")
        }

        fn exec_stream(
            &self,
            _handle: &EnvironmentHandle,
            command: HarnessCommand,
            stdin: Option<String>,
            _timeout: Option<Duration>,
            on_output: &(dyn Fn(ExecStream, &str) + Send + Sync),
        ) -> Result<ExecResponse> {
            *self.last_command.lock().unwrap() = Some(command);
            *self.last_stdin.lock().unwrap() = stdin;
            match self.exec_stream_outcome.lock().unwrap().take().expect("exec_stream stubbed") {
                StreamOutcome::Deltas(deltas, result) => {
                    for (stream, text) in deltas {
                        on_output(stream, &text);
                    }
                    result
                }
                StreamOutcome::Fail(err) => Err(err),
            }
        }

        fn exec(
            &self,
            _handle: &EnvironmentHandle,
            _command: HarnessCommand,
            _stdin: Option<String>,
            _timeout: Option<Duration>,
        ) -> Result<ExecResponse> {
            self.exec_calls.fetch_add(1, Ordering::SeqCst);
            self.exec_result.lock().unwrap().take().expect("exec stubbed")
        }

        fn teardown(&self, _handle: &EnvironmentHandle) -> Result<()> {
            self.torn_down.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn drain(mut run: SessionRun) -> Vec<SessionEvent> {
        let mut events = Vec::new();
        while let Some(event) = run.events.recv().await {
            let finished =
                matches!(event, SessionEvent::Finished { .. } | SessionEvent::Error { recoverable: false, .. });
            events.push(event);
            if finished {
                break;
            }
        }
        events
    }

    fn spec_for_test() -> EnvironmentSpec {
        EnvironmentSpec {
            kind: "container".to_string(),
            repos: Vec::new(),
            image: None,
            resources: None,
            env: BTreeMap::new(),
            metadata: Value::Null,
        }
    }

    fn command_for_test() -> HarnessCommand {
        HarnessCommand {
            program: "claude".to_string(),
            args: vec!["--print".to_string()],
            env: BTreeMap::new(),
            cwd: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn env_routed_run_streams_deltas_through_the_session_channel_and_tears_down() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Deltas(
            vec![(ExecStream::Stdout, "out-1".to_string()), (ExecStream::Stderr, "err-1".to_string())],
            Ok(ok_response(7)),
        ));

        let run = spawn_environment_run(
            backend.clone(),
            spec_for_test(),
            command_for_test(),
            None,
            None,
            "environment:container".to_string(),
        );
        assert_eq!(run.selected_backend, "environment:container");
        let events = drain(run).await;

        assert!(
            matches!(&events[0], SessionEvent::Started { backend, .. } if backend == "environment:container"),
            "first frame is Started: {events:?}"
        );
        assert!(
            events.iter().any(|e| matches!(e, SessionEvent::TextDelta { text } if text == "out-1")),
            "stdout delta rides TextDelta (same channel as the local path): {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SessionEvent::Error { message, recoverable: true } if message == "err-1")),
            "stderr delta rides a recoverable Error frame: {events:?}"
        );
        assert!(
            matches!(events.last(), Some(SessionEvent::Finished { exit_code: Some(7) })),
            "terminal frame carries the exec exit code: {events:?}"
        );
        assert!(backend.torn_down.load(Ordering::SeqCst), "teardown runs after a successful exec");
        let command = backend.last_command.lock().unwrap().clone().expect("command routed to the backend");
        assert_eq!(command.program, "claude", "the HarnessCommand reaches EnvironmentExecBackend::exec_stream");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepare_failure_fails_the_run_without_executing_locally() {
        let backend = Arc::new(FakeBackend::new());
        *backend.prepare_result.lock().unwrap() = Some(Err(anyhow!("no docker daemon")));

        let run = spawn_environment_run(
            backend.clone(),
            spec_for_test(),
            command_for_test(),
            None,
            None,
            "environment:container".to_string(),
        );
        let events = drain(run).await;
        assert!(
            matches!(
                events.last(),
                Some(SessionEvent::Error { message, recoverable: false }) if message.contains("prepare failed")
            ),
            "prepare failure is a terminal error, never a silent local run: {events:?}"
        );
        assert_eq!(backend.exec_calls.load(Ordering::SeqCst), 0, "nothing executed after a failed prepare");
        assert!(!backend.torn_down.load(Ordering::SeqCst), "no handle -> no teardown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exec_failure_still_tears_down_and_fails_the_run() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Fail(anyhow!("plugin died")));

        let run = spawn_environment_run(
            backend.clone(),
            spec_for_test(),
            command_for_test(),
            None,
            None,
            "environment:container".to_string(),
        );
        let events = drain(run).await;
        assert!(
            matches!(
                events.last(),
                Some(SessionEvent::Error { message, recoverable: false }) if message.contains("exec failed")
            ),
            "exec failure is terminal: {events:?}"
        );
        assert!(backend.torn_down.load(Ordering::SeqCst), "teardown still runs after a failed exec");
        assert_eq!(backend.exec_calls.load(Ordering::SeqCst), 0, "a non-METHOD_NOT_SUPPORTED failure is not retried");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn method_not_supported_falls_back_to_buffered_exec() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() =
            Some(StreamOutcome::Fail(anyhow::Error::from(HostError::Rpc(RpcError {
                code: METHOD_NOT_SUPPORTED,
                message: "no exec_stream".to_string(),
                data: None,
            }))));
        *backend.exec_result.lock().unwrap() = Some(Ok(ExecResponse {
            exit_code: Some(0),
            stdout: "buffered-out".to_string(),
            stderr: "buffered-err".to_string(),
            timed_out: false,
        }));

        let run = spawn_environment_run(
            backend.clone(),
            spec_for_test(),
            command_for_test(),
            None,
            None,
            "environment:container".to_string(),
        );
        let events = drain(run).await;
        assert_eq!(backend.exec_calls.load(Ordering::SeqCst), 1, "buffered exec retried exactly once");
        assert!(
            events.iter().any(|e| matches!(e, SessionEvent::FinalText { text } if text == "buffered-out")),
            "aggregated stdout emitted once via FinalText: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SessionEvent::Error { message, recoverable: true } if message == "buffered-err")),
            "aggregated stderr emitted as a recoverable frame: {events:?}"
        );
        assert!(matches!(events.last(), Some(SessionEvent::Finished { exit_code: Some(0) })), "events: {events:?}");
        assert!(backend.torn_down.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_exit_code_is_a_terminal_error() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Deltas(
            Vec::new(),
            Ok(ExecResponse { exit_code: None, stdout: String::new(), stderr: String::new(), timed_out: false }),
        ));

        let run = spawn_environment_run(
            backend.clone(),
            spec_for_test(),
            command_for_test(),
            None,
            None,
            "environment:container".to_string(),
        );
        let events = drain(run).await;
        assert!(
            matches!(
                events.last(),
                Some(SessionEvent::Error { message, recoverable: false }) if message.contains("without an exit code")
            ),
            "a signal-killed command must not be reported as success: {events:?}"
        );
        assert!(backend.torn_down.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_exec_is_a_terminal_error() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Deltas(
            Vec::new(),
            Ok(ExecResponse { exit_code: None, stdout: String::new(), stderr: String::new(), timed_out: true }),
        ));

        let run = spawn_environment_run(
            backend.clone(),
            spec_for_test(),
            command_for_test(),
            None,
            Some(Duration::from_secs(5)),
            "environment:container".to_string(),
        );
        let events = drain(run).await;
        assert!(
            matches!(
                events.last(),
                Some(SessionEvent::Error { message, recoverable: false }) if message.contains("timed out")
            ),
            "timeout is terminal: {events:?}"
        );
        assert!(backend.torn_down.load(Ordering::SeqCst));
    }
}
