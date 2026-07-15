//! REQUIREMENT-048: route a workflow RUN's harness execution through a resolved
//! `environment` plugin instead of running it on the local host, preparing ONE
//! node per workflow run that is shared across every phase.
//!
//! This is the runner-side twin of the kernel's ad-hoc `animus agent run`
//! environment seam (`orchestrator-cli` `environment_exec.rs`, TASK-166 Phase
//! 2). When a workflow routes to a NON-LOCAL environment (a container / remote
//! sandbox plugin), [`workflow_execute`](crate::workflow_execute) resolves it
//! ONCE at the start of the run and prepares a single bare node
//! ([`PreparedEnvironment::prepare`]). Each phase's harness command is then
//! executed INSIDE that held node via [`PreparedEnvironment::exec_session`]
//! (`exec_stream` on the SAME pinned [`EnvironmentClient`] + [`EnvironmentHandle`]),
//! with the streamed stdout/stderr surfaced through the SAME [`SessionEvent`]
//! channel a local [`SessionRun`] yields — so the runner's existing
//! `process_phase_event_stream` consumer drives it UNCHANGED. The node is torn
//! down ONCE when the run finishes (success, failure, or early exit).
//!
//! Because the [`EnvironmentClient`] pins one resident host for its lifetime,
//! holding it across the run keeps ONE node + ONE plugin process for every phase
//! — so a clone performed in one phase is visible to the next.
//!
//! ## Gate (default = byte-for-byte the current local path)
//!
//! [`resolve_workflow_environment`] / [`resolve_exec_environment`] decide whether
//! a run is env-routed:
//!
//! - `ANIMUS_ENVIRONMENT_EXEC` unset → consult the compiled config via
//!   [`orchestrator_config::workflow_config::resolve_environment`], honoring the
//!   workflow-level `environment:` override plus the subject kind against the
//!   `environment_routing:` table.
//! - `ANIMUS_ENVIRONMENT_EXEC` set to a falsy token (`0` / `false` / `no` /
//!   `off` / empty) → hard kill-switch: local execution even when config routes.
//! - `ANIMUS_ENVIRONMENT_EXEC` set to any other value → that value is the
//!   environment plugin id (dev/test override).
//!
//! A resolution of `None` or a LOCAL environment id (`local` / `worktree`)
//! leaves the existing local path untouched.
//!
//! ## Bare node, no auto-clone
//!
//! The prepared node is EMPTY: the [`EnvironmentSpec`] carries no `repos` (the
//! workflow clones what it needs via a command phase that renders the subject's
//! `git_repo` custom field, e.g. `git clone {{git_repo}} .`). Only a matched
//! routing rule's opaque `spec` overrides (`image` / `resources` / `env` /
//! metadata) ride the prepared spec.
//!
//! ## Failure posture
//!
//! Once a non-local environment IS resolved, the run never silently falls back
//! to local execution — a missing plugin, a failed `prepare`, or a failed exec
//! fails with an actionable error. The single transparent fallback is
//! protocol-level: a plugin that does not implement `environment/exec_stream`
//! (METHOD_NOT_SUPPORTED) is retried with the buffered `environment/exec`, which
//! is safe because METHOD_NOT_SUPPORTED means the command never started.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use animus_environment_protocol::{EnvironmentHandle, EnvironmentSpec, ExecResponse, ExecStream, HarnessCommand};
use animus_plugin_protocol::error_codes::METHOD_NOT_SUPPORTED;
use animus_session_backend::session::{SessionEvent, SessionRequest, SessionRun};
use anyhow::{anyhow, Context, Result};
use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{GenericFilePath, Stream as LocalSocketStream};
use orchestrator_config::workflow_config::resolve_environment;
use orchestrator_core::EnvironmentClient;
use orchestrator_plugin_host::HostError;
use serde::{Deserialize, Serialize};
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

/// A resolved non-local environment for a workflow run: the plugin id plus any
/// opaque `spec` overrides the matched routing rule carried. The node is
/// prepared BARE (no repos) — the workflow clones what it needs via a command
/// phase (see the module docs).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedEnvironment {
    pub(crate) id: String,
    pub(crate) spec_overrides: Option<BTreeMap<String, Value>>,
}

/// The routing context threaded into [`resolve_exec_environment`]: the subject
/// kind, plus the phase- and workflow-level `environment:` overrides. All `None`
/// reproduces routing-table-only resolution. Workflow-run-level resolution sets
/// only `subject_kind` + `workflow_env` (see [`resolve_workflow_environment`]).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PhaseRouting<'a> {
    pub(crate) subject_kind: Option<&'a str>,
    pub(crate) phase_env: Option<&'a str>,
    pub(crate) workflow_env: Option<&'a str>,
}

/// Resolve the NON-LOCAL environment (if any) that should execute a run's
/// harness. Returns `None` for the default local path — see the module docs for
/// the full gate semantics. A non-local id routes a BARE node regardless of
/// repos (the workflow clones via a command).
pub(crate) fn resolve_exec_environment(
    project_root: &Path,
    harness: &str,
    routing_ctx: PhaseRouting,
) -> Option<ResolvedEnvironment> {
    let resolved = match std::env::var("ANIMUS_ENVIRONMENT_EXEC") {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" | "0" | "false" | "no" | "off" => return None,
            _ => ResolvedEnvironment { id: raw.trim().to_string(), spec_overrides: None },
        },
        Err(_) => {
            let config = orchestrator_core::load_workflow_config_or_default(project_root).config;
            let routing = config.environment_routing.as_ref();
            // Precedence (see `resolve_environment`): phase `environment:` >
            // first matching routing rule > workflow `environment:` > routing
            // default. So a WorkflowDefinition `environment:` routes the whole
            // run, and a phase-level `environment:` wins for that phase.
            let id = resolve_environment(
                routing_ctx.subject_kind,
                Some(harness),
                routing_ctx.phase_env,
                routing_ctx.workflow_env,
                routing,
            )?;
            // Re-find the routing rule that SELECTED this id so its opaque `spec`
            // overrides (image / resources / env / metadata) ride the prepared
            // EnvironmentSpec. A phase-level `environment:` override wins BEFORE
            // rules are consulted, so when `phase_env` is set the id did NOT come
            // from a rule — a coincidental same-id rule must not lend its spec.
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
            ResolvedEnvironment { id, spec_overrides: rule_spec }
        }
    };
    if is_local_environment(&resolved.id) {
        return None;
    }
    Some(resolved)
}

/// Resolve the workflow-run-level environment: the WorkflowDefinition's
/// `environment:` (if any) plus the subject kind, honoring the
/// `ANIMUS_ENVIRONMENT_EXEC` override and the `environment_routing:` table. A
/// non-local resolution means the run prepares ONE shared node; `None` keeps the
/// local per-phase path. Harness is left empty here — a workflow-level
/// `environment:` override and kind-only routing rules do not depend on it.
pub(crate) fn resolve_workflow_environment(
    project_root: &Path,
    workflow_ref: &str,
    subject_kind: Option<&str>,
) -> Option<ResolvedEnvironment> {
    // The `ANIMUS_ENVIRONMENT_EXEC` override short-circuits without needing the
    // workflow's `environment:`, so only look it up for the config path.
    let workflow_env = if std::env::var_os("ANIMUS_ENVIRONMENT_EXEC").is_some() {
        None
    } else {
        let config = orchestrator_core::load_workflow_config_or_default(project_root).config;
        crate::phase_executor::phase_environment_overrides(&config, workflow_ref, "").1
    };
    resolve_exec_environment(
        project_root,
        "",
        PhaseRouting { subject_kind, phase_env: None, workflow_env: workflow_env.as_deref() },
    )
}

/// What a phase needs from the per-run environment node, abstracted over WHO
/// owns the node. Two implementations exist:
///
/// - [`PreparedEnvironment`] — the RUNNER owns the node (standalone CLI / the
///   non-brokered daemon path): it prepares ONE node at the start of the run,
///   holds it across every phase, and tears it down at the end.
/// - [`BrokeredEnvironment`] — the DAEMON owns ONE node per workflow RUN and
///   exposes it over a private local socket; the runner only ACQUIREs a handle
///   and EXECs through it (no prepare, no teardown — see REQUIREMENT-048).
///
/// Both yield a [`SessionRun`] whose event stream mirrors the local provider
/// path, so `phase_executor`'s `process_phase_event_stream` consumer is
/// identical for either owner.
pub trait HeldEnvironment: Send + Sync {
    /// The resolved environment plugin id (for logging / diagnostics).
    fn id(&self) -> &str;
    /// Execute a phase's harness command INSIDE the held node, returning a
    /// [`SessionRun`] whose event stream mirrors the local provider path.
    fn exec_session(&self, project_root: &Path, request: &SessionRequest) -> Result<SessionRun>;
    /// Execute a RAW command (not a harness/provider launch) INSIDE the held
    /// node, BUFFERED — used by env-routed COMMAND phases so a `git clone` /
    /// `git commit` / `gh pr create` runs in the SAME shared workspace the run's
    /// agent phases edit (REQUIREMENT-048). `cwd` is the command's resolved
    /// (host) working dir; the impl maps it into the node's `workspace_root`
    /// (see [`map_command_cwd`]). `program`/`args`/`env` are already templated.
    fn exec_command(
        &self,
        project_root: &Path,
        program: &str,
        args: &[String],
        env: &std::collections::BTreeMap<String, String>,
        cwd: Option<&str>,
        stdin: Option<String>,
        timeout: Option<std::time::Duration>,
    ) -> Result<EnvCommandOutput>;
}

/// The buffered result of a RAW command exec inside a held node (REQUIREMENT-048
/// command-phase routing). Mirrors the fields of an environment [`ExecResponse`]
/// the command-phase result is built from, with `exit_code` normalized to `-1`
/// when the process produced none (signal / OOM kill) so it slots into the same
/// [`crate::phase_command::CommandExecutionResult`] the local path yields.
// `pub` (not `pub(crate)`) so it does not leak a private type through the `pub`
// `HeldEnvironment::exec_command` return position, matching the sibling `pub`
// env types (`PreparedEnvironment` / `BrokeredEnvironment`).
pub struct EnvCommandOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

impl From<ExecResponse> for EnvCommandOutput {
    fn from(response: ExecResponse) -> Self {
        Self {
            exit_code: response.exit_code.unwrap_or(-1),
            stdout: response.stdout,
            stderr: response.stderr,
            timed_out: response.timed_out,
        }
    }
}

/// Map a command phase's resolved (host) cwd into the environment. A
/// [`HarnessCommand::cwd`] is relative to the node's `workspace_root`:
/// - the daemon `project_root` (the common `ProjectRoot`-mode cwd) maps to
///   `None`, so the command runs in the node's workspace root — the SAME shared
///   workspace the run's agent phases edit;
/// - a cwd BELOW the project root maps to that relative subpath;
/// - an absolute cwd OUTSIDE the project root (e.g. `/tmp/animus-work/...`) is
///   passed through verbatim (absolute paths dominate).
fn map_command_cwd(project_root: &Path, cwd: Option<&str>) -> Option<String> {
    let cwd = cwd.map(str::trim).filter(|value| !value.is_empty())?;
    let root = project_root.canonicalize().unwrap_or_else(|_| project_root.to_path_buf());
    let cwd_path = Path::new(cwd);
    let canonical = cwd_path.canonicalize().unwrap_or_else(|_| cwd_path.to_path_buf());
    match canonical.strip_prefix(&root) {
        Ok(relative) if relative.as_os_str().is_empty() => None,
        Ok(relative) => Some(relative.to_string_lossy().to_string()),
        // Absolute path outside the project root: dominates — pass it through.
        Err(_) => Some(cwd.to_string()),
    }
}

/// A prepared, per-workflow-run environment: ONE resident host + ONE plugin
/// process, shared across every phase. Prepared once at the start of the run and
/// torn down once at the end (guaranteed via a [`Drop`] guard in
/// [`workflow_execute`](crate::workflow_execute)).
pub struct PreparedEnvironment {
    backend: Arc<dyn EnvironmentExecBackend>,
    handle: EnvironmentHandle,
    id: String,
    backend_label: String,
    torn_down: AtomicBool,
    /// A DEDICATED tokio runtime that owns the resident host's stdio I/O driver
    /// for the whole environment lifetime. The lease's driver is spawned (via
    /// `tokio::spawn`) onto whatever runtime is current when the host is first
    /// leased; if that were a per-call throwaway runtime it would be dropped the
    /// instant `prepare` returned, killing the driver and orphaning the node so
    /// the next `exec` fails with "plugin connection lost". Holding this runtime
    /// for the struct's lifetime keeps the driver alive across prepare → exec →
    /// teardown. `None` only for test-injected backends (no real host). Shut down
    /// via `shutdown_background()` in `Drop` (safe from any context).
    runtime: Option<tokio::runtime::Runtime>,
}

impl PreparedEnvironment {
    /// Resolve the environment plugin and prepare a BARE node for the whole run.
    /// Blocking (the [`EnvironmentClient`] surface is blocking) — call via
    /// [`Self::prepare_off_runtime`] from an async context.
    pub(crate) fn prepare(
        project_root: &Path,
        environment: &ResolvedEnvironment,
        github_repo: Option<&str>,
    ) -> Result<Self> {
        let environment_id = environment.id.as_str();
        let client = EnvironmentClient::resolve(project_root, environment_id).map_err(|err| {
            anyhow!(
                "workflow run is routed to environment '{environment_id}' but no usable environment plugin was \
                 resolved (the run is NOT executed locally when an environment is requested): {err}"
            )
        })?;
        let backend_label = format!("environment:{}", client.plugin_name());
        Self::prepare_with_backend(Arc::new(client), backend_label, environment, github_repo)
    }

    /// Prepare against an already-resolved backend (production wraps an
    /// [`EnvironmentClient`]; tests inject a fake). Builds a BARE
    /// [`EnvironmentSpec`] (no repos) and applies any routing-rule `spec`
    /// overrides.
    fn prepare_with_backend(
        backend: Arc<dyn EnvironmentExecBackend>,
        backend_label: String,
        environment: &ResolvedEnvironment,
        github_repo: Option<&str>,
    ) -> Result<Self> {
        let mut spec = EnvironmentSpec {
            kind: environment.id.clone(),
            repos: Vec::new(),
            image: None,
            resources: None,
            env: BTreeMap::new(),
            metadata: Value::Null,
        };
        if let Some(overrides) = environment.spec_overrides.clone() {
            apply_spec_overrides(&mut spec, overrides);
        }
        // Repo-scope the node's GitHub App token: merged AFTER the routing-rule
        // overrides so it lands on (and preserves) any metadata they set.
        merge_github_repo_metadata(&mut spec, github_repo);
        let handle =
            backend.prepare(spec).map_err(|err| anyhow!("environment prepare failed for {backend_label}: {err:#}"))?;
        Ok(Self {
            backend,
            handle,
            id: environment.id.clone(),
            backend_label,
            torn_down: AtomicBool::new(false),
            runtime: None,
        })
    }

    /// Prepare the node on a DEDICATED, long-lived runtime and hand that runtime
    /// back inside the [`PreparedEnvironment`] so the resident host's stdio I/O
    /// driver — spawned during lease acquisition — outlives `prepare` and stays
    /// reachable for every later `exec`/`teardown` RPC (which drive the host's
    /// channels from their own throwaway runtimes; cross-runtime channel comms
    /// are fine as long as the driver's runtime is alive).
    ///
    /// The runtime is built and `block_on` is entered from a bare OS thread (a
    /// runtime cannot be created from within another runtime's worker), then the
    /// idle runtime — whose own worker threads keep the driver parked and live —
    /// is moved back to the async caller and stored on the struct.
    pub(crate) async fn prepare_off_runtime(
        project_root: &Path,
        environment: &ResolvedEnvironment,
        github_repo: Option<String>,
    ) -> Result<Self> {
        let project_root = project_root.to_path_buf();
        let environment = environment.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let result = (|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .context("building dedicated runtime for the environment host")?;
                // Lease + prepare INSIDE this runtime so the host's I/O driver
                // binds here (not to a per-call throwaway that would die on return).
                let mut prepared =
                    runtime.block_on(async { Self::prepare(&project_root, &environment, github_repo.as_deref()) })?;
                prepared.runtime = Some(runtime);
                Ok::<_, anyhow::Error>(prepared)
            })();
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| anyhow!("environment prepare thread terminated unexpectedly"))?
    }

    /// The resolved environment plugin id.
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// Execute a phase's harness command INSIDE the held node — `exec_stream` on
    /// the SAME pinned client + handle, with NO prepare and NO teardown (those
    /// bracket the whole run). Returns a [`SessionRun`] whose event stream
    /// mirrors the local provider path, so the caller's consumer is unchanged.
    pub(crate) fn exec_session(&self, project_root: &Path, request: &SessionRequest) -> Result<SessionRun> {
        let (command, stdin) = harness_command_for_request(project_root, request)?;
        // Mirror the local path's default run cap: an un-timed env exec would
        // otherwise be unbounded, so a hung provider command inside the
        // environment would drain forever.
        let timeout = Some(Duration::from_secs(request.timeout_secs.unwrap_or(DEFAULT_ENVIRONMENT_RUN_TIMEOUT_SECS)));
        Ok(spawn_environment_exec(
            self.backend.clone(),
            self.handle.clone(),
            command,
            stdin,
            timeout,
            self.backend_label.clone(),
        ))
    }

    /// Teardown the node ONCE (idempotent across the explicit end-of-run call and
    /// the [`Drop`] backstop). Runs on a dedicated OS thread and joins it so the
    /// blocking teardown RPC does not require the async runtime and cannot panic
    /// from a nested runtime.
    pub(crate) fn teardown(&self) {
        if self.torn_down.swap(true, Ordering::SeqCst) {
            return;
        }
        let backend = self.backend.clone();
        let handle = self.handle.clone();
        let backend_label = self.backend_label.clone();
        let _ = std::thread::spawn(move || {
            if let Err(err) = backend.teardown(&handle) {
                eprintln!("warning: environment teardown failed for {backend_label} (handle {}): {err:#}", handle.id);
            }
        })
        .join();
    }
}

impl Drop for PreparedEnvironment {
    /// Shut the dedicated host runtime down WITHOUT blocking. `teardown()` (the
    /// RPC that deletes the node) has already run via the end-of-run call or the
    /// [`PreparedEnvironmentGuard`](crate::workflow_execute) backstop, and it
    /// needs this runtime's I/O driver alive — so tearing the node down is NOT
    /// done here. `shutdown_background` is used instead of an implicit drop
    /// because dropping a runtime inline panics when the surrounding thread is
    /// itself inside an async runtime (this `Drop` can fire on any thread).
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl HeldEnvironment for PreparedEnvironment {
    fn id(&self) -> &str {
        // Inherent `id` wins method resolution, so this delegates (no recursion).
        PreparedEnvironment::id(self)
    }

    fn exec_session(&self, project_root: &Path, request: &SessionRequest) -> Result<SessionRun> {
        PreparedEnvironment::exec_session(self, project_root, request)
    }

    fn exec_command(
        &self,
        project_root: &Path,
        program: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
        cwd: Option<&str>,
        stdin: Option<String>,
        timeout: Option<Duration>,
    ) -> Result<EnvCommandOutput> {
        let command = HarnessCommand {
            program: program.to_string(),
            args: args.to_vec(),
            env: env.clone(),
            cwd: map_command_cwd(project_root, cwd),
        };
        // Mirror the harness path's default run cap so an un-timed command exec
        // is bounded (a hung `git`/`gh` inside the node would otherwise drain
        // forever).
        let timeout = timeout.or(Some(Duration::from_secs(DEFAULT_ENVIRONMENT_RUN_TIMEOUT_SECS)));
        // Run the blocking BUFFERED exec on a dedicated OS thread (like
        // `teardown`) so it uses the client's own throwaway runtime and never
        // nests on the async caller's runtime; the resident host's I/O driver
        // stays live on the struct's dedicated runtime for the whole run.
        let backend = self.backend.clone();
        let handle = self.handle.clone();
        let backend_label = self.backend_label.clone();
        let response = std::thread::spawn(move || backend.exec(&handle, command, stdin, timeout))
            .join()
            .map_err(|_| anyhow!("environment exec_command thread panicked"))?
            .map_err(|err| anyhow!("environment exec_command failed in {backend_label}: {err:#}"))?;
        Ok(response.into())
    }
}

/// Merge the run's target repo into the [`EnvironmentSpec`]'s `metadata` as
/// `github_repo`, so the environment plugin resolves the GitHub App INSTALLATION
/// for THAT repo (`GET /repos/{owner}/{repo}/installation`) and scopes the minted
/// installation token to it — instead of falling back to the first of possibly
/// several org installations (which mints a token for the WRONG org and 403s on
/// push). The repo is the subject's `git_repo` custom field (the same value the
/// harness renders as `{{git_repo}}`). Any existing `metadata` object is
/// PRESERVED (`github_repo` is merged in, not clobbered); a `None`/empty repo
/// leaves `metadata` untouched (a bare non-coding run must not invent a repo).
fn merge_github_repo_metadata(spec: &mut EnvironmentSpec, github_repo: Option<&str>) {
    let Some(repo) = github_repo.map(str::trim).filter(|repo| !repo.is_empty()) else {
        return;
    };
    if !matches!(spec.metadata, Value::Object(_)) {
        spec.metadata = Value::Object(serde_json::Map::new());
    }
    if let Value::Object(map) = &mut spec.metadata {
        map.insert("github_repo".to_string(), Value::String(repo.to_string()));
    }
}

/// Merge a routing rule's opaque `spec` overrides into the compiled
/// [`EnvironmentSpec`]: the wire-typed keys (`image`, `resources`, `env`) land
/// on their typed fields; everything else is carried opaquely on `metadata`. The
/// node is prepared BARE, so a rule's `repos` key is deliberately IGNORED (the
/// workflow clones what it needs via a command phase) and never leaks onto
/// `metadata`.
fn apply_spec_overrides(spec: &mut EnvironmentSpec, overrides: BTreeMap<String, Value>) {
    let mut metadata = serde_json::Map::new();
    for (key, value) in overrides {
        match key.as_str() {
            // Ignored: the per-run node is bare (no auto-clone).
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
    // Launch a WRITE-CAPABLE phase in the tool's edit-permitting mode when no
    // explicit `permission_mode` is set — driven by the phase CAPABILITY (which
    // rides `extras.phase_capabilities`), not by `tool == "claude"` alone. Today
    // the local path injects the same default via
    // `runtime_contract::apply_phase_capability_launch_flags`; the env-exec path
    // did not, so a write-capable claude phase launched in its interactive "ask"
    // mode where `-p` runs cannot edit files and the agent degrades to only
    // DESCRIBING the change. An ephemeral per-run node is an isolated sandbox, so
    // bypass is the correct default for a write-capable claude phase; codex
    // launches edit-capable by default so it needs no flag; an explicit
    // `permission_mode` (e.g. `plan`) still wins; a READ-ONLY phase is NOT forced
    // into an edit-permitting mode.
    let writes_files = request
        .extras
        .pointer("/phase_capabilities")
        .and_then(|caps| serde_json::from_value::<protocol::PhaseCapabilities>(caps.clone()).ok())
        .map(|caps| caps.writes_files)
        .unwrap_or(false);
    let effective_permission_mode = request
        .permission_mode
        .as_deref()
        .map(str::trim)
        .filter(|mode| !mode.is_empty())
        .or(if writes_files && is_claude { Some("bypassPermissions") } else { None });
    let args = apply_permission_mode(&request.tool, args, effective_permission_mode);
    let args = apply_reasoning_effort(
        &request.tool,
        args,
        request.extras.pointer("/reasoning_effort").and_then(Value::as_str),
    );
    let args = apply_codex_node_sandbox(&request.tool, args);

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

/// Codex sandboxes its own shell with bubblewrap, which CANNOT create namespaces
/// inside a nested per-run node container ("bwrap: Creating new namespace failed:
/// Permission denied") — so codex can't exec ANY command (even a read-only
/// `git diff`) in the node, and every codex node phase fails to inspect the
/// workspace. The node is ALREADY an isolated sandbox, so for the env-exec path
/// force codex's `sandbox_mode` to full access and `approval_policy` to never so
/// it can run commands — the codex analogue of claude's `bypassPermissions`
/// default. `ensure_codex_config_override` upserts, so this wins over any earlier
/// `approval_policy` set from a permission mode.
fn apply_codex_node_sandbox(tool: &str, args: Vec<String>) -> Vec<String> {
    if !tool.trim().eq_ignore_ascii_case("codex") {
        return args;
    }
    let mut contract = serde_json::json!({ "cli": { "launch": { "args": args } } });
    animus_runtime_shared::inject_codex_config_overrides_list(
        &mut contract,
        "codex",
        &["sandbox_mode=\"danger-full-access\"".to_string(), "approval_policy=\"never\"".to_string()],
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

/// Spawn the per-phase exec pipeline on a dedicated OS thread (the
/// [`EnvironmentClient`] surface is blocking) against an ALREADY-PREPARED handle,
/// returning a [`SessionRun`] whose event stream mirrors the local provider
/// path. NO prepare and NO teardown happen here — those bracket the whole
/// workflow run in [`PreparedEnvironment`]. Must be called from within a tokio
/// runtime (the forwarder task bridges the pipeline's unbounded sends onto the
/// bounded `SessionRun` channel).
pub(crate) fn spawn_environment_exec(
    backend: Arc<dyn EnvironmentExecBackend>,
    handle: EnvironmentHandle,
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
        run_environment_exec_pipeline(backend.as_ref(), &handle, command, stdin, timeout, &backend_label, &pipeline_tx);
    });

    SessionRun { session_id: None, events: events_rx, selected_backend, fallback_reason: None, pid: None }
}

/// The blocking exec_stream pipeline against a prepared `handle`. Emits the same
/// event grammar the provider path does: `Started`, stdout deltas as
/// `TextDelta`, stderr deltas as recoverable `Error` frames, then a terminal
/// `Finished` (or an unrecoverable `Error`). Teardown is NOT performed here (the
/// node is shared across phases and torn down once per run).
fn run_environment_exec_pipeline(
    backend: &dyn EnvironmentExecBackend,
    handle: &EnvironmentHandle,
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

    let stream_events = events.clone();
    let on_output = move |stream: ExecStream, text: &str| {
        let event = match stream {
            ExecStream::Stdout => SessionEvent::TextDelta { text: text.to_string() },
            ExecStream::Stderr => SessionEvent::Error { message: text.to_string(), recoverable: true },
        };
        let _ = stream_events.send(event);
    };

    let result = match backend.exec_stream(handle, command.clone(), stdin.clone(), timeout, &on_output) {
        // A plugin without exec_stream support answers METHOD_NOT_SUPPORTED
        // before the command ever starts, so retrying with the buffered exec is
        // safe (no double-execution risk). The aggregated output is emitted
        // once, post-hoc — deltas were never streamed.
        Err(err) if is_method_not_supported(&err) => {
            backend.exec(handle, command, stdin, timeout).inspect(|response| {
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

// ---------------------------------------------------------------------------
// REQUIREMENT-048 cross-phase broker (runner client)
// ---------------------------------------------------------------------------
//
// The daemon dispatches ONE runner subprocess per phase, so a runner-owned
// `PreparedEnvironment` cannot share a workspace between phases. The daemon
// broker owns ONE node per workflow RUN and exposes it over a private local
// socket. When all four broker env vars are set the runner ACQUIREs a handle to
// that node and EXECs each phase through it — no prepare, no teardown (the
// daemon owns the node lifecycle). This is a PRIVATE daemon<->runner IPC (NOT
// animus-protocol); the frame structs below are defined independently here per
// the wire contract.

/// Local socket path the runner dials to reach the daemon's per-run broker.
pub(crate) const BROKER_SOCKET_ENV: &str = "ANIMUS_ENVIRONMENT_BROKER_SOCKET";
/// Per-daemon bearer capability echoed on every broker frame.
pub(crate) const BROKER_TOKEN_ENV: &str = "ANIMUS_ENVIRONMENT_BROKER_TOKEN";
/// The workflow run id this dispatch belongs to (the broker's single-flight key).
pub(crate) const BROKER_RUN_ID_ENV: &str = "ANIMUS_ENVIRONMENT_BROKER_RUN_ID";
/// The resolved environment plugin id (e.g. `animus-environment-railway`).
pub(crate) const BROKER_ENVIRONMENT_ID_ENV: &str = "ANIMUS_ENVIRONMENT_BROKER_ENVIRONMENT_ID";

/// The four broker connection parameters, read together from the environment.
/// `None` from [`broker_env`] means "not brokered" — the runner keeps its owned
/// [`PreparedEnvironment`] path (standalone CLI / non-daemon).
#[derive(Debug, Clone)]
struct BrokerEnv {
    socket_path: PathBuf,
    token: String,
    run_id: String,
    environment_id: String,
}

/// Read the four broker env vars. Returns `None` unless ALL are present and
/// non-empty (partial presence is treated as absent — the owned path).
fn broker_env() -> Option<BrokerEnv> {
    let read =
        |key: &str| std::env::var(key).ok().map(|value| value.trim().to_string()).filter(|value| !value.is_empty());
    Some(BrokerEnv {
        socket_path: PathBuf::from(read(BROKER_SOCKET_ENV)?),
        token: read(BROKER_TOKEN_ENV)?,
        run_id: read(BROKER_RUN_ID_ENV)?,
        environment_id: read(BROKER_ENVIRONMENT_ID_ENV)?,
    })
}

/// Acquire frame (runner -> daemon): one request, one terminal response.
#[derive(Serialize)]
struct AcquireRequest<'a> {
    op: &'a str,
    token: &'a str,
    run_id: &'a str,
    environment_id: &'a str,
    spec: EnvironmentSpec,
}

#[derive(Deserialize)]
struct AcquireResponse {
    ok: bool,
    #[serde(default)]
    workspace_root: Option<String>,
    #[serde(default)]
    handle_id: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

/// Exec frame (runner -> daemon): one request, then a stream of `{"out",...}`
/// lines terminated by exactly one `{"done":ExecResponse}` or `{"error"}`.
#[derive(Serialize)]
struct ExecRequest<'a> {
    op: &'a str,
    token: &'a str,
    run_id: &'a str,
    handle_id: &'a str,
    command: HarnessCommand,
    stdin: Option<String>,
    timeout_secs: Option<u64>,
}

/// The connection coordinates the exec pipeline dials with — cloned per phase so
/// each exec opens its own short-lived connection (one connection per RPC).
#[derive(Clone)]
struct BrokerExecTarget {
    socket_path: PathBuf,
    token: String,
    run_id: String,
    handle_id: String,
}

/// A per-workflow-run environment node OWNED BY THE DAEMON, reached over the
/// broker socket. Constructed by [`BrokeredEnvironment::acquire_from_env`],
/// which dials the socket and sends the `acquire` frame to obtain the shared
/// `workspace_root` + `handle_id`. It NEVER prepares or tears down a node.
#[derive(Debug)]
pub struct BrokeredEnvironment {
    socket_path: PathBuf,
    token: String,
    run_id: String,
    environment_id: String,
    #[allow(dead_code)]
    workspace_root: String,
    handle_id: String,
    backend_label: String,
}

impl BrokeredEnvironment {
    /// If the four broker env vars are present, dial the broker and ACQUIRE the
    /// run's shared node (single-flighted by the daemon on `run_id`). Returns:
    /// - `None` — not brokered (env vars absent): the caller keeps the owned path.
    /// - `Some(Ok(env))` — a brokered handle to the shared node.
    /// - `Some(Err(_))` — brokered mode was expected but acquire failed: the
    ///   caller MUST fail the phase (never fall back to an owned prepare).
    ///
    /// The blocking socket dial runs on a blocking thread so it never stalls the
    /// async runtime's worker.
    pub(crate) async fn acquire_from_env(github_repo: Option<String>) -> Option<Result<Self>> {
        let env = broker_env()?;
        let joined = tokio::task::spawn_blocking(move || Self::acquire(env, github_repo.as_deref())).await;
        Some(joined.map_err(|err| anyhow!("environment broker acquire task panicked: {err}")).and_then(|res| res))
    }

    /// Dial the broker socket and perform the `acquire` handshake.
    fn acquire(env: BrokerEnv, github_repo: Option<&str>) -> Result<Self> {
        // Bare spec (no repos), carrying `metadata.animus_run_id` so the plugin
        // names the node deterministically per run — plus `github_repo` (when the
        // subject carries one) so the plugin repo-scopes the minted GitHub App
        // installation token to the run's target repo.
        let mut spec = EnvironmentSpec {
            kind: env.environment_id.clone(),
            repos: Vec::new(),
            image: None,
            resources: None,
            env: BTreeMap::new(),
            metadata: serde_json::json!({ "animus_run_id": env.run_id }),
        };
        merge_github_repo_metadata(&mut spec, github_repo);
        let request = AcquireRequest {
            op: "acquire",
            token: &env.token,
            run_id: &env.run_id,
            environment_id: &env.environment_id,
            spec,
        };
        let line = serde_json::to_string(&request).context("serializing environment broker acquire frame")?;

        let mut stream = dial(&env.socket_path)
            .with_context(|| format!("dialing environment broker socket at {}", env.socket_path.display()))?;
        write_frame(&mut stream, &line).context("sending environment broker acquire frame")?;

        let mut reader = BufReader::new(stream);
        let mut response_line = String::new();
        if reader.read_line(&mut response_line).context("reading environment broker acquire response")? == 0 {
            return Err(anyhow!(
                "environment broker at {} closed the connection before answering acquire",
                env.socket_path.display()
            ));
        }
        let response: AcquireResponse = serde_json::from_str(response_line.trim())
            .with_context(|| format!("parsing environment broker acquire response: {}", response_line.trim()))?;
        if !response.ok {
            return Err(anyhow!(
                "environment broker refused to acquire run '{}' for environment '{}': {}",
                env.run_id,
                env.environment_id,
                response.error.unwrap_or_else(|| "no reason given".to_string())
            ));
        }
        let workspace_root = response
            .workspace_root
            .ok_or_else(|| anyhow!("environment broker acquire succeeded but returned no workspace_root"))?;
        let handle_id = response
            .handle_id
            .ok_or_else(|| anyhow!("environment broker acquire succeeded but returned no handle_id"))?;

        let backend_label = format!("environment-broker:{}", env.environment_id);
        Ok(Self {
            socket_path: env.socket_path,
            token: env.token,
            run_id: env.run_id,
            environment_id: env.environment_id,
            workspace_root,
            handle_id,
            backend_label,
        })
    }
}

impl HeldEnvironment for BrokeredEnvironment {
    fn id(&self) -> &str {
        &self.environment_id
    }

    fn exec_session(&self, project_root: &Path, request: &SessionRequest) -> Result<SessionRun> {
        let (command, stdin) = harness_command_for_request(project_root, request)?;
        // Mirror the owned path's default run cap so an un-timed brokered exec is
        // not unbounded.
        let timeout_secs = request.timeout_secs.unwrap_or(DEFAULT_ENVIRONMENT_RUN_TIMEOUT_SECS);
        Ok(spawn_brokered_exec(
            BrokerExecTarget {
                socket_path: self.socket_path.clone(),
                token: self.token.clone(),
                run_id: self.run_id.clone(),
                handle_id: self.handle_id.clone(),
            },
            command,
            stdin,
            timeout_secs,
            self.backend_label.clone(),
        ))
    }

    fn exec_command(
        &self,
        project_root: &Path,
        program: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
        cwd: Option<&str>,
        stdin: Option<String>,
        timeout: Option<Duration>,
    ) -> Result<EnvCommandOutput> {
        let command = HarnessCommand {
            program: program.to_string(),
            args: args.to_vec(),
            env: env.clone(),
            cwd: map_command_cwd(project_root, cwd),
        };
        let timeout_secs = timeout.map(|t| t.as_secs()).unwrap_or(DEFAULT_ENVIRONMENT_RUN_TIMEOUT_SECS);
        let target = BrokerExecTarget {
            socket_path: self.socket_path.clone(),
            token: self.token.clone(),
            run_id: self.run_id.clone(),
            handle_id: self.handle_id.clone(),
        };
        // Buffered variant of the streaming broker exec: reuse the SAME socket
        // dial + framing helper ([`brokered_exec_stream`]), but COLLECT the
        // streamed `{"out",...}` frames into stdout/stderr strings instead of
        // forwarding them as `SessionEvent`s. Interior mutability because the
        // helper's callback is `Fn` (shared), not `FnMut`.
        let stdout = std::sync::Mutex::new(String::new());
        let stderr = std::sync::Mutex::new(String::new());
        let on_output = |stream: BrokerStream, text: &str| match stream {
            BrokerStream::Stdout => stdout.lock().expect("brokered stdout mutex").push_str(text),
            BrokerStream::Stderr => stderr.lock().expect("brokered stderr mutex").push_str(text),
        };
        let response = brokered_exec_stream(&target, command, stdin, timeout_secs, &on_output)
            .map_err(|err| anyhow!("environment exec_command failed in {}: {err:#}", self.backend_label))?;
        // `on_output`'s borrows of stdout/stderr end here (its last use was the
        // `&on_output` above), so the mutexes are free to unwrap.
        Ok(EnvCommandOutput {
            exit_code: response.exit_code.unwrap_or(-1),
            stdout: stdout.into_inner().expect("brokered stdout mutex"),
            stderr: stderr.into_inner().expect("brokered stderr mutex"),
            timed_out: response.timed_out,
        })
    }
}

/// Dial a local socket at `socket_path` (newline-JSON transport). Same
/// `interprocess` local-socket idiom as the reattach back-channel.
fn dial(socket_path: &Path) -> std::io::Result<LocalSocketStream> {
    let name = socket_path.to_fs_name::<GenericFilePath>()?;
    LocalSocketStream::connect(name)
}

/// Write one newline-delimited JSON frame and flush.
fn write_frame<W: Write>(writer: &mut W, line: &str) -> std::io::Result<()> {
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// stdout/stderr discriminator on the broker's `{"out",...}` frame — a local
/// mirror of [`ExecStream`] so the wire parse does not depend on that type's
/// serde representation.
#[derive(Clone, Copy)]
enum BrokerStream {
    Stdout,
    Stderr,
}

/// Spawn the per-phase brokered exec pipeline: it opens one connection to the
/// broker, sends the `exec` frame, and streams the `{"out",...}` output frames
/// into a [`SessionRun`] whose event grammar MIRRORS
/// [`spawn_environment_exec`], so the caller's consumer is unchanged. Must be
/// called from within a tokio runtime (the forwarder task bridges the pipeline's
/// unbounded sends onto the bounded `SessionRun` channel).
fn spawn_brokered_exec(
    target: BrokerExecTarget,
    command: HarnessCommand,
    stdin: Option<String>,
    timeout_secs: u64,
    backend_label: String,
) -> SessionRun {
    let (events_tx, events_rx) = mpsc::channel::<SessionEvent>(256);
    // The pipeline thread sends through an unbounded channel; the async forwarder
    // applies the bounded `SessionRun` channel's backpressure. Mirrors
    // `spawn_environment_exec`.
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
        run_brokered_exec_pipeline(&target, command, stdin, timeout_secs, &backend_label, &pipeline_tx);
    });

    SessionRun { session_id: None, events: events_rx, selected_backend, fallback_reason: None, pid: None }
}

/// The blocking brokered exec pipeline. Emits the same event grammar the owned
/// path does: `Started`, stdout deltas as `TextDelta`, stderr deltas as
/// recoverable `Error` frames, then a terminal `Finished` (or an unrecoverable
/// `Error`). There is NO METHOD_NOT_SUPPORTED fallback — brokered exec is the
/// daemon's job and has no buffered-exec twin on this socket.
fn run_brokered_exec_pipeline(
    target: &BrokerExecTarget,
    command: HarnessCommand,
    stdin: Option<String>,
    timeout_secs: u64,
    backend_label: &str,
    events: &mpsc::UnboundedSender<SessionEvent>,
) {
    let send = |event: SessionEvent| {
        let _ = events.send(event);
    };
    send(SessionEvent::Started { backend: backend_label.to_string(), session_id: None, pid: None });

    let stream_events = events.clone();
    let on_output = move |stream: BrokerStream, text: &str| {
        let event = match stream {
            BrokerStream::Stdout => SessionEvent::TextDelta { text: text.to_string() },
            BrokerStream::Stderr => SessionEvent::Error { message: text.to_string(), recoverable: true },
        };
        let _ = stream_events.send(event);
    };

    match brokered_exec_stream(target, command, stdin, timeout_secs, &on_output) {
        Ok(response) if response.timed_out => send(SessionEvent::Error {
            message: format!("environment exec timed out in {backend_label} after {timeout_secs}s"),
            recoverable: false,
        }),
        // A finished command with NO exit code was signal-killed / OOM-killed;
        // surface it as a terminal error (downstream treats `None` as success).
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

/// Open a connection, send the `exec` frame, and drive the response stream,
/// forwarding `{"out",...}` frames through `on_output` and returning the
/// terminal [`ExecResponse`] (`{"done"}`) or an error (`{"error"}` / a dropped
/// connection before a terminal frame).
fn brokered_exec_stream(
    target: &BrokerExecTarget,
    command: HarnessCommand,
    stdin: Option<String>,
    timeout_secs: u64,
    on_output: &(dyn Fn(BrokerStream, &str) + Send + Sync),
) -> Result<ExecResponse> {
    let request = ExecRequest {
        op: "exec",
        token: &target.token,
        run_id: &target.run_id,
        handle_id: &target.handle_id,
        command,
        stdin,
        timeout_secs: Some(timeout_secs),
    };
    let line = serde_json::to_string(&request).context("serializing environment broker exec frame")?;

    let mut stream = dial(&target.socket_path)
        .with_context(|| format!("dialing environment broker socket at {}", target.socket_path.display()))?;
    write_frame(&mut stream, &line).context("sending environment broker exec frame")?;

    let mut reader = BufReader::new(stream);
    let mut buf = String::new();
    loop {
        buf.clear();
        if reader.read_line(&mut buf).context("reading environment broker exec frame")? == 0 {
            return Err(anyhow!("environment broker closed the connection before a terminal exec frame"));
        }
        let trimmed = buf.trim();
        if trimmed.is_empty() {
            continue;
        }
        let frame: Value = serde_json::from_str(trimmed)
            .with_context(|| format!("parsing environment broker exec frame: {trimmed}"))?;
        if let Some(out) = frame.get("out").and_then(Value::as_str) {
            let text = frame.get("text").and_then(Value::as_str).unwrap_or_default();
            match out {
                "stdout" => on_output(BrokerStream::Stdout, text),
                "stderr" => on_output(BrokerStream::Stderr, text),
                other => return Err(anyhow!("environment broker sent an unknown output stream '{other}'")),
            }
        } else if let Some(done) = frame.get("done") {
            return serde_json::from_value(done.clone()).context("parsing environment broker exec ExecResponse");
        } else if let Some(error) = frame.get("error") {
            return Err(anyhow!("environment broker exec error: {}", error.as_str().unwrap_or("unknown error")));
        }
        // Unknown frame shape: ignore forward-compatibly and keep reading.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    /// The bare `ANIMUS_ENVIRONMENT_EXEC=<id>` override routes a non-local id as
    /// a BARE node (no repos needed — the workflow clones via a command).
    #[test]
    fn env_var_explicit_id_routes_a_bare_node() {
        let _lock = crate::test_env::scoped_state_serializer();
        let tmp = tempfile::tempdir().unwrap();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", Some("container"));
        let resolved =
            resolve_exec_environment(tmp.path(), "claude", PhaseRouting::default()).expect("non-local routes");
        assert_eq!(resolved.id, "container");
        assert!(resolved.spec_overrides.is_none(), "the bare env-var override carries no rule spec");
        // A LOCAL id stays local even via the override.
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
    fn config_routing_selects_the_environment() {
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

        // A non-local id routes a bare node — no repos required.
        let resolved = resolve_exec_environment(&root, "codex", PhaseRouting::default()).expect("rule routes");
        assert_eq!(resolved.id, "sandbox-env");
        // Non-matching harness, no default -> local path.
        assert_eq!(resolve_exec_environment(&root, "claude", PhaseRouting::default()), None);
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
    // Gate: workflow-level `environment:` override (bare node)
    // -----------------------------------------------------------------

    /// A WorkflowDefinition-level `environment:` routes the whole run to a BARE
    /// node — no repos, no workspace (the workflow clones via a command phase).
    #[test]
    fn workflow_level_environment_routes_a_bare_node() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let resolved = resolve_exec_environment(
            tmp.path(),
            "claude",
            PhaseRouting { workflow_env: Some("railway-env-123"), ..Default::default() },
        )
        .expect("workflow env routes");
        assert_eq!(resolved.id, "railway-env-123");
        assert!(resolved.spec_overrides.is_none(), "a bare workflow-level env carries no rule spec");
    }

    /// A phase-level `environment:` wins over the workflow-level one (precedence:
    /// phase_env > rule > workflow_env > default).
    #[test]
    fn phase_level_environment_overrides_the_workflow_environment() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        let resolved = resolve_exec_environment(
            tmp.path(),
            "claude",
            PhaseRouting { phase_env: Some("phase-env"), workflow_env: Some("workflow-env"), ..Default::default() },
        )
        .expect("phase env wins");
        assert_eq!(resolved.id, "phase-env");
    }

    /// A LOCAL workflow-level `environment:` id keeps the local path.
    #[test]
    fn workflow_level_local_environment_stays_local() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            resolve_exec_environment(
                tmp.path(),
                "claude",
                PhaseRouting { workflow_env: Some("local"), ..Default::default() }
            ),
            None
        );
    }

    /// `resolve_workflow_environment` loads the compiled config and routes the
    /// whole run — via a subject-kind routing rule or the `ANIMUS_ENVIRONMENT_EXEC`
    /// override. (Reading a WorkflowDefinition's `environment:` field is covered
    /// by `phase_executor`'s `phase_environment_overrides` unit test, which this
    /// helper delegates to for the workflow-level override.)
    #[test]
    fn resolve_workflow_environment_routes_via_config_and_override() {
        let _lock = crate::test_env::scoped_state_serializer();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".animus")).unwrap();
        std::fs::write(
            root.join(".animus").join("workflows.yaml"),
            r#"
environment_routing:
  rules:
    - match:
        kind: task
      environment: railway-env
"#,
        )
        .unwrap();
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        {
            let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", None);
            // A subject-kind rule routes the whole run to a bare node.
            let resolved = resolve_workflow_environment(&root, "standard", Some("task")).expect("kind rule routes");
            assert_eq!(resolved.id, "railway-env");
            // A subject kind with no matching rule stays local.
            assert_eq!(resolve_workflow_environment(&root, "standard", Some("blog")), None);
        }

        // The dev/test override forces a bare node regardless of config.
        let _gate = EnvVarGuard::set("ANIMUS_ENVIRONMENT_EXEC", Some("container"));
        assert_eq!(
            resolve_workflow_environment(&root, "standard", Some("task")).map(|env| env.id),
            Some("container".to_string())
        );
    }

    // -----------------------------------------------------------------
    // Spec builder: bare node + routing-rule spec overrides
    // -----------------------------------------------------------------

    /// The prepared node is BARE (no repos) but a routing rule's opaque `spec`
    /// overrides (image / env / metadata) still ride the prepared EnvironmentSpec.
    #[test]
    fn merge_github_repo_metadata_scopes_the_token_and_preserves_existing_metadata() {
        let mut spec = EnvironmentSpec {
            kind: "railway".to_string(),
            repos: Vec::new(),
            image: None,
            resources: None,
            env: BTreeMap::new(),
            metadata: Value::Null,
        };

        // Null metadata -> a fresh object carrying only github_repo.
        merge_github_repo_metadata(&mut spec, Some("launchapp-dev/animus-cli"));
        assert_eq!(
            spec.metadata.pointer("/github_repo").and_then(Value::as_str),
            Some("launchapp-dev/animus-cli"),
            "github_repo lands on metadata so the plugin repo-scopes the token"
        );

        // Existing metadata keys (e.g. the broker's animus_run_id) are PRESERVED.
        spec.metadata = serde_json::json!({ "animus_run_id": "RUN-1", "network": "none" });
        merge_github_repo_metadata(&mut spec, Some("  launchapp-dev/animus-cli  "));
        assert_eq!(spec.metadata.pointer("/animus_run_id").and_then(Value::as_str), Some("RUN-1"));
        assert_eq!(spec.metadata.pointer("/network").and_then(Value::as_str), Some("none"));
        assert_eq!(
            spec.metadata.pointer("/github_repo").and_then(Value::as_str),
            Some("launchapp-dev/animus-cli"),
            "the repo is trimmed and merged in without clobbering sibling keys"
        );

        // None / empty repo (a non-coding run) leaves metadata untouched — never
        // invents a repo.
        let mut bare = EnvironmentSpec {
            kind: "railway".to_string(),
            repos: Vec::new(),
            image: None,
            resources: None,
            env: BTreeMap::new(),
            metadata: Value::Null,
        };
        merge_github_repo_metadata(&mut bare, None);
        assert!(bare.metadata.is_null(), "no repo -> metadata stays untouched");
        merge_github_repo_metadata(&mut bare, Some("   "));
        assert!(bare.metadata.is_null(), "blank repo -> metadata stays untouched");
    }

    #[test]
    fn prepared_spec_is_bare_but_carries_rule_spec_overrides() {
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
"#,
        )
        .unwrap();
        let _config_source_seam =
            orchestrator_config::workflow_config::config_source_client::install_yaml_config_source_base(&root);

        let resolved = resolve_exec_environment(&root, "claude", PhaseRouting::default()).expect("rule routes");
        assert_eq!(resolved.id, "container-env");

        let backend = Arc::new(FakeBackend::new());
        let prepared = PreparedEnvironment::prepare_with_backend(
            backend.clone(),
            "environment:container-env".to_string(),
            &resolved,
            None,
        )
        .expect("prepare succeeds");
        assert_eq!(prepared.id(), "container-env");

        let spec = backend.last_spec.lock().unwrap().clone().expect("spec captured on prepare");
        // BARE node: the rule's `repos` key is NOT materialized onto the spec.
        assert!(spec.repos.is_empty(), "the prepared node carries no repos");
        assert!(spec.metadata.pointer("/repos").is_none(), "repos must not leak onto metadata");
        // The rest of the rule spec still lands on its typed / opaque fields.
        assert_eq!(spec.image.as_deref(), Some("acme/dev:latest"));
        assert_eq!(spec.env.get("IN_CONTAINER").map(String::as_str), Some("1"));
        assert_eq!(spec.metadata.pointer("/network").and_then(Value::as_str), Some("none"));
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
            "phase_capabilities": { "writes_files": true },
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
        // claude env-exec defaults to bypassPermissions (inserted after the program).
        assert_eq!(
            command.args,
            vec![
                "--flag".to_string(),
                "--permission-mode".to_string(),
                "bypassPermissions".to_string(),
                "value".to_string()
            ]
        );
        assert_eq!(command.env.get("FROM_LAUNCH").map(String::as_str), Some("launch"));
        assert_eq!(command.env.get("SHARED").map(String::as_str), Some("request"), "request env wins on collision");
    }

    #[test]
    fn harness_command_strips_machine_output_from_a_contract_launch_too() {
        let mut request = sample_request("claude");
        request.extras = serde_json::json!({
            "phase_capabilities": { "writes_files": true },
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
        assert_eq!(
            command.args,
            vec![
                "--print".to_string(),
                "--permission-mode".to_string(),
                "bypassPermissions".to_string(),
                "prompt".to_string()
            ]
        );
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
    fn harness_command_defaults_bypass_only_for_write_capable_claude_phases() {
        // Write-capable claude phase with no explicit mode -> bypassPermissions.
        let mut write_request = sample_request("claude");
        write_request.extras = serde_json::json!({ "phase_capabilities": { "writes_files": true } });
        let (command, _stdin) =
            harness_command_for_request(Path::new("."), &write_request).expect("write-capable builds");
        assert!(
            command.args.iter().any(|arg| arg == "bypassPermissions"),
            "a write-capable claude phase launches in bypassPermissions: {:?}",
            command.args
        );

        // Read-only claude phase (no write capability) is NOT forced into an
        // edit-permitting mode.
        let mut read_only_request = sample_request("claude");
        read_only_request.extras = serde_json::json!({ "phase_capabilities": { "writes_files": false } });
        let (command, _stdin) =
            harness_command_for_request(Path::new("."), &read_only_request).expect("read-only builds");
        assert!(
            !command.args.iter().any(|arg| arg == "bypassPermissions"),
            "a read-only claude phase is not forced into an edit-permitting mode: {:?}",
            command.args
        );
    }

    #[test]
    fn harness_command_disables_the_codex_sandbox_for_the_node() {
        // Codex's bwrap sandbox can't create namespaces in the nested node, so a
        // codex node phase must launch with the sandbox off + auto-approve or it
        // can't exec any shell (even a read-only `git diff`).
        let request = sample_request("codex");
        let (command, _stdin) = harness_command_for_request(Path::new("."), &request).expect("codex builds");
        let joined = command.args.join(" ");
        assert!(
            joined.contains("sandbox_mode=\"danger-full-access\""),
            "codex node argv disables the inner sandbox: {:?}",
            command.args
        );
        assert!(
            joined.contains("approval_policy=\"never\""),
            "codex node argv auto-approves: {:?}",
            command.args
        );

        // A non-codex tool is untouched.
        let claude = sample_request("claude");
        let (claude_cmd, _s) = harness_command_for_request(Path::new("."), &claude).expect("claude builds");
        assert!(!claude_cmd.args.join(" ").contains("sandbox_mode"), "claude gets no codex sandbox flag");
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
        prepare_calls: AtomicUsize,
        exec_calls: AtomicUsize,
        exec_stream_calls: AtomicUsize,
        teardown_calls: AtomicUsize,
        last_spec: Mutex<Option<EnvironmentSpec>>,
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
                prepare_result: Mutex::new(None),
                exec_stream_outcome: Mutex::new(None),
                exec_result: Mutex::new(None),
                prepare_calls: AtomicUsize::new(0),
                exec_calls: AtomicUsize::new(0),
                exec_stream_calls: AtomicUsize::new(0),
                teardown_calls: AtomicUsize::new(0),
                last_spec: Mutex::new(None),
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
        fn prepare(&self, spec: EnvironmentSpec) -> Result<EnvironmentHandle> {
            self.prepare_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_spec.lock().unwrap() = Some(spec);
            // Default to a fresh handle so a `PreparedEnvironment` can be built
            // without stubbing; an explicit `prepare_result` (e.g. a failure)
            // wins when set.
            self.prepare_result.lock().unwrap().take().unwrap_or_else(|| Ok(sample_handle()))
        }

        fn exec_stream(
            &self,
            _handle: &EnvironmentHandle,
            command: HarnessCommand,
            stdin: Option<String>,
            _timeout: Option<Duration>,
            on_output: &(dyn Fn(ExecStream, &str) + Send + Sync),
        ) -> Result<ExecResponse> {
            self.exec_stream_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_command.lock().unwrap() = Some(command);
            *self.last_stdin.lock().unwrap() = stdin;
            // Default to a clean exit-0 stream when unstubbed, so a multi-phase
            // exec test does not have to re-stub before every call.
            match self.exec_stream_outcome.lock().unwrap().take() {
                Some(StreamOutcome::Deltas(deltas, result)) => {
                    for (stream, text) in deltas {
                        on_output(stream, &text);
                    }
                    result
                }
                Some(StreamOutcome::Fail(err)) => Err(err),
                None => Ok(ok_response(0)),
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
            self.teardown_calls.fetch_add(1, Ordering::SeqCst);
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

    fn command_for_test() -> HarnessCommand {
        HarnessCommand {
            program: "claude".to_string(),
            args: vec!["--print".to_string()],
            env: BTreeMap::new(),
            cwd: None,
        }
    }

    /// A `ResolvedEnvironment` for a bare non-local node.
    fn bare_env(id: &str) -> ResolvedEnvironment {
        ResolvedEnvironment { id: id.to_string(), spec_overrides: None }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn env_exec_streams_deltas_through_the_session_channel() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Deltas(
            vec![(ExecStream::Stdout, "out-1".to_string()), (ExecStream::Stderr, "err-1".to_string())],
            Ok(ok_response(7)),
        ));

        let run = spawn_environment_exec(
            backend.clone(),
            sample_handle(),
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
        // The per-phase exec pipeline NEVER prepares or tears down — those bracket
        // the whole run in `PreparedEnvironment`.
        assert_eq!(backend.prepare_calls.load(Ordering::SeqCst), 0, "exec pipeline does not prepare");
        assert_eq!(backend.teardown_calls.load(Ordering::SeqCst), 0, "exec pipeline does not tear down");
        let command = backend.last_command.lock().unwrap().clone().expect("command routed to the backend");
        assert_eq!(command.program, "claude", "the HarnessCommand reaches EnvironmentExecBackend::exec_stream");
    }

    /// `PreparedEnvironment::prepare` propagates a `prepare` failure as an error —
    /// the run is never silently executed locally.
    #[test]
    fn prepare_failure_is_an_error_never_a_silent_local_run() {
        let backend = Arc::new(FakeBackend::new());
        *backend.prepare_result.lock().unwrap() = Some(Err(anyhow!("no docker daemon")));
        let err = match PreparedEnvironment::prepare_with_backend(
            backend.clone(),
            "environment:container".to_string(),
            &bare_env("container"),
            None,
        ) {
            Ok(_) => panic!("prepare failure must surface as an error"),
            Err(err) => err,
        };
        assert!(format!("{err:#}").contains("prepare failed"), "error names the failed prepare: {err:#}");
        assert_eq!(backend.exec_calls.load(Ordering::SeqCst), 0, "nothing executed after a failed prepare");
        assert_eq!(backend.teardown_calls.load(Ordering::SeqCst), 0, "no handle -> no teardown");
    }

    /// The per-workflow node is prepared ONCE, shared across every phase's exec,
    /// and torn down ONCE (idempotent across the explicit call + Drop backstop).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_workflow_node_prepares_once_execs_many_and_tears_down_once() {
        let backend = Arc::new(FakeBackend::new());
        let prepared = PreparedEnvironment::prepare_with_backend(
            backend.clone(),
            "environment:container".to_string(),
            &bare_env("container"),
            None,
        )
        .expect("prepare succeeds");
        assert_eq!(backend.prepare_calls.load(Ordering::SeqCst), 1, "prepared exactly once");

        // Two phases exec inside the SAME held node (default clean exit-0 stream).
        for _ in 0..2 {
            let run = prepared.exec_session(Path::new("."), &sample_request("claude")).expect("exec builds");
            let events = drain(run).await;
            assert!(matches!(events.last(), Some(SessionEvent::Finished { exit_code: Some(0) })), "events: {events:?}");
        }
        assert_eq!(backend.exec_stream_calls.load(Ordering::SeqCst), 2, "both phases exec on the shared handle");
        assert_eq!(backend.prepare_calls.load(Ordering::SeqCst), 1, "no re-prepare between phases");
        assert_eq!(backend.teardown_calls.load(Ordering::SeqCst), 0, "not torn down mid-run");

        // Teardown twice -> the backend is disposed exactly once.
        prepared.teardown();
        prepared.teardown();
        assert_eq!(backend.teardown_calls.load(Ordering::SeqCst), 1, "torn down exactly once (idempotent)");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exec_failure_fails_the_run() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Fail(anyhow!("plugin died")));

        let run = spawn_environment_exec(
            backend.clone(),
            sample_handle(),
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

        let run = spawn_environment_exec(
            backend.clone(),
            sample_handle(),
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_exit_code_is_a_terminal_error() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Deltas(
            Vec::new(),
            Ok(ExecResponse { exit_code: None, stdout: String::new(), stderr: String::new(), timed_out: false }),
        ));

        let run = spawn_environment_exec(
            backend.clone(),
            sample_handle(),
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_exec_is_a_terminal_error() {
        let backend = Arc::new(FakeBackend::new());
        *backend.exec_stream_outcome.lock().unwrap() = Some(StreamOutcome::Deltas(
            Vec::new(),
            Ok(ExecResponse { exit_code: None, stdout: String::new(), stderr: String::new(), timed_out: true }),
        ));

        let run = spawn_environment_exec(
            backend.clone(),
            sample_handle(),
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
    }

    // -----------------------------------------------------------------
    // Broker client framing (fake in-process socket server)
    // -----------------------------------------------------------------

    use interprocess::local_socket::{GenericFilePath as TestGenericFilePath, ListenerOptions};

    fn socket_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broker.sock");
        (dir, path)
    }

    /// Bind a fake broker socket and, on the FIRST connection, read one request
    /// line and hand it (plus the writable stream) to `handler`. The listener is
    /// bound before this returns, so a client can connect immediately with no race.
    fn serve_once(
        path: PathBuf,
        handler: impl FnOnce(String, &mut dyn Write) + Send + 'static,
    ) -> std::thread::JoinHandle<()> {
        let name = path.to_fs_name::<TestGenericFilePath>().unwrap();
        let listener = ListenerOptions::new().name(name).create_sync().unwrap();
        std::thread::spawn(move || {
            let conn = listener.accept().unwrap();
            let mut reader = BufReader::new(conn);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut conn = reader.into_inner();
            handler(line, &mut conn);
        })
    }

    #[test]
    fn broker_env_requires_all_four_vars() {
        let _lock = crate::test_env::scoped_state_serializer();
        let _s = EnvVarGuard::set(BROKER_SOCKET_ENV, Some("/tmp/broker.sock"));
        let _t = EnvVarGuard::set(BROKER_TOKEN_ENV, Some("tok"));
        let _r = EnvVarGuard::set(BROKER_RUN_ID_ENV, Some("run-1"));
        {
            // Missing environment_id -> not brokered.
            let _e = EnvVarGuard::set(BROKER_ENVIRONMENT_ID_ENV, None);
            assert!(broker_env().is_none(), "partial broker env is treated as absent");
        }
        let _e = EnvVarGuard::set(BROKER_ENVIRONMENT_ID_ENV, Some("railway"));
        let env = broker_env().expect("all four present");
        assert_eq!(env.token, "tok");
        assert_eq!(env.run_id, "run-1");
        assert_eq!(env.environment_id, "railway");
        assert_eq!(env.socket_path, PathBuf::from("/tmp/broker.sock"));
    }

    #[test]
    fn brokered_acquire_sends_bare_spec_and_parses_workspace_and_handle() {
        let (_dir, path) = socket_path();
        let server = serve_once(path.clone(), |req_line, out| {
            let v: Value = serde_json::from_str(req_line.trim()).unwrap();
            assert_eq!(v["op"], "acquire");
            assert_eq!(v["token"], "tok");
            assert_eq!(v["run_id"], "run-1");
            assert_eq!(v["environment_id"], "railway");
            // Bare spec: empty repos are skipped on the wire; run id is stamped
            // on metadata for deterministic per-run node naming.
            assert!(v["spec"].get("repos").is_none(), "bare node carries no repos: {v}");
            assert_eq!(v["spec"]["metadata"]["animus_run_id"], "run-1");
            // The target repo rides metadata so the plugin repo-scopes the token,
            // alongside (not clobbering) the run id.
            assert_eq!(v["spec"]["metadata"]["github_repo"], "launchapp-dev/animus-cli");
            writeln!(out, r#"{{"ok":true,"workspace_root":"/workspace","handle_id":"h-1"}}"#).unwrap();
            out.flush().unwrap();
        });
        let env = BrokerEnv {
            socket_path: path,
            token: "tok".to_string(),
            run_id: "run-1".to_string(),
            environment_id: "railway".to_string(),
        };
        let brokered = BrokeredEnvironment::acquire(env, Some("launchapp-dev/animus-cli")).expect("acquire succeeds");
        assert_eq!(brokered.id(), "railway");
        assert_eq!(brokered.handle_id, "h-1");
        assert_eq!(brokered.workspace_root, "/workspace");
        server.join().unwrap();
    }

    #[test]
    fn brokered_acquire_failure_is_an_error() {
        let (_dir, path) = socket_path();
        let server = serve_once(path.clone(), |_req, out| {
            writeln!(out, r#"{{"ok":false,"error":"no capacity"}}"#).unwrap();
            out.flush().unwrap();
        });
        let env = BrokerEnv {
            socket_path: path,
            token: "tok".to_string(),
            run_id: "run-1".to_string(),
            environment_id: "railway".to_string(),
        };
        let err = BrokeredEnvironment::acquire(env, None).expect_err("ok:false must fail");
        assert!(format!("{err:#}").contains("no capacity"), "error surfaces the broker reason: {err:#}");
        server.join().unwrap();
    }

    #[test]
    fn brokered_exec_streams_out_frames_then_terminal_done() {
        let (_dir, path) = socket_path();
        let server = serve_once(path.clone(), |req_line, out| {
            let v: Value = serde_json::from_str(req_line.trim()).unwrap();
            assert_eq!(v["op"], "exec");
            assert_eq!(v["handle_id"], "h-1");
            assert_eq!(v["command"]["program"], "claude");
            writeln!(out, r#"{{"out":"stdout","text":"hello "}}"#).unwrap();
            writeln!(out, r#"{{"out":"stderr","text":"warn"}}"#).unwrap();
            writeln!(out, r#"{{"out":"stdout","text":"world"}}"#).unwrap();
            writeln!(out, r#"{{"done":{{"exit_code":0,"stdout":"","stderr":"","timed_out":false}}}}"#).unwrap();
            out.flush().unwrap();
        });
        let target = BrokerExecTarget {
            socket_path: path,
            token: "tok".to_string(),
            run_id: "run-1".to_string(),
            handle_id: "h-1".to_string(),
        };
        let collected = Mutex::new(Vec::<(String, String)>::new());
        let response = brokered_exec_stream(&target, command_for_test(), None, 60, &|stream, text| {
            let kind = match stream {
                BrokerStream::Stdout => "out",
                BrokerStream::Stderr => "err",
            };
            collected.lock().unwrap().push((kind.to_string(), text.to_string()));
        })
        .expect("exec succeeds");
        assert_eq!(response.exit_code, Some(0));
        assert_eq!(
            collected.into_inner().unwrap(),
            vec![
                ("out".to_string(), "hello ".to_string()),
                ("err".to_string(), "warn".to_string()),
                ("out".to_string(), "world".to_string()),
            ]
        );
        server.join().unwrap();
    }

    #[test]
    fn brokered_exec_error_frame_is_a_terminal_error() {
        let (_dir, path) = socket_path();
        let server = serve_once(path.clone(), |_req, out| {
            writeln!(out, r#"{{"error":"boom inside the node"}}"#).unwrap();
            out.flush().unwrap();
        });
        let target = BrokerExecTarget {
            socket_path: path,
            token: "tok".to_string(),
            run_id: "run-1".to_string(),
            handle_id: "h-1".to_string(),
        };
        let err =
            brokered_exec_stream(&target, command_for_test(), None, 60, &|_, _| {}).expect_err("error frame fails");
        assert!(format!("{err:#}").contains("boom inside the node"), "error surfaces the broker message: {err:#}");
        server.join().unwrap();
    }

    /// The full brokered pipeline yields a `SessionRun` whose event grammar
    /// MIRRORS the owned path: `Started`, stdout `TextDelta`, terminal `Finished`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn brokered_exec_session_mirrors_the_session_event_grammar() {
        let (_dir, path) = socket_path();
        let server = serve_once(path.clone(), |_req, out| {
            writeln!(out, r#"{{"out":"stdout","text":"delta"}}"#).unwrap();
            writeln!(out, r#"{{"done":{{"exit_code":3,"stdout":"","stderr":"","timed_out":false}}}}"#).unwrap();
            out.flush().unwrap();
        });
        let target = BrokerExecTarget {
            socket_path: path,
            token: "tok".to_string(),
            run_id: "run-1".to_string(),
            handle_id: "h-1".to_string(),
        };
        let run = spawn_brokered_exec(target, command_for_test(), None, 60, "environment-broker:railway".to_string());
        assert_eq!(run.selected_backend, "environment-broker:railway");
        let events = drain(run).await;
        assert!(
            matches!(&events[0], SessionEvent::Started { backend, .. } if backend == "environment-broker:railway"),
            "first frame is Started: {events:?}"
        );
        assert!(
            events.iter().any(|e| matches!(e, SessionEvent::TextDelta { text } if text == "delta")),
            "stdout rides TextDelta (same channel as the owned path): {events:?}"
        );
        assert!(
            matches!(events.last(), Some(SessionEvent::Finished { exit_code: Some(3) })),
            "terminal frame carries the exec exit code: {events:?}"
        );
        server.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn brokered_exec_session_fails_closed_when_the_socket_is_unreachable() {
        // No server bound at this path: a dial failure must surface as a terminal
        // error (fail closed), never a silent fallback.
        let (_dir, path) = socket_path();
        let target = BrokerExecTarget {
            socket_path: path,
            token: "tok".to_string(),
            run_id: "run-1".to_string(),
            handle_id: "h-1".to_string(),
        };
        let run = spawn_brokered_exec(target, command_for_test(), None, 60, "environment-broker:railway".to_string());
        let events = drain(run).await;
        assert!(
            matches!(
                events.last(),
                Some(SessionEvent::Error { message, recoverable: false }) if message.contains("exec failed")
            ),
            "an unreachable broker fails the phase: {events:?}"
        );
    }
}
