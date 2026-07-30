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
//! session RPC. The node brings up its own daemon,
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
//! Animus CLI rc.30 deliberately narrows `orchestrator_core::EnvironmentClient`
//! to the baseline prepare/exec/teardown contract, while Protocol rc.12 retains
//! the optional `environment/exec_session` surface. This module therefore owns
//! the small session-specific resident-host adapter it needs. The adapter uses
//! the kernel's public plugin-host registry and the same spawn fingerprint as
//! `EnvironmentClient`, so prepare, exec_session, and teardown still land on one
//! pinned environment process. The actual delegation path remains compiled only
//! under the `remote-animus-session` feature.
//!
//! The PURE decision logic ([`capabilities_advertise_exec_session`],
//! [`map_journal_event_kind`], [`session_status_to_workflow_status`]) is compiled
//! and unit-tested unconditionally, so the mechanism is reviewable without the
//! feature.

use std::path::Path;

use animus_actor::Actor;
use animus_execution_protocol::ExecutionFence;
#[cfg(feature = "remote-animus-session")]
use animus_workflow_runner_protocol::PublicationReceipt;
use anyhow::Result;
use orchestrator_config::{WorkflowPublicationCleanupPolicy, WorkflowPublicationConfig};
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

/// Journal notification shape retained by Protocol rc.12 for
/// `environment/exec_session`. This used to be re-exported by
/// `orchestrator_core`; keeping the wire-shaped type here decouples the runner
/// from that removed convenience API.
#[cfg(feature = "remote-animus-session")]
#[derive(Debug, serde::Deserialize)]
struct SessionJournalEvent {
    handle_id: String,
    workflow_id: Option<String>,
    event_kind: String,
    phase_id: Option<String>,
    status: Option<String>,
    ts: String,
    payload: serde_json::Value,
    #[allow(dead_code)]
    terminal: bool,
}

#[cfg(feature = "remote-animus-session")]
fn forward_session_journal<F>(notification: &animus_plugin_protocol::RpcNotification, handle_id: &str, on_journal: &F)
where
    F: Fn(&SessionJournalEvent),
{
    if notification.method != animus_environment_protocol::NOTIFICATION_ENVIRONMENT_JOURNAL {
        return;
    }
    let Some(params) = notification.params.clone() else {
        return;
    };
    if let Ok(event) = serde_json::from_value::<SessionJournalEvent>(params) {
        if event.handle_id == handle_id {
            on_journal(&event);
        }
    }
}

/// Session-only environment client for CLI rc.30. Holds a resident-host lease
/// for its whole lifetime so all stateful environment RPCs share one process.
#[cfg(feature = "remote-animus-session")]
#[derive(Clone)]
struct SessionRegistryKey {
    plugin_path: std::path::PathBuf,
    binary_mtime: u128,
    spawn_context: String,
}

#[cfg(feature = "remote-animus-session")]
struct PinnedSessionLease {
    lease: orchestrator_plugin_host::resident_host_registry::ResidentHostLease,
    generation: u64,
}

#[cfg(feature = "remote-animus-session")]
enum SessionHostCallError {
    Death(anyhow::Error),
    Other(anyhow::Error),
}

#[cfg(feature = "remote-animus-session")]
impl SessionHostCallError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Death(error) | Self::Other(error) => error,
        }
    }
}

#[cfg(feature = "remote-animus-session")]
struct SessionEnvironmentClient {
    plugin_name: String,
    plugin: orchestrator_plugin_host::DiscoveredPlugin,
    forwarded_env: Vec<String>,
    registry_key: SessionRegistryKey,
    pinned: tokio::sync::Mutex<Option<PinnedSessionLease>>,
}

#[cfg(feature = "remote-animus-session")]
impl SessionEnvironmentClient {
    async fn resolve(project_root: &Path, environment_id: &str) -> Result<Self> {
        use anyhow::{anyhow, Context};
        use orchestrator_plugin_host::resident_host_registry::{binary_mtime_nanos, spawn_context_fingerprint};

        let plugins = orchestrator_plugin_host::discover_by_kind(project_root.to_path_buf(), PLUGIN_KIND_ENVIRONMENT)
            .with_context(|| format!("discovering environment plugins for {}", project_root.display()))?;
        let plugin = if let Some(exact) = plugins.iter().find(|plugin| plugin.name == environment_id) {
            exact.clone()
        } else if plugins.len() == 1 {
            plugins.into_iter().next().expect("length checked")
        } else {
            let candidates = plugins.iter().map(|plugin| plugin.name.as_str()).collect::<Vec<_>>().join(", ");
            return Err(anyhow!(
                "no installed environment plugin matches environment id '{environment_id}'; installed environment plugins: [{candidates}]"
            ));
        };

        let forwarded_env: Vec<String> = std::env::vars().map(|(name, _)| name).collect();
        let registry_key = SessionRegistryKey {
            plugin_path: plugin.path.clone(),
            binary_mtime: binary_mtime_nanos(&plugin.path),
            spawn_context: spawn_context_fingerprint(&forwarded_env, None, plugin.manifest.notification_buffer_size),
        };
        let client = Self {
            plugin_name: plugin.name.clone(),
            plugin,
            forwarded_env,
            registry_key,
            pinned: tokio::sync::Mutex::new(None),
        };
        let _ = client.pinned_host().await?;
        Ok(client)
    }

    async fn acquire_lease(&self) -> Result<orchestrator_plugin_host::resident_host_registry::ResidentHostLease> {
        use anyhow::Context;
        use orchestrator_plugin_host::resident_host_registry::global_resident_host_registry;
        use orchestrator_plugin_host::{PluginHost, PluginSpawnOptions};

        let plugin = self.plugin.clone();
        let forwarded_env = self.forwarded_env.clone();
        global_resident_host_registry()
            .get_or_spawn(
                &self.registry_key.plugin_path,
                self.registry_key.binary_mtime,
                &self.registry_key.spawn_context,
                || async move {
                    let options = PluginSpawnOptions::for_manifest(
                        plugin.name.clone(),
                        &plugin.manifest.env_required,
                        forwarded_env,
                        None,
                    )
                    .with_notification_buffer_hint(plugin.manifest.notification_buffer_size);
                    let host = PluginHost::spawn_with_options(&plugin.path, &[], options)
                        .await
                        .with_context(|| format!("spawning environment plugin {}", plugin.name))?;
                    if let Err(error) = host.handshake().await {
                        let _ = host.clone().shutdown().await;
                        return Err(error)
                            .with_context(|| format!("handshake with environment plugin {}", plugin.name));
                    }
                    Ok(host)
                },
            )
            .await
    }

    async fn pinned_host(&self) -> Result<(orchestrator_plugin_host::PluginHost, u64)> {
        let mut pinned = self.pinned.lock().await;
        if pinned.is_none() {
            let lease = self.acquire_lease().await?;
            let generation = lease.generation();
            *pinned = Some(PinnedSessionLease { lease, generation });
        }
        let pinned = pinned.as_ref().expect("lease populated above");
        Ok((pinned.lease.host().clone(), pinned.generation))
    }

    async fn invalidate_generation(&self, generation: u64) {
        {
            let mut pinned = self.pinned.lock().await;
            if pinned.as_ref().is_some_and(|lease| lease.generation == generation) {
                *pinned = None;
            }
        }
        orchestrator_plugin_host::resident_host_registry::global_resident_host_registry()
            .invalidate_generation(
                &self.registry_key.plugin_path,
                self.registry_key.binary_mtime,
                &self.registry_key.spawn_context,
                generation,
            )
            .await;
    }

    async fn classify_host_error(
        &self,
        generation: u64,
        error: orchestrator_plugin_host::HostError,
    ) -> SessionHostCallError {
        use orchestrator_plugin_host::session::plugin_supervisor::{classify, RetryDecision};

        match classify(&error) {
            RetryDecision::DeathLike => {
                self.invalidate_generation(generation).await;
                SessionHostCallError::Death(anyhow::Error::new(error))
            }
            RetryDecision::StructuredError => SessionHostCallError::Other(anyhow::Error::new(error)),
        }
    }

    async fn request_once(
        &self,
        method: &'static str,
        params: serde_json::Value,
        timeout: Option<std::time::Duration>,
    ) -> std::result::Result<serde_json::Value, SessionHostCallError> {
        let (host, generation) = self.pinned_host().await.map_err(SessionHostCallError::Other)?;
        let result = match timeout {
            Some(timeout) => host.request_typed_with_timeout(method, Some(params), timeout).await,
            None => host.request_typed(method, Some(params)).await,
        };
        match result {
            Ok(value) => Ok(value),
            Err(error) => Err(self.classify_host_error(generation, error).await),
        }
    }

    /// Control calls mirror rc.30 `EnvironmentClient`: retry once only after a
    /// death-like failure because prepare and teardown are safe to replay.
    async fn control_request(
        &self,
        method: &'static str,
        params: serde_json::Value,
        timeout: std::time::Duration,
    ) -> Result<serde_json::Value> {
        let retry_params = params.clone();
        match self.request_once(method, params, Some(timeout)).await {
            Ok(value) => Ok(value),
            Err(SessionHostCallError::Death(_)) => {
                self.request_once(method, retry_params, Some(timeout)).await.map_err(SessionHostCallError::into_anyhow)
            }
            Err(error) => Err(error.into_anyhow()),
        }
    }

    async fn prepare(
        &self,
        spec: animus_environment_protocol::EnvironmentSpec,
        actor: Option<&Actor>,
    ) -> Result<animus_environment_protocol::EnvironmentHandle> {
        use std::time::Duration;

        use animus_environment_protocol::{PrepareRequest, PrepareResponse, METHOD_ENVIRONMENT_PREPARE};
        use anyhow::Context;

        let params =
            request_params_with_actor(PrepareRequest { spec }, actor, "serializing environment prepare request")?;
        let value = self
            .control_request(METHOD_ENVIRONMENT_PREPARE, params, Duration::from_secs(360))
            .await
            .with_context(|| format!("environment prepare via {}", self.plugin_name))?;
        let response: PrepareResponse = serde_json::from_value(value)
            .with_context(|| format!("decoding prepare response from {}", self.plugin_name))?;
        Ok(response.handle)
    }

    async fn probe(&self, handle: &animus_environment_protocol::EnvironmentHandle) -> bool {
        use std::collections::BTreeMap;
        use std::time::Duration;

        use animus_environment_protocol::{ExecRequest, HarnessCommand, METHOD_ENVIRONMENT_EXEC};

        let request = ExecRequest {
            handle: handle.clone(),
            command: HarnessCommand { program: "true".to_string(), args: Vec::new(), env: BTreeMap::new(), cwd: None },
            stdin: None,
            timeout_secs: Some(10),
        };
        let Ok(params) = serde_json::to_value(request) else {
            return false;
        };
        self.request_once(METHOD_ENVIRONMENT_EXEC, params, Some(Duration::from_secs(40))).await.is_ok()
    }

    async fn exec_git(
        &self,
        handle: &animus_environment_protocol::EnvironmentHandle,
        args: Vec<String>,
    ) -> Result<crate::phase_git::PublicationCommandOutput> {
        use std::collections::BTreeMap;
        use std::time::Duration;

        use animus_environment_protocol::{ExecRequest, ExecResponse, HarnessCommand, METHOD_ENVIRONMENT_EXEC};
        use anyhow::Context;

        let request = ExecRequest {
            handle: handle.clone(),
            command: HarnessCommand {
                program: "git".to_string(),
                args,
                env: BTreeMap::new(),
                // The delegated Animus owns the checkout at its workspace root.
                // Never pass the unrelated home runner's absolute cwd here.
                cwd: None,
            },
            stdin: None,
            timeout_secs: Some(180),
        };
        let params = serde_json::to_value(request).context("serializing environment git verification request")?;
        let value = self
            .request_once(METHOD_ENVIRONMENT_EXEC, params, Some(Duration::from_secs(210)))
            .await
            .map_err(SessionHostCallError::into_anyhow)
            .with_context(|| format!("git verification via {}", self.plugin_name))?;
        let response: ExecResponse = serde_json::from_value(value)
            .with_context(|| format!("decoding git verification response from {}", self.plugin_name))?;
        Ok(crate::phase_git::PublicationCommandOutput {
            success: response.exit_code == Some(0) && !response.timed_out,
            stdout: response.stdout,
            stderr: response.stderr,
        })
    }

    async fn exec_session<F>(
        &self,
        handle: &animus_environment_protocol::EnvironmentHandle,
        subject_id: String,
        workflow_ref: Option<String>,
        dispatch_input: Option<String>,
        workflow_id: Option<String>,
        execution_fence: Option<ExecutionFence>,
        actor: Option<&Actor>,
        on_journal: F,
    ) -> Result<animus_environment_protocol::ExecSessionResponse>
    where
        F: Fn(&SessionJournalEvent) + Send + Sync,
    {
        use animus_environment_protocol::{ExecSessionRequest, ExecSessionResponse, METHOD_ENVIRONMENT_EXEC_SESSION};
        use anyhow::Context;
        use tokio::sync::broadcast::error::RecvError;

        let request = ExecSessionRequest {
            handle: handle.clone(),
            subject_id,
            workflow_ref,
            dispatch_input,
            workflow_id,
            execution_fence,
        };
        let params = request_params_with_actor(request, actor, "serializing environment exec_session request")?;
        let (host, generation) = self.pinned_host().await?;
        let mut notifications = host.subscribe_notifications();
        let response = host.request_typed(METHOD_ENVIRONMENT_EXEC_SESSION, Some(params));
        tokio::pin!(response);

        let result = loop {
            tokio::select! {
                notification = notifications.recv() => match notification {
                    Ok(notification) => forward_session_journal(&notification, &handle.id, &on_journal),
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => break response.await,
                },
                result = &mut response => break result,
            }
        };

        let value = match result {
            Ok(value) => value,
            Err(error) => {
                return Err(self.classify_host_error(generation, error).await.into_anyhow())
                    .with_context(|| format!("environment exec_session via {}", self.plugin_name));
            }
        };

        let buffered = notifications.len();
        for _ in 0..buffered {
            let Ok(notification) = notifications.try_recv() else {
                break;
            };
            forward_session_journal(&notification, &handle.id, &on_journal);
        }

        serde_json::from_value::<ExecSessionResponse>(value)
            .with_context(|| format!("decoding exec_session response from {}", self.plugin_name))
    }

    async fn teardown(&self, handle: &animus_environment_protocol::EnvironmentHandle) -> Result<()> {
        use std::time::Duration;

        use animus_environment_protocol::{TeardownRequest, TeardownResponse, METHOD_ENVIRONMENT_TEARDOWN};
        use anyhow::Context;

        let params = serde_json::to_value(TeardownRequest { handle: handle.clone() })
            .context("serializing environment teardown request")?;
        let value = self
            .control_request(METHOD_ENVIRONMENT_TEARDOWN, params, Duration::from_secs(60))
            .await
            .with_context(|| format!("environment teardown via {}", self.plugin_name))?;
        let _: TeardownResponse = serde_json::from_value(value)
            .with_context(|| format!("decoding teardown response from {}", self.plugin_name))?;
        Ok(())
    }
}

#[cfg(feature = "remote-animus-session")]
#[async_trait::async_trait]
trait SessionPublicationCommands: Sync {
    async fn run_git(&self, args: Vec<String>) -> Result<crate::phase_git::PublicationCommandOutput>;
}

#[cfg(feature = "remote-animus-session")]
struct BoundSessionPublication<'a> {
    client: &'a SessionEnvironmentClient,
    handle: &'a animus_environment_protocol::EnvironmentHandle,
}

#[cfg(feature = "remote-animus-session")]
#[async_trait::async_trait]
impl SessionPublicationCommands for BoundSessionPublication<'_> {
    async fn run_git(&self, args: Vec<String>) -> Result<crate::phase_git::PublicationCommandOutput> {
        self.client.exec_git(self.handle, args).await
    }
}

#[cfg(feature = "remote-animus-session")]
async fn verify_session_publication<E>(
    executor: &E,
    proof: &PublicationReceipt,
    execution: &ExecutionFence,
) -> Result<bool>
where
    E: SessionPublicationCommands,
{
    if proof.validate_against_execution(execution).is_err() {
        return Ok(false);
    }
    let Some(repository) = execution.repository.as_ref() else {
        return Ok(false);
    };
    let configured_remote = executor.run_git(vec!["config".into(), "--get".into(), "remote.origin.url".into()]).await?;
    if !configured_remote.success {
        return Ok(false);
    }
    let configured_remote = crate::workflow_execute::canonical_remote_url(configured_remote.stdout.trim())?;
    let expected_identity = crate::workflow_execute::normalized_repository_identity(&repository.repository);
    if crate::workflow_execute::normalized_repository_identity(&configured_remote) != expected_identity
        || crate::workflow_execute::normalized_repository_identity(&proof.remote) != expected_identity
    {
        return Ok(false);
    }
    let observed =
        executor.run_git(vec!["ls-remote".into(), "--refs".into(), "origin".into(), proof.remote_ref.clone()]).await?;
    if !observed.success
        || crate::workflow_execute::exact_remote_sha(&observed.stdout, &proof.remote_ref).as_deref()
            != Some(proof.commit_sha.as_str())
    {
        return Ok(false);
    }
    let verify_ref = format!("refs/animus/session-proof/{}", &proof.commit_sha[..12]);
    let fetched = executor
        .run_git(vec![
            "fetch".into(),
            "--no-tags".into(),
            "origin".into(),
            format!("+{}:{verify_ref}", proof.remote_ref),
        ])
        .await?;
    if !fetched.success {
        return Ok(false);
    }

    let actual_commit = executor.run_git(vec!["rev-parse".into(), format!("{verify_ref}^{{commit}}")]).await?;
    let actual_tree = executor.run_git(vec!["rev-parse".into(), format!("{verify_ref}^{{tree}}")]).await?;
    let _ = executor.run_git(vec!["update-ref".into(), "-d".into(), verify_ref]).await;

    Ok(actual_commit.success
        && actual_tree.success
        && actual_commit.stdout.trim() == proof.commit_sha
        && actual_tree.stdout.trim() == proof.tree_sha)
}

/// Attach the SDK's well-known top-level actor field after serializing the
/// Protocol request. Protocol rc.12 intentionally does not own transport actor
/// context, so this preserves its typed request without forking the wire type.
#[cfg(feature = "remote-animus-session")]
fn request_params_with_actor<T: serde::Serialize>(
    request: T,
    actor: Option<&Actor>,
    context: &'static str,
) -> Result<serde_json::Value> {
    use anyhow::{anyhow, Context};

    let mut params = serde_json::to_value(request).context(context)?;
    if let Some(actor) = actor {
        let object = params.as_object_mut().ok_or_else(|| anyhow!("{context}: request must serialize as an object"))?;
        object
            .insert("actor".to_string(), serde_json::to_value(actor).context("serializing environment request actor")?);
    }
    Ok(params)
}

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

#[cfg_attr(not(feature = "remote-animus-session"), allow(dead_code))]
fn session_succeeded(status: WorkflowStatus) -> bool {
    matches!(status, WorkflowStatus::Completed)
}

#[cfg_attr(not(feature = "remote-animus-session"), allow(dead_code))]
fn session_should_teardown(
    status: WorkflowStatus,
    publication_required: bool,
    cleanup: WorkflowPublicationCleanupPolicy,
    publication_durable: bool,
) -> bool {
    matches!(status, WorkflowStatus::Cancelled)
        || (matches!(status, WorkflowStatus::Completed)
            && (!publication_required
                || (publication_durable && matches!(cleanup, WorkflowPublicationCleanupPolicy::AfterRemoteVerified))))
}

#[cfg(feature = "remote-animus-session")]
fn terminal_publication_receipt(event: &SessionJournalEvent, workflow_id: &str) -> Option<PublicationReceipt> {
    if !event.terminal || event.event_kind != "workflow_completed" || event.workflow_id.as_deref() != Some(workflow_id)
    {
        return None;
    }
    let receipt = event
        .payload
        .get("publication_receipt")
        .or_else(|| event.payload.pointer("/post_success/publication_receipt"))?;
    serde_json::from_value(receipt.clone()).ok()
}

#[cfg(feature = "remote-animus-session")]
fn validate_session_response_fence(
    response: &animus_environment_protocol::ExecSessionResponse,
    expected: Option<&ExecutionFence>,
    required: bool,
) -> Result<()> {
    match (expected, response.execution_fence.as_ref()) {
        (Some(expected), Some(actual)) if expected == actual => Ok(()),
        (Some(_), Some(_)) => anyhow::bail!("remote session returned a different execution fence"),
        (Some(_), None) => anyhow::bail!("remote session omitted the required execution fence"),
        (None, Some(_)) if required => anyhow::bail!("remote session returned unexpected scheduler authority"),
        (None, _) => Ok(()),
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
        "output_chunk"
            | "tool_call"
            | "tool_result"
            | "thinking"
            | "started"
            | "finished"
            | "metadata"
            | "error"
            | "artifact"
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
fn persist_session_transcript(project_root: &str, parent_workflow_id: &str, event: &SessionJournalEvent) {
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
    let selected = plugins.iter().find(|plugin| plugin.name == environment_id).or(if plugins.len() == 1 {
        plugins.first()
    } else {
        None
    });
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
    _publication: Option<&WorkflowPublicationConfig>,
    _execution_fence: Option<&ExecutionFence>,
    _actor: Option<&Actor>,
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
/// via a single `environment/exec_session` RPC (REQ-052): prepare a bare
/// node, hand it the subject + workflow ref, forward every `environment/journal`
/// event home through `event_emitter`, tear the node down, and synthesize a
/// [`WorkflowExecuteInternalResult`] from the terminal `ExecSessionResponse`.
///
/// `phases_requested` is the workflow's declared phase ids -- used only to fill
/// the result summary; the node, not the runner, actually drives them.
///
/// Environment-host work runs on a DEDICATED OS thread with its own
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
    publication: Option<&WorkflowPublicationConfig>,
    execution_fence: Option<&ExecutionFence>,
    actor: Option<&Actor>,
    event_emitter: Option<&SharedWorkflowEventEmitter>,
) -> Result<WorkflowExecuteInternalResult> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use anyhow::{anyhow, Context};
    use serde_json::Value;

    use crate::workflow_event_emitter::RuntimeWorkflowEvent;

    let started = Instant::now();

    // Shared, thread-safe accumulators the journal-forwarding closure writes and
    // the driver reads back after the session ends.
    let phase_results: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let phases_completed = Arc::new(AtomicUsize::new(0));
    let publication_required = publication.is_some_and(|publication| publication.required);
    let cleanup_policy = publication
        .map(|publication| publication.cleanup)
        .unwrap_or(WorkflowPublicationCleanupPolicy::AfterRemoteVerified);
    let publication_durable = Arc::new(AtomicBool::new(!publication_required));
    let publication_receipt: Arc<Mutex<Option<PublicationReceipt>>> = Arc::new(Mutex::new(None));

    // Clones captured by the (Fn + Send + Sync) journal callback.
    let emitter = event_emitter.cloned();
    let workflow_id_for_events = workflow_id.to_string();
    let project_root_for_journal = project_root.to_string();
    let phase_results_sink = phase_results.clone();
    let phases_completed_sink = phases_completed.clone();
    let publication_receipt_sink = publication_receipt.clone();

    let on_journal = move |event: &SessionJournalEvent| {
        if let Some(receipt) = terminal_publication_receipt(event, &workflow_id_for_events) {
            if let Ok(mut sink) = publication_receipt_sink.lock() {
                *sink = Some(receipt);
            }
        }
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
    // REQ-052 one-id (TASK-723): hand the delegating run's id to the node so it
    // RESUMES this row (`execute --workflow-id`) instead of minting its own --
    // exactly ONE journal_runs row per dispatch, and the transcript mirror
    // re-keys to a no-op. `None`-tolerant on the wire (old env plugins ignore it).
    let workflow_id_owned = workflow_id.to_string();
    let actor_owned = actor.cloned();
    let execution_fence_owned = execution_fence.cloned();
    let publication_durable_for_thread = publication_durable.clone();
    let publication_receipt_for_thread = publication_receipt.clone();

    // TASK-933 (companion to animus-cli rc.28): this delegated node is otherwise
    // invisible to the daemon after a restart -- the home runner holds the handle
    // in memory only, so a crash leaks the node (a full standalone animus keeps
    // running) and a re-dispatch prepares a SECOND one. Persist the node's
    // `EnvironmentBinding` into the run's session checkpoint at the phase the
    // rc.28 reconciler reads (`current_phase_id` = the phase at
    // `current_phase_index`), so the reconciler can reap a dead node / preserve a
    // live one / terminalize a ghost. `scoped_state_root` + the current phase id
    // are computed HOME-side (before the blocking thread); `None` degrades to the
    // pre-existing no-persist behavior rather than failing the run.
    let scoped_root_opt: Option<std::path::PathBuf> = protocol::scoped_state_root(Path::new(project_root));
    let binding_phase_id: Option<String> = match hub.workflows().get(workflow_id).await {
        Ok(wf) => wf.phases.get(wf.current_phase_index).map(|phase| phase.phase_id.clone()),
        Err(_) => None,
    }
    .or_else(|| phases_requested.first().cloned());
    // On a restart re-dispatch the run may already have a persisted node whose
    // handle is still live -- REUSE it (reattach, skip prepare) instead of
    // leaking it and preparing a fresh one. Read the candidate handle home-side;
    // liveness is probed inside the thread (needs the resolved client).
    let reuse_candidate: Option<animus_environment_protocol::EnvironmentHandle> =
        match (scoped_root_opt.as_deref(), environment_id) {
            (Some(scoped_root), env_id) => {
                crate::phase_session::find_reusable_binding(scoped_root, workflow_id, env_id)
                    .ok()
                    .flatten()
                    .map(|binding| binding.handle)
            }
            _ => None,
        };
    let scoped_root_for_thread = scoped_root_opt.clone();
    let binding_phase_id_for_thread = binding_phase_id.clone();

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("building dedicated runtime for the remote-animus session host")?;
            runtime.block_on(async move {
                let client = SessionEnvironmentClient::resolve(Path::new(&project_root_owned), &environment_id_owned)
                    .await
                    .map_err(|err| {
                        anyhow!(
                            "workflow is routed to session-capable environment '{environment_id_owned}' but no \
                             usable environment plugin was resolved (the run is NOT executed locally when a \
                             session environment is requested): {err}"
                        )
                    })?;
                // TASK-933: reuse a still-live persisted node (skip prepare) or
                // prepare a fresh one. `SessionEnvironmentClient::probe` is a trivial,
                // side-effect-free `exec` of `true` -- there is no environment
                // `status` method, so this exec IS the liveness check (mirrors the
                // rc.28 reconciler's `probe_delegate`).
                let (handle, reused) = match reuse_candidate {
                    Some(candidate) if client.probe(&candidate).await => (candidate, true),
                    _ => {
                        let handle = client.prepare(spec, actor_owned.as_ref()).await.map_err(|err| {
                            anyhow!("remote-animus session prepare failed for '{environment_id_owned}': {err:#}")
                        })?;
                        (handle, false)
                    }
                };
                if reused {
                    eprintln!(
                        "info: remote-animus session reattached to a live persisted node for '{environment_id_owned}' \
                         (handle {}) across restart -- skipped prepare",
                        handle.id
                    );
                }
                // TASK-933: persist the node binding into the run's session
                // checkpoint at the reconciler's phase BEFORE exec_session, so a
                // crash mid-session leaves a durable handle to reap/reattach.
                if let (Some(scoped_root), Some(phase_id)) =
                    (scoped_root_for_thread.as_deref(), binding_phase_id_for_thread.as_deref())
                {
                    persist_session_binding(scoped_root, &workflow_id_owned, phase_id, &environment_id_owned, &handle);
                }
                // Unbounded: an agent-run session's duration is not known up front.
                let response = client.exec_session(
                    &handle,
                    subject_id_owned,
                    Some(workflow_ref_owned),
                    dispatch_input_owned,
                    Some(workflow_id_owned.clone()),
                    execution_fence_owned.clone(),
                    actor_owned.as_ref(),
                    on_journal,
                ).await;
                if let Ok(response) = response.as_ref() {
                    validate_session_response_fence(response, execution_fence_owned.as_ref(), publication_required)?;
                }
                if response.is_ok() && !publication_durable_for_thread.load(std::sync::atomic::Ordering::SeqCst) {
                    let receipt = publication_receipt_for_thread
                        .lock()
                        .ok()
                        .and_then(|guard| guard.clone());
                    let verified = if let (Some(receipt), Some(execution)) =
                        (receipt.as_ref(), execution_fence_owned.as_ref())
                    {
                        verify_session_publication(
                            &BoundSessionPublication { client: &client, handle: &handle },
                            receipt,
                            execution,
                        )
                        .await
                        .unwrap_or(false)
                    } else {
                        false
                    };
                    publication_durable_for_thread.store(verified, std::sync::atomic::Ordering::SeqCst);
                }
                // A paused node session remains resumable: keep its prepared
                // environment and live binding. All terminal outcomes (and an
                // exec error) still get best-effort teardown.
                let durable = publication_durable_for_thread.load(std::sync::atomic::Ordering::SeqCst);
                let should_teardown = response
                    .as_ref()
                    .map(|response| {
                        session_should_teardown(
                            session_status_to_workflow_status(&response.status),
                            publication_required,
                            cleanup_policy,
                            durable,
                        )
                    })
                    .unwrap_or(false);
                if should_teardown {
                    match client.teardown(&handle).await {
                        Ok(()) => {
                            // TASK-811/933: node reaped -> flip the checkpoint binding
                            // torn_down so the reconciler does not try to re-reap a
                            // node that is already gone.
                            if let (Some(scoped_root), Some(phase_id)) =
                                (scoped_root_for_thread.as_deref(), binding_phase_id_for_thread.as_deref())
                            {
                                let _ = crate::phase_session::mark_environment_torn_down(
                                    scoped_root,
                                    &workflow_id_owned,
                                    phase_id,
                                );
                            }
                        }
                        Err(err) => {
                            // Teardown failed: leave the binding NOT torn_down so the
                            // daemon reconciler retries the reap on its next sweep.
                            eprintln!(
                                "warning: remote-animus session teardown failed for '{environment_id_owned}' (handle {}): {err:#}",
                                handle.id
                            );
                        }
                    }
                }
                response.map_err(|err| {
                    anyhow!("remote-animus session exec_session failed for '{environment_id_owned}': {err:#}")
                })
            })
        })();
        let _ = tx.send(result);
    });

    let response = rx.await.map_err(|_| anyhow!("remote-animus session thread terminated unexpectedly"))??;
    let publication_durable = publication_durable.load(Ordering::SeqCst);
    let verified_publication_receipt = publication_receipt.lock().ok().and_then(|guard| guard.clone());

    let node_workflow_status = session_status_to_workflow_status(&response.status);
    // A node saying "completed" is insufficient. The parent reports success
    // only when the session protocol also carried positive publication proof.
    let workflow_status = if node_workflow_status == WorkflowStatus::Completed && !publication_durable {
        WorkflowStatus::Failed
    } else {
        node_workflow_status
    };

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
        // REQ-052 one-id: the node journals INTO this shared row and terminalizes
        // it itself, so it is normally ALREADY terminal here. These are pure
        // BACKSTOPS for the crash/lag case where the node did NOT record its
        // terminal. CLI rc.30 removed `force_failed`; `fail_current_phase` is the
        // supported non-terminal transition and rejects a terminal row rather
        // than clobbering its richer status. An error is logged so a stuck row is
        // diagnosable instead of silently inviting orphan re-dispatch.
        WorkflowStatus::Failed | WorkflowStatus::Escalated => {
            if let Err(err) = hub
                .workflows()
                .fail_current_phase(workflow_id, format!("remote-animus session ended {}", response.status))
                .await
            {
                eprintln!(
                    "warning: remote-animus session backstop fail_current_phase('{workflow_id}') failed: {err:#} \
                     (shared row may remain Running; daemon reconciliation will retry)"
                );
            }
        }
        WorkflowStatus::Cancelled => {
            if let Err(err) = hub.workflows().cancel(workflow_id).await {
                eprintln!(
                    "warning: remote-animus session backstop cancel('{workflow_id}') failed: {err:#} \
                     (shared row may remain Running; daemon reconciliation will retry)"
                );
            }
        }
        WorkflowStatus::Paused => {
            if let Err(err) = hub.workflows().pause(workflow_id).await {
                eprintln!(
                    "warning: remote-animus session backstop pause('{workflow_id}') failed: {err:#} \
                     (shared row may remain Running; daemon reconciliation will retry)"
                );
            }
        }
        _ => {}
    }

    // TASK-933: close terminal checkpoints. Paused sessions intentionally remain
    // Running with a live binding so resume/reconciliation can reattach to the
    // prepared node rather than manufacturing a failed/completed checkpoint.
    if let (Some(scoped_root), Some(phase_id)) = (scoped_root_opt.as_deref(), binding_phase_id.as_deref()) {
        let _ =
            crate::phase_session::update_publication_durable(scoped_root, workflow_id, phase_id, publication_durable);
        let _ = finalize_session_checkpoint(scoped_root, workflow_id, phase_id, workflow_status, &response.status);
    }

    // Emit the single terminal workflow event home (the driver owns it; the
    // journal map deliberately does not forward node-level terminal events).
    if let Some(emitter) = event_emitter {
        match workflow_status {
            WorkflowStatus::Completed => emitter.emit(RuntimeWorkflowEvent {
                workflow_id: workflow_id.to_string(),
                kind: RuntimeWorkflowEventKind::WorkflowCompleted,
                payload: serde_json::json!({
                    "final_status": "completed",
                    "source": "environment_session",
                    "publication_durable": publication_durable,
                    "publication_receipt": verified_publication_receipt,
                }),
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
        success: session_succeeded(workflow_status),
        workflow_id: workflow_id.to_string(),
        execution_fence: execution_fence.cloned(),
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
            "status": if publication_durable { "completed" } else { "failed" },
            "publication_durable": publication_durable,
            "publication_receipt": verified_publication_receipt,
            "reason": "remote-animus session owns the full workflow (incl. post-success) on the node",
        }),
        publication_receipt: verified_publication_receipt,
    })
}

#[cfg(feature = "remote-animus-session")]
fn finalize_session_checkpoint(
    scoped_root: &Path,
    workflow_id: &str,
    phase_id: &str,
    workflow_status: WorkflowStatus,
    node_status: &str,
) -> std::io::Result<()> {
    match workflow_status {
        WorkflowStatus::Completed => crate::phase_session::update_session_completed(scoped_root, workflow_id, phase_id),
        // Paused is resumable, not terminal. Leave the checkpoint Running and
        // retain its non-torn-down environment binding for reattachment.
        WorkflowStatus::Paused => Ok(()),
        _ => crate::phase_session::update_session_failed(
            scoped_root,
            workflow_id,
            phase_id,
            &format!("remote-animus session ended {node_status}"),
        ),
    }
}

/// Persist the delegated node's [`EnvironmentBinding`](crate::phase_session::EnvironmentBinding)
/// into the run's session checkpoint at `phase_id` (the phase the rc.28
/// reconciler reads via `current_phase_id`). The node itself drives the phase,
/// so this checkpoint is purely the RECONCILE ORACLE: rc.28's `auto_resume`
/// SKIPS env-bound checkpoints (it never mis-resumes them as a local provider),
/// and its reconciler reads the binding here to reap a dead node / preserve a
/// live one / terminalize a ghost. Create-or-overwrite as `Running` (the run IS
/// in flight on the node). Best-effort — a write failure degrades restart
/// reconciliation (possible node leak) but never fails the delegated run.
#[cfg(feature = "remote-animus-session")]
fn persist_session_binding(
    scoped_root: &Path,
    workflow_id: &str,
    phase_id: &str,
    environment_id: &str,
    handle: &animus_environment_protocol::EnvironmentHandle,
) {
    use crate::phase_session::{
        update_session_environment, update_session_running, write_session_pending, EnvironmentBinding,
    };

    // `run_id` is the workflow id (the REQ-052 one-id identity); `provider` names
    // the environment so the synthetic checkpoint is self-describing.
    if let Err(err) = write_session_pending(scoped_root, workflow_id, phase_id, environment_id, workflow_id, None) {
        eprintln!("warning: failed to seed remote-animus session checkpoint for {workflow_id}/{phase_id}: {err}");
        return;
    }
    let _ = update_session_running(scoped_root, workflow_id, phase_id);
    let binding = EnvironmentBinding {
        environment_id: environment_id.to_string(),
        handle: handle.clone(),
        bound_at: chrono::Utc::now().to_rfc3339(),
        torn_down: false,
    };
    if let Err(err) = update_session_environment(scoped_root, workflow_id, phase_id, binding) {
        eprintln!("warning: failed to persist remote-animus env binding for {workflow_id}/{phase_id}: {err}");
    }
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

    #[cfg(feature = "remote-animus-session")]
    fn test_execution(repository: &str) -> ExecutionFence {
        let mut execution = ExecutionFence::direct(
            "run",
            1,
            Some(animus_execution_protocol::SubjectGeneration {
                qualified_id: "task:TASK-SESSION".to_string(),
                generation: 1,
            }),
        );
        execution.repository = Some(animus_execution_protocol::RepositoryReservation {
            repository: repository.to_string(),
            base_ref: "refs/heads/main".to_string(),
            head_ref: "refs/heads/reviewed".to_string(),
        });
        execution
    }

    #[cfg(feature = "remote-animus-session")]
    fn test_publication_receipt(repository: &str, commit: &str, tree: &str) -> PublicationReceipt {
        PublicationReceipt {
            schema: animus_workflow_runner_protocol::PUBLICATION_RECEIPT_SCHEMA_ID.to_string(),
            version: animus_workflow_runner_protocol::PUBLICATION_RECEIPT_VERSION,
            workflow_id: "run".to_string(),
            workflow_generation: 1,
            subject: animus_workflow_runner_protocol::PublicationSubjectGeneration {
                qualified_id: "task:TASK-SESSION".to_string(),
                generation: 1,
            },
            commit_sha: commit.to_string(),
            tree_sha: tree.to_string(),
            remote: repository.to_string(),
            remote_ref: "refs/heads/reviewed".to_string(),
            observed_remote_sha: commit.to_string(),
            recovery_ref: "refs/heads/reviewed".to_string(),
            pull_request: None,
            issuer: animus_workflow_runner_protocol::PublicationReceiptIssuer::Phase {
                phase_id: "publish".to_string(),
                component: "session-test-publisher".to_string(),
                version: "1.0.0".to_string(),
            },
            issued_at: chrono::Utc::now(),
        }
    }

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

    #[cfg(feature = "remote-animus-session")]
    #[test]
    fn rc12_session_journal_notification_decodes_and_filters_by_handle() {
        use std::sync::Mutex;

        use animus_environment_protocol::NOTIFICATION_ENVIRONMENT_JOURNAL;
        use animus_plugin_protocol::RpcNotification;

        let received = Mutex::new(Vec::new());
        let notification = RpcNotification::new(
            NOTIFICATION_ENVIRONMENT_JOURNAL,
            Some(serde_json::json!({
                "handle_id": "node-1",
                "workflow_id": "wf-node",
                "event_kind": "phase_completed",
                "phase_id": "implementation",
                "status": "completed",
                "ts": "2026-07-26T00:00:00Z",
                "payload": { "run_id": "wf-wf-node-implementation" },
                "terminal": false
            })),
        );

        forward_session_journal(&notification, "other-node", &|event| {
            received.lock().expect("lock").push(event.event_kind.clone());
        });
        assert!(received.lock().expect("lock").is_empty());

        forward_session_journal(&notification, "node-1", &|event| {
            received.lock().expect("lock").push(event.event_kind.clone());
        });
        assert_eq!(received.lock().expect("lock").as_slice(), ["phase_completed"]);
    }

    #[cfg(feature = "remote-animus-session")]
    #[test]
    fn prepare_and_exec_session_requests_carry_the_same_top_level_actor() {
        use animus_environment_protocol::{EnvironmentHandle, ExecSessionRequest, PrepareRequest};

        let actor = Actor {
            user_id: "user-42".to_string(),
            claims: vec!["admin".to_string()],
            tenant_id: Some("tenant-7".to_string()),
        };
        let actor_value = serde_json::to_value(&actor).expect("serialize actor");
        let prepare = request_params_with_actor(
            PrepareRequest { spec: build_session_spec("railway", None) },
            Some(&actor),
            "serialize prepare",
        )
        .expect("actor-bound prepare params");
        let exec = request_params_with_actor(
            ExecSessionRequest {
                handle: EnvironmentHandle {
                    id: "node-1".to_string(),
                    workspace_root: "/work".to_string(),
                    metadata: serde_json::Value::Null,
                },
                subject_id: "task:TASK-1".to_string(),
                workflow_ref: Some("coding".to_string()),
                dispatch_input: None,
                workflow_id: Some("wf-1".to_string()),
                execution_fence: None,
            },
            Some(&actor),
            "serialize exec_session",
        )
        .expect("actor-bound exec_session params");

        assert_eq!(prepare.get("actor"), Some(&actor_value));
        assert_eq!(exec.get("actor"), Some(&actor_value));
        assert_eq!(exec.pointer("/actor/user_id").and_then(|value| value.as_str()), Some("user-42"));
    }

    #[cfg(feature = "remote-animus-session")]
    #[test]
    fn system_session_request_omits_actor_field() {
        use animus_environment_protocol::PrepareRequest;

        let prepare = request_params_with_actor(
            PrepareRequest { spec: build_session_spec("railway", None) },
            None,
            "serialize prepare",
        )
        .expect("system prepare params");

        assert!(prepare.get("actor").is_none(), "None must preserve the legacy system-scoped wire shape");
    }

    #[cfg(feature = "remote-animus-session")]
    #[tokio::test]
    async fn death_like_session_host_is_invalidated_before_next_reacquire() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        use orchestrator_plugin_host::resident_host_registry::install_resident_host_registry_for_test;
        use orchestrator_plugin_host::{DiscoveredPlugin, DiscoverySource, PluginHost};

        fn disconnected_host() -> PluginHost {
            let (host_reader, plugin_writer) = tokio::io::duplex(256);
            let (plugin_reader, host_writer) = tokio::io::duplex(256);
            drop(plugin_writer);
            drop(plugin_reader);
            PluginHost::from_streams("dead-session-environment", host_reader, host_writer)
        }

        let registry = install_resident_host_registry_for_test(4);
        let tmp = tempfile::tempdir().expect("tempdir");
        let plugin_path = tmp.path().join("fake-environment");
        std::fs::write(&plugin_path, b"fake").expect("fake plugin path");
        let key = SessionRegistryKey {
            plugin_path: plugin_path.clone(),
            binary_mtime: 42,
            spawn_context: "session-death-regression".to_string(),
        };
        let spawn_count = Arc::new(AtomicUsize::new(0));
        let first_count = spawn_count.clone();
        let lease = registry
            .get_or_spawn(&plugin_path, key.binary_mtime, &key.spawn_context, || async move {
                first_count.fetch_add(1, Ordering::SeqCst);
                Ok(disconnected_host())
            })
            .await
            .expect("first lease");
        let dead_generation = lease.generation();
        let client = SessionEnvironmentClient {
            plugin_name: "fake-environment".to_string(),
            plugin: DiscoveredPlugin {
                name: "fake-environment".to_string(),
                path: plugin_path.clone(),
                manifest: crate::plugin::plugin_manifest(),
                source: DiscoverySource::ExplicitConfig,
            },
            forwarded_env: Vec::new(),
            registry_key: key.clone(),
            pinned: tokio::sync::Mutex::new(Some(PinnedSessionLease { lease, generation: dead_generation })),
        };
        tokio::task::yield_now().await;

        let handle = animus_environment_protocol::EnvironmentHandle {
            id: "dead-handle".to_string(),
            workspace_root: "/work".to_string(),
            metadata: serde_json::Value::Null,
        };
        assert!(!client.probe(&handle).await, "dead host probe must fail");
        assert!(
            !registry.contains(&plugin_path, key.binary_mtime, &key.spawn_context),
            "death-like failure must remove exactly the cached dead generation"
        );

        let second_count = spawn_count.clone();
        let replacement = registry
            .get_or_spawn(&plugin_path, key.binary_mtime, &key.spawn_context, || async move {
                second_count.fetch_add(1, Ordering::SeqCst);
                Ok(disconnected_host())
            })
            .await
            .expect("replacement lease");
        assert_ne!(replacement.generation(), dead_generation, "reacquire must install a fresh generation");
        assert_eq!(spawn_count.load(Ordering::SeqCst), 2, "the dead cached host was not reused");
        drop(replacement);
        registry.shutdown_all().await;
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
        for kind in [
            "output_chunk",
            "tool_call",
            "tool_result",
            "thinking",
            "started",
            "finished",
            "metadata",
            "error",
            "artifact",
        ] {
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

    #[test]
    fn only_durably_completed_or_explicitly_cancelled_session_is_torn_down() {
        assert!(session_succeeded(WorkflowStatus::Completed));
        for status in
            [WorkflowStatus::Paused, WorkflowStatus::Failed, WorkflowStatus::Escalated, WorkflowStatus::Cancelled]
        {
            assert!(!session_succeeded(status), "{status:?} must not be reported as success");
        }
        assert!(!session_should_teardown(
            WorkflowStatus::Paused,
            true,
            WorkflowPublicationCleanupPolicy::AfterRemoteVerified,
            true,
        ));
        assert!(session_should_teardown(
            WorkflowStatus::Completed,
            true,
            WorkflowPublicationCleanupPolicy::AfterRemoteVerified,
            true,
        ));
        assert!(!session_should_teardown(
            WorkflowStatus::Completed,
            true,
            WorkflowPublicationCleanupPolicy::Retain,
            true,
        ));
        assert!(session_should_teardown(
            WorkflowStatus::Completed,
            false,
            WorkflowPublicationCleanupPolicy::Retain,
            false,
        ));
        assert!(!session_should_teardown(
            WorkflowStatus::Failed,
            false,
            WorkflowPublicationCleanupPolicy::AfterRemoteVerified,
            false,
        ));
        assert!(
            session_should_teardown(WorkflowStatus::Cancelled, true, WorkflowPublicationCleanupPolicy::Retain, false,),
            "explicit cancellation is the operator escape hatch for a held unpublished environment"
        );
    }

    #[test]
    #[cfg(feature = "remote-animus-session")]
    fn publication_receipt_only_comes_from_matching_terminal_completion() {
        let receipt =
            test_publication_receipt("https://github.com/launchapp-dev/example.git", &"1".repeat(40), &"2".repeat(40));
        let event = |event_kind: &str, terminal: bool, workflow_id: &str| SessionJournalEvent {
            handle_id: "node".into(),
            workflow_id: Some(workflow_id.into()),
            event_kind: event_kind.into(),
            phase_id: None,
            status: None,
            ts: String::new(),
            payload: serde_json::json!({"publication_receipt": receipt.clone()}),
            terminal,
        };
        assert!(terminal_publication_receipt(&event("phase_completed", true, "run"), "run").is_none());
        assert!(terminal_publication_receipt(&event("workflow_completed", false, "run"), "run").is_none());
        assert!(terminal_publication_receipt(&event("workflow_completed", true, "other"), "run").is_none());
        assert!(terminal_publication_receipt(&event("workflow_completed", true, "run"), "run").is_some());
    }

    #[cfg(feature = "remote-animus-session")]
    #[test]
    fn session_response_must_echo_the_exact_execution_generation() {
        let expected = test_execution("https://github.com/launchapp-dev/example.git");
        let response = |execution_fence| animus_environment_protocol::ExecSessionResponse {
            workflow_id: Some("run".to_string()),
            execution_fence,
            status: "completed".to_string(),
        };

        assert!(validate_session_response_fence(&response(Some(expected.clone())), Some(&expected), true).is_ok());
        assert!(validate_session_response_fence(&response(None), Some(&expected), true).is_err());

        let mut stale = expected.clone();
        stale.workflow_generation += 1;
        assert!(validate_session_response_fence(&response(Some(stale)), Some(&expected), true).is_err());
        assert!(validate_session_response_fence(&response(Some(expected)), None, true).is_err());
        assert!(validate_session_response_fence(&response(None), None, false).is_ok());
    }

    #[cfg(feature = "remote-animus-session")]
    #[tokio::test]
    async fn session_proof_is_verified_in_node_checkout_before_exactly_one_teardown() {
        use std::process::Command;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct NodeGit {
            cwd: std::path::PathBuf,
        }

        #[async_trait::async_trait]
        impl SessionPublicationCommands for NodeGit {
            async fn run_git(&self, args: Vec<String>) -> Result<crate::phase_git::PublicationCommandOutput> {
                let output = Command::new("git").arg("-C").arg(&self.cwd).args(args).output()?;
                Ok(crate::phase_git::PublicationCommandOutput {
                    success: output.status.success(),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                })
            }
        }

        fn git(cwd: &Path, args: &[&str]) {
            assert!(Command::new("git").arg("-C").arg(cwd).args(args).status().unwrap().success());
        }

        let root = tempfile::tempdir().unwrap();
        let host = root.path().join("host-without-checkout");
        let remote = root.path().join("remote.git");
        let node = root.path().join("node-checkout");
        std::fs::create_dir(&host).unwrap();
        git(root.path(), &["init", "--bare", remote.to_str().unwrap()]);
        git(root.path(), &["init", "-b", "reviewed", node.to_str().unwrap()]);
        git(&node, &["config", "user.name", "Node"]);
        git(&node, &["config", "user.email", "node@example.invalid"]);
        std::fs::write(node.join("reviewed.txt"), "reviewed\n").unwrap();
        git(&node, &["add", "."]);
        git(&node, &["commit", "-m", "reviewed"]);
        git(&node, &["remote", "add", "origin", remote.to_str().unwrap()]);
        git(&node, &["push", "origin", "HEAD:refs/heads/reviewed"]);

        assert!(!crate::phase_git::is_git_repo(host.to_str().unwrap()));
        let commit = String::from_utf8(
            Command::new("git")
                .args(["-C", node.to_str().unwrap(), "rev-parse", "HEAD^{commit}"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        let tree = String::from_utf8(
            Command::new("git")
                .args(["-C", node.to_str().unwrap(), "rev-parse", "HEAD^{tree}"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        let execution = test_execution(remote.to_str().unwrap());
        let receipt = test_publication_receipt(remote.to_str().unwrap(), &commit, &tree);
        let executor = NodeGit { cwd: node };

        let verified = verify_session_publication(&executor, &receipt, &execution).await.unwrap();
        let teardowns = AtomicUsize::new(0);
        if session_should_teardown(
            WorkflowStatus::Completed,
            true,
            WorkflowPublicationCleanupPolicy::AfterRemoteVerified,
            verified,
        ) {
            teardowns.fetch_add(1, Ordering::SeqCst);
        }
        assert!(verified);
        assert_eq!(teardowns.load(Ordering::SeqCst), 1);

        let mut forged = receipt;
        forged.tree_sha = "0".repeat(40);
        let forged_verified = verify_session_publication(&executor, &forged, &execution).await.unwrap();
        let forged_teardowns = AtomicUsize::new(0);
        if session_should_teardown(
            WorkflowStatus::Completed,
            true,
            WorkflowPublicationCleanupPolicy::AfterRemoteVerified,
            forged_verified,
        ) {
            forged_teardowns.fetch_add(1, Ordering::SeqCst);
        }
        assert!(!forged_verified);
        assert_eq!(forged_teardowns.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "remote-animus-session")]
    #[test]
    fn paused_session_keeps_running_checkpoint_and_live_binding() {
        use crate::phase_session::{read_checkpoint, SessionCheckpointStatus};

        let tmp = tempfile::tempdir().expect("tempdir");
        let handle = animus_environment_protocol::EnvironmentHandle {
            id: "paused-node".to_string(),
            workspace_root: "/work".to_string(),
            metadata: serde_json::json!({ "resume": true }),
        };
        persist_session_binding(tmp.path(), "wf-paused", "implementation", "fake-environment", &handle);
        finalize_session_checkpoint(tmp.path(), "wf-paused", "implementation", WorkflowStatus::Paused, "paused")
            .expect("paused checkpoint finalization");

        let checkpoint = read_checkpoint(tmp.path(), "wf-paused", "implementation").expect("read").expect("checkpoint");
        assert_eq!(checkpoint.status, SessionCheckpointStatus::Running);
        assert!(checkpoint.completed_at.is_none());
        assert!(checkpoint.blocked_reason.is_none());
        let binding = checkpoint.environment.expect("live environment binding");
        assert_eq!(binding.handle.id, "paused-node");
        assert!(!binding.torn_down, "paused node must remain available for resume");
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

    // TASK-933: the remote-animus session path persists the node binding into the
    // run's session checkpoint at the reconciler's phase, as a Running checkpoint
    // carrying exactly rc.28's EnvironmentBinding shape. This is THE write the
    // released daemon reconciler reads to reap/reattach the node by handle.
    #[cfg(feature = "remote-animus-session")]
    #[test]
    fn persist_session_binding_writes_running_checkpoint_with_the_rc28_shape() {
        use crate::phase_session::{read_checkpoint, SessionCheckpointStatus};

        let tmp = tempfile::tempdir().expect("tempdir");
        let scoped_root = tmp.path();
        let handle = animus_environment_protocol::EnvironmentHandle {
            id: "node-xyz".to_string(),
            workspace_root: "/work".to_string(),
            metadata: serde_json::json!({ "railway_service_id": "svc-9" }),
        };

        persist_session_binding(scoped_root, "wf-sess-1", "implementation", "animus-environment-railway", &handle);

        let cp = read_checkpoint(scoped_root, "wf-sess-1", "implementation").expect("read").expect("present");
        assert_eq!(cp.status, SessionCheckpointStatus::Running, "the delegated run is in flight on the node");
        let binding = cp.environment.expect("binding persisted for the reconciler");
        assert_eq!(binding.environment_id, "animus-environment-railway");
        assert_eq!(binding.handle.id, "node-xyz");
        assert_eq!(
            binding.handle.metadata.pointer("/railway_service_id").and_then(|v| v.as_str()),
            Some("svc-9"),
            "opaque handle metadata is preserved for teardown-by-handle"
        );
        assert!(!binding.torn_down, "a freshly persisted binding is live");

        // Teardown-success path: the reconciler must not re-reap a gone node.
        crate::phase_session::mark_environment_torn_down(scoped_root, "wf-sess-1", "implementation").expect("mark");
        let cp = read_checkpoint(scoped_root, "wf-sess-1", "implementation").expect("read").expect("present");
        assert!(cp.environment.expect("binding").torn_down, "teardown flips torn_down");
    }
}
