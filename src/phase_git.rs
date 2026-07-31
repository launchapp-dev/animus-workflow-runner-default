use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Output, Stdio};
use std::sync::OnceLock;

fn git_status(cwd: &str, args: &[&str], operation: &str) -> Result<()> {
    let status = ProcessCommand::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("failed to run git operation '{operation}' in {}", cwd))?;
    if !status.success() {
        anyhow::bail!("git operation '{}' failed in {}: git {}", operation, cwd, args.join(" "));
    }
    Ok(())
}

pub fn is_git_repo(project_root: &str) -> bool {
    ProcessCommand::new("git")
        .arg("-C")
        .arg(project_root)
        .args(["rev-parse", "--is-inside-work-tree"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

pub fn git_has_pending_changes(cwd: &str) -> Result<bool> {
    let output = ProcessCommand::new("git")
        .arg("-C")
        .arg(cwd)
        .args(["status", "--porcelain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .with_context(|| format!("failed to inspect git status in {}", cwd))?;

    if !output.status.success() {
        anyhow::bail!("git status --porcelain failed in {}", cwd);
    }

    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

pub fn ensure_git_identity(cwd: &str) -> Result<()> {
    let email = format!("{}@local", protocol::ACTOR_DAEMON);
    for (key, default_value) in [("user.name", "Animus Daemon"), ("user.email", email.as_str())] {
        let output = ProcessCommand::new("git")
            .arg("-C")
            .arg(cwd)
            .args(["config", "--get", key])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .with_context(|| format!("failed to read git config {} in {}", key, cwd))?;

        let configured = output.status.success() && !String::from_utf8_lossy(&output.stdout).trim().is_empty();
        if !configured {
            git_status(cwd, &["config", key, default_value], "configure git identity")?;
        }
    }

    Ok(())
}

pub fn commit_implementation_changes(cwd: &str, commit_message: &str) -> Result<()> {
    // Do NOT force a commit or require a git repo. Portal-harness / orchestration
    // agent phases run at a non-repo project root (e.g. /app) and edit no files;
    // failing them here ("requires a git repository for commit") was wrong. Coding
    // phases run in a cloned repo and their `code-open-pr` phase commits + pushes
    // explicitly, so this auto-commit is only a convenience for the in-repo case.
    // No repo, or no pending changes -> nothing to do (no-op, not an error).
    if !is_git_repo(cwd) {
        tracing::debug!(cwd, "commit skipped — not a git repository (non-coding phase)");
        return Ok(());
    }
    if !git_has_pending_changes(cwd)? {
        tracing::info!(cwd, "No pending changes to commit — agent likely already committed");
        return Ok(());
    }

    let commit_message = commit_message.trim();
    if commit_message.is_empty() {
        anyhow::bail!("implementation phase requires a non-empty commit message");
    }
    ensure_git_identity(cwd)?;
    git_status(cwd, &["add", "-A"], "stage implementation changes")?;
    git_status(cwd, &["commit", "-m", commit_message], "commit implementation changes")?;
    Ok(())
}

/// Durable evidence produced by the publication gate.  A workflow may only be
/// reported successful (and its execution environment released) when
/// `remote_ref` is present.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicationProof {
    pub commit: String,
    pub tree: String,
    pub remote: String,
    pub remote_ref: Option<String>,
    pub recovery_ref: Option<String>,
    pub bundle_path: Option<PathBuf>,
    pub diagnostic: Option<String>,
}

impl PublicationProof {
    pub fn is_durable(&self) -> bool {
        self.remote_ref.is_some()
    }
}

/// Transport-neutral result for a Git command. Environment-routed publication
/// uses this shape so the publication protocol can run in the checkout's node
/// without depending on a host-local repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationCommandOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub(crate) fn proof_shape_is_valid(proof: &PublicationProof) -> bool {
    let Some(remote_ref) = proof.remote_ref.as_deref() else {
        return false;
    };
    proof.remote == "origin"
        && remote_ref.starts_with("refs/heads/")
        && proof.commit.len() == 40
        && proof.tree.len() == 40
        && proof.commit.bytes().all(|byte| byte.is_ascii_hexdigit())
        && proof.tree.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn command_text<F>(run_git: &mut F, args: Vec<String>, operation: &str) -> Result<String>
where
    F: FnMut(Vec<String>) -> Result<PublicationCommandOutput>,
{
    let output = run_git(args)?;
    if !output.success {
        anyhow::bail!("{operation} failed: {}", redact_git_diagnostic(&output.stderr));
    }
    Ok(output.stdout.trim().to_string())
}

/// Independently verify a terminal publication proof using Git commands in the
/// same checkout/environment that produced it. Fetching the claimed remote ref
/// makes the reachability and tree checks server-backed instead of trusting the
/// serialized journal payload or an unrelated host object database.
pub(crate) fn verify_publication_proof_with<F>(proof: &PublicationProof, mut run_git: F) -> Result<bool>
where
    F: FnMut(Vec<String>) -> Result<PublicationCommandOutput>,
{
    if !proof_shape_is_valid(proof) {
        return Ok(false);
    }
    let remote_ref = proof.remote_ref.as_deref().expect("shape checked remote ref");
    let verify_ref = format!("refs/animus/proof/{}", &proof.commit[..12]);
    let refspec = format!("+{remote_ref}:{verify_ref}");
    let fetched = run_git(vec!["fetch".into(), "--no-tags".into(), proof.remote.clone(), refspec])?;
    if !fetched.success {
        return Ok(false);
    }

    let ancestor =
        run_git(vec!["merge-base".into(), "--is-ancestor".into(), proof.commit.clone(), verify_ref.clone()])?.success;
    let actual_commit = command_text(
        &mut run_git,
        vec!["rev-parse".into(), format!("{}^{{commit}}", proof.commit)],
        "verify publication commit",
    );
    let actual_tree = command_text(
        &mut run_git,
        vec!["rev-parse".into(), format!("{}^{{tree}}", proof.commit)],
        "verify publication tree",
    );
    let _ = run_git(vec!["update-ref".into(), "-d".into(), verify_ref]);

    Ok(ancestor
        && matches!(
            (actual_commit, actual_tree),
            (Ok(actual_commit), Ok(actual_tree))
                if actual_commit == proof.commit && actual_tree == proof.tree
        ))
}

/// Publish a clean committed checkout through an arbitrary Git command
/// transport. This is the environment-safe counterpart to
/// [`publish_head_durably`]. If both server refs reject the commit, the caller
/// must retain the environment; no host-local fallback can preserve node-only
/// objects.
pub(crate) fn publish_head_durably_with<F>(
    remote: &str,
    branch: &str,
    run_id: &str,
    mut run_git: F,
) -> Result<PublicationProof>
where
    F: FnMut(Vec<String>) -> Result<PublicationCommandOutput>,
{
    let inside = command_text(
        &mut run_git,
        vec!["rev-parse".into(), "--is-inside-work-tree".into()],
        "detect publication repository",
    )?;
    if inside != "true" {
        anyhow::bail!("publication requires a git repository");
    }
    let pending =
        command_text(&mut run_git, vec!["status".into(), "--porcelain".into()], "inspect publication worktree")?;
    if !pending.is_empty() {
        anyhow::bail!("publication requires a clean committed worktree");
    }

    let commit =
        command_text(&mut run_git, vec!["rev-parse".into(), "HEAD^{commit}".into()], "resolve publication commit")?;
    let tree = command_text(&mut run_git, vec!["rev-parse".into(), "HEAD^{tree}".into()], "resolve publication tree")?;
    let branch = sanitize_branch(branch);
    if branch.is_empty() {
        anyhow::bail!("publication branch is empty or invalid");
    }

    let target_ref = format!("refs/heads/{branch}");
    let push = run_git(vec!["push".into(), remote.into(), format!("{commit}:{target_ref}")])?;
    let target_proof = PublicationProof {
        commit: commit.clone(),
        tree: tree.clone(),
        remote: remote.to_string(),
        remote_ref: Some(target_ref),
        recovery_ref: None,
        bundle_path: None,
        diagnostic: (!push.success).then(|| "concurrent publication already installed the same commit".to_string()),
    };
    if verify_publication_proof_with(&target_proof, &mut run_git).unwrap_or(false) {
        return Ok(target_proof);
    }

    let short = &commit[..12.min(commit.len())];
    let run = sanitize_ref_component(run_id);
    let recovery_ref = format!("refs/heads/animus/recovery/{branch}-{run}-{short}");
    let recovery = run_git(vec!["push".into(), remote.into(), format!("{commit}:{recovery_ref}")])?;
    let recovery_proof = PublicationProof {
        commit,
        tree,
        remote: remote.to_string(),
        remote_ref: Some(recovery_ref.clone()),
        recovery_ref: Some(recovery_ref),
        bundle_path: None,
        diagnostic: Some(format!(
            "target branch changed concurrently; exact reviewed commit preserved on a recovery ref ({})",
            redact_git_diagnostic(&push.stderr).trim()
        )),
    };
    if recovery.success && verify_publication_proof_with(&recovery_proof, &mut run_git).unwrap_or(false) {
        return Ok(recovery_proof);
    }

    Ok(PublicationProof {
        remote_ref: None,
        recovery_ref: None,
        diagnostic: Some(format!(
            "publication and recovery-ref push failed: {}; recovery: {}; environment retained",
            redact_git_diagnostic(&push.stderr).trim(),
            redact_git_diagnostic(&recovery.stderr).trim()
        )),
        ..recovery_proof
    })
}

fn git_output(cwd: &str, args: &[&str]) -> Result<Output> {
    ProcessCommand::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .with_context(|| format!("failed to run git in {}", cwd))
}

fn git_text(cwd: &str, args: &[&str], operation: &str) -> Result<String> {
    let output = git_output(cwd, args)?;
    if !output.status.success() {
        anyhow::bail!("{operation} failed: {}", redact_git_diagnostic(&String::from_utf8_lossy(&output.stderr)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Remove credentials which Git may echo as part of an HTTP remote URL.
pub fn redact_git_diagnostic(input: &str) -> String {
    static URL_CREDENTIALS: OnceLock<regex::Regex> = OnceLock::new();
    static AUTHORIZATION_HEADER: OnceLock<regex::Regex> = OnceLock::new();
    static NAMED_SECRETS: OnceLock<regex::Regex> = OnceLock::new();

    let mut value = input.to_string();
    // URL userinfo (including GitHub tokens) is the most common leak.
    let url_credentials =
        URL_CREDENTIALS.get_or_init(|| regex::Regex::new(r"(?i)(https?://)[^/@\s]+@").expect("valid regex"));
    value = url_credentials.replace_all(&value, "${1}[REDACTED]@").into_owned();

    // Authorization headers commonly contain a scheme and then the actual
    // credential. Redact both so `Authorization: Bearer secret` cannot leave
    // `secret` behind after replacing only the first non-whitespace token.
    let authorization_header = AUTHORIZATION_HEADER
        .get_or_init(|| regex::Regex::new(r"(?i)\bauthorization\s*[:=]\s*[^\r\n]+").expect("valid regex"));
    value = authorization_header.replace_all(&value, "authorization=[REDACTED]").into_owned();

    // Also cover token-bearing environment-style fragments emitted by helpers.
    let named_secrets = NAMED_SECRETS
        .get_or_init(|| regex::Regex::new(r"(?i)\b(token|password|oauth)[=:]\s*[^\s]+").expect("valid regex"));
    value = named_secrets.replace_all(&value, "$1=[REDACTED]").into_owned();
    value
}

/// Whether a failed Git publication was rejected by a server-side
/// authorization or repository policy that requires operator action.
///
/// This deliberately excludes non-fast-forward and transport failures: those
/// retain the existing recovery-ref and retry behavior. Callers must also
/// scope this classifier to a publication phase so an unrelated command that
/// happens to contain one of these phrases is not paused.
pub(crate) fn is_non_retryable_publication_denial(input: &str) -> bool {
    let message = input.to_ascii_lowercase();
    [
        "refusing to allow a github app to create or update workflow",
        "workflows permission",
        "resource not accessible by integration",
        "permission to ",
        "protected branch hook declined",
        "protected branch update failed",
        "repository rule violations",
        "push declined due to repository rule",
        "changes must be made through a pull request",
        "not allowed to push",
        "insufficient permission",
        "insufficient scope",
        "write access to repository not granted",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

pub(crate) fn publication_denial_escalation(diagnostic: &str, commit: Option<&str>, tree: Option<&str>) -> String {
    let diagnostic = redact_git_diagnostic(diagnostic);
    let commit = commit.filter(|value| !value.trim().is_empty()).unwrap_or("unavailable");
    let tree = tree.filter(|value| !value.trim().is_empty()).unwrap_or("unavailable");
    format!(
        "Git publication is blocked by a non-retryable authorization or repository-policy denial. \
         The execution environment is retained and no code rework was consumed. \
         Unpublished commit: {commit}; tree: {tree}. \
         Redacted remote diagnostic: {diagnostic}. \
         Remediation: grant the publishing GitHub App/token permission to update the rejected ref \
         (including Actions workflows permission when workflow files changed), or adjust the \
         repository/branch policy, then explicitly resume publication. The environment must remain \
         held until durable publication or explicit workflow cancellation."
    )
}

fn sanitize_ref_component(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') { ch } else { '-' })
        .collect();
    cleaned.trim_matches(['-', '.']).chars().take(80).collect()
}

fn sanitize_branch(value: &str) -> String {
    value.split('/').map(sanitize_ref_component).filter(|component| !component.is_empty()).collect::<Vec<_>>().join("/")
}

fn remote_ref_reaches(cwd: &str, remote: &str, remote_ref: &str, commit: &str) -> Result<bool> {
    let output = git_output(cwd, &["ls-remote", "--refs", remote, remote_ref])?;
    if !output.status.success() {
        anyhow::bail!(
            "remote reachability query failed: {}",
            redact_git_diagnostic(&String::from_utf8_lossy(&output.stderr))
        );
    }
    let Some(remote_tip) = String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.split_whitespace().next().map(str::to_string))
    else {
        return Ok(false);
    };
    if remote_tip == commit {
        return Ok(true);
    }

    // The branch may advance immediately after our push. Fetch its tip into an
    // isolated ref and prove that the reviewed commit remains an ancestor.
    let verify_ref = format!("refs/animus/verify/{}", &commit[..12.min(commit.len())]);
    let refspec = format!("+{remote_ref}:{verify_ref}");
    let fetched = git_output(cwd, &["fetch", "--no-tags", remote, &refspec])?;
    if !fetched.status.success() {
        return Ok(false);
    }
    let ancestor = git_output(cwd, &["merge-base", "--is-ancestor", commit, &verify_ref])?.status.success();
    let _ = git_output(cwd, &["update-ref", "-d", &verify_ref]);
    Ok(ancestor)
}

/// Re-check a serialized publication proof against the remote server. This is
/// used by a delegating runner: journal payloads are transport data, not proof,
/// until Git independently confirms both the reachable commit and its tree.
pub fn verify_publication_proof(cwd: &str, proof: &PublicationProof) -> Result<bool> {
    let Some(remote_ref) = proof.remote_ref.as_deref() else {
        return Ok(false);
    };
    if proof.remote != "origin"
        || !remote_ref.starts_with("refs/heads/")
        || proof.commit.len() != 40
        || proof.tree.len() != 40
        || !proof.commit.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !proof.tree.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(false);
    }
    if !remote_ref_reaches(cwd, &proof.remote, remote_ref, &proof.commit)? {
        return Ok(false);
    }

    let verify_ref = format!("refs/animus/proof/{}", &proof.commit[..12]);
    let refspec = format!("+{remote_ref}:{verify_ref}");
    let fetched = git_output(cwd, &["fetch", "--no-tags", &proof.remote, &refspec])?;
    if !fetched.status.success() {
        return Ok(false);
    }
    // Resolve both identities from the object fetched from the claimed remote
    // ref. Do not let an unrelated, pre-existing local object satisfy proof
    // verification merely because it has the claimed object id.
    let actual_commit = git_text(cwd, &["rev-parse", &format!("{verify_ref}^{{commit}}")], "verify publication commit");
    let actual_tree = git_text(cwd, &["rev-parse", &format!("{verify_ref}^{{tree}}")], "verify publication tree");
    let _ = git_output(cwd, &["update-ref", "-d", &verify_ref]);
    Ok(matches!(
        (actual_commit, actual_tree),
        (Ok(actual_commit), Ok(actual_tree))
            if actual_commit == proof.commit && actual_tree == proof.tree
    ))
}

fn write_unpublished_bundle(cwd: &str, commit: &str, durable_root: &Path) -> Result<PathBuf> {
    // This directory belongs to the home runner, not to the checkout (which may
    // live on a disposable environment). A bundle in `.git` disappears with
    // precisely the node whose publication failure we are trying to survive.
    let directory = durable_root.join("unpublished-git");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{}.bundle", &commit[..12.min(commit.len())]));
    let path_text = path.to_string_lossy().to_string();
    // `git bundle create <path> <raw-sha>` may select no named refs and reject
    // the bundle as empty. Pin the commit under a private ref for the duration
    // of bundle creation so the artifact is independently cloneable.
    let bundle_ref = format!("refs/animus/unpublished/{}", &commit[..12.min(commit.len())]);
    git_text(cwd, &["update-ref", &bundle_ref, commit], "pin unpublished commit")?;
    let output = git_output(cwd, &["bundle", "create", &path_text, &bundle_ref])?;
    let _ = git_output(cwd, &["update-ref", "-d", &bundle_ref]);
    if !output.status.success() {
        anyhow::bail!(
            "failed to preserve unpublished commit: {}",
            redact_git_diagnostic(&String::from_utf8_lossy(&output.stderr))
        );
    }
    Ok(path)
}

/// Publish HEAD without ever force-updating another run's work.
///
/// A rejected same-head push is accepted only after `ls-remote` proves the
/// exact commit is the remote tip.  A divergent non-fast-forward collision is
/// preserved under a unique recovery ref.  If even that push fails, a bundle
/// is written inside the repository's git directory and the result fails
/// closed.
pub fn publish_head_durably(
    cwd: &str,
    remote: &str,
    branch: &str,
    run_id: &str,
    durable_root: &Path,
) -> Result<PublicationProof> {
    if !is_git_repo(cwd) {
        anyhow::bail!("publication requires a git repository");
    }
    if git_has_pending_changes(cwd)? {
        anyhow::bail!("publication requires a clean committed worktree");
    }
    let commit = git_text(cwd, &["rev-parse", "HEAD^{commit}"], "resolve publication commit")?;
    let tree = git_text(cwd, &["rev-parse", "HEAD^{tree}"], "resolve publication tree")?;
    let branch = sanitize_branch(branch);
    if branch.is_empty() {
        anyhow::bail!("publication branch is empty or invalid");
    }
    let target_ref = format!("refs/heads/{branch}");
    let refspec = format!("{commit}:{target_ref}");
    let push = git_output(cwd, &["push", remote, &refspec])?;

    // Do not trust a zero exit status alone: query the server for durable proof.
    if remote_ref_reaches(cwd, remote, &target_ref, &commit).unwrap_or(false) {
        return Ok(PublicationProof {
            commit,
            tree,
            remote: remote.to_string(),
            remote_ref: Some(target_ref),
            recovery_ref: None,
            bundle_path: None,
            diagnostic: if push.status.success() {
                None
            } else {
                Some("concurrent publication already installed the same commit".to_string())
            },
        });
    }

    let short = &commit[..12.min(commit.len())];
    let run = sanitize_ref_component(run_id);
    let recovery_ref = format!("refs/heads/animus/recovery/{}-{}-{}", branch, run, short);
    let recovery_spec = format!("{commit}:{recovery_ref}");
    let recovery = git_output(cwd, &["push", remote, &recovery_spec])?;
    if recovery.status.success() && remote_ref_reaches(cwd, remote, &recovery_ref, &commit).unwrap_or(false) {
        return Ok(PublicationProof {
            commit,
            tree,
            remote: remote.to_string(),
            remote_ref: Some(recovery_ref.clone()),
            recovery_ref: Some(recovery_ref),
            bundle_path: None,
            diagnostic: Some(format!(
                "target branch changed concurrently; exact reviewed commit preserved on a recovery ref ({})",
                redact_git_diagnostic(&String::from_utf8_lossy(&push.stderr)).trim()
            )),
        });
    }

    let bundle_path = write_unpublished_bundle(cwd, &commit, durable_root)?;
    Ok(PublicationProof {
        commit,
        tree,
        remote: remote.to_string(),
        remote_ref: None,
        recovery_ref: None,
        bundle_path: Some(bundle_path),
        diagnostic: Some(format!(
            "publication and recovery-ref push failed: {}; recovery: {}",
            redact_git_diagnostic(&String::from_utf8_lossy(&push.stderr)).trim(),
            redact_git_diagnostic(&String::from_utf8_lossy(&recovery.stderr)).trim()
        )),
    })
}

pub fn current_branch(cwd: &str) -> Result<String> {
    git_text(cwd, &["branch", "--show-current"], "resolve current branch")
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use tempfile::TempDir;

    fn git(path: &Path, args: &[&str]) {
        let status = ProcessCommand::new("git").arg("-C").arg(path).args(args).status().unwrap();
        assert!(status.success(), "git {}", args.join(" "));
    }

    fn fixture() -> (TempDir, PathBuf, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let remote = root.path().join("remote.git");
        let work = root.path().join("work");
        git(root.path(), &["init", "--bare", remote.to_str().unwrap()]);
        git(root.path(), &["init", "-b", "reviewed", work.to_str().unwrap()]);
        git(&work, &["config", "user.name", "Test"]);
        git(&work, &["config", "user.email", "test@example.invalid"]);
        std::fs::write(work.join("reviewed.txt"), "reviewed\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "reviewed"]);
        git(&work, &["remote", "add", "origin", remote.to_str().unwrap()]);
        (root, remote, work)
    }

    #[test]
    fn commit_implementation_changes_is_a_noop_outside_a_git_repository() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("portal-agent-output.txt"), "complete\n").unwrap();

        commit_implementation_changes(root.path().to_str().unwrap(), "").unwrap();

        assert_eq!(std::fs::read_to_string(root.path().join("portal-agent-output.txt")).unwrap(), "complete\n");
    }

    #[test]
    fn commit_implementation_changes_is_a_noop_for_a_clean_repository() {
        let (_root, _remote, work) = fixture();

        commit_implementation_changes(work.to_str().unwrap(), "").unwrap();

        assert!(!git_has_pending_changes(work.to_str().unwrap()).unwrap());
    }

    #[test]
    fn commit_implementation_changes_still_requires_a_message_for_a_dirty_repository() {
        let (_root, _remote, work) = fixture();
        std::fs::write(work.join("reviewed.txt"), "changed\n").unwrap();

        let error = commit_implementation_changes(work.to_str().unwrap(), "").unwrap_err();

        assert!(error.to_string().contains("requires a non-empty commit message"));
        assert!(git_has_pending_changes(work.to_str().unwrap()).unwrap());
    }

    #[test]
    fn publication_proves_exact_remote_commit_and_same_head_is_idempotent() {
        let (root, _remote, work) = fixture();
        let first = publish_head_durably(work.to_str().unwrap(), "origin", "reviewed", "run-a", root.path()).unwrap();
        assert!(first.is_durable());
        assert_eq!(first.remote_ref.as_deref(), Some("refs/heads/reviewed"));

        let second = publish_head_durably(work.to_str().unwrap(), "origin", "reviewed", "run-b", root.path()).unwrap();
        assert!(second.is_durable());
        assert_eq!(second.commit, first.commit);
        assert!(second.recovery_ref.is_none());
    }

    #[test]
    fn environment_transport_publishes_when_host_has_no_checkout() {
        let (root, _remote, work) = fixture();
        let host = tempfile::tempdir().unwrap();
        assert!(!is_git_repo(host.path().to_str().unwrap()));

        let proof = publish_head_durably_with("origin", "reviewed", "remote-run", |args| {
            let output = ProcessCommand::new("git").arg("-C").arg(&work).args(&args).output()?;
            Ok(PublicationCommandOutput {
                success: output.status.success(),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        })
        .unwrap();

        assert!(proof.is_durable());
        assert_eq!(proof.remote_ref.as_deref(), Some("refs/heads/reviewed"));
        assert!(verify_publication_proof(work.to_str().unwrap(), &proof).unwrap());
        assert!(root.path().join("remote.git").is_dir());
    }

    #[test]
    fn divergent_concurrent_run_is_preserved_without_force_push() {
        let (root, remote, work) = fixture();
        let other = root.path().join("other");
        git(root.path(), &["clone", remote.to_str().unwrap(), other.to_str().unwrap()]);
        git(&other, &["config", "user.name", "Other"]);
        git(&other, &["config", "user.email", "other@example.invalid"]);
        git(&other, &["checkout", "--orphan", "reviewed"]);
        std::fs::write(other.join("other.txt"), "other\n").unwrap();
        git(&other, &["add", "."]);
        git(&other, &["commit", "-m", "other"]);
        git(&other, &["push", "origin", "HEAD:refs/heads/reviewed"]);

        let proof =
            publish_head_durably(work.to_str().unwrap(), "origin", "reviewed", "run/collision", root.path()).unwrap();
        assert!(proof.is_durable());
        assert!(proof.recovery_ref.as_deref().unwrap().starts_with("refs/heads/animus/recovery/"));
        let target =
            git_text(work.to_str().unwrap(), &["ls-remote", "origin", "refs/heads/reviewed"], "target").unwrap();
        assert!(!target.starts_with(&proof.commit), "collision target must not be overwritten");
    }

    #[test]
    fn unpublished_work_is_bundled_when_remote_is_unreachable() {
        let (root, _remote, work) = fixture();
        git(&work, &["remote", "set-url", "origin", "/definitely/missing/remote.git"]);
        let proof = publish_head_durably(work.to_str().unwrap(), "origin", "reviewed", "run-a", root.path()).unwrap();
        assert!(!proof.is_durable());
        assert!(proof.bundle_path.as_ref().unwrap().is_file());
        assert!(
            proof.bundle_path.as_ref().unwrap().starts_with(root.path().join("unpublished-git")),
            "fallback artifact must live outside the disposable checkout"
        );
    }

    #[test]
    fn diagnostics_redact_credentials() {
        let diagnostic = "fatal: https://secret-token@github.com/o/r token=abc password: hunter2 \
                          Authorization: Bearer ghp_still-secret";
        let redacted = redact_git_diagnostic(diagnostic);
        assert!(!redacted.contains("secret-token"));
        assert!(!redacted.contains("abc"));
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("ghp_still-secret"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn publication_authorization_denials_are_non_retryable_but_collisions_and_transport_are_not() {
        assert!(is_non_retryable_publication_denial(
            "remote: refusing to allow a GitHub App to create or update workflow `.github/workflows/ci.yml` \
             without `workflows` permission"
        ));
        assert!(is_non_retryable_publication_denial(
            "remote: error: GH013: Repository rule violations found for refs/heads/main"
        ));
        assert!(!is_non_retryable_publication_denial("! [rejected] reviewed -> reviewed (non-fast-forward)"));
        assert!(!is_non_retryable_publication_denial(
            "fatal: unable to access 'https://github.com/o/r': Could not resolve host"
        ));
    }

    #[test]
    fn authorization_escalation_is_redacted_and_preserves_exact_git_identity() {
        let escalation = publication_denial_escalation(
            "fatal: https://secret-token@github.com/o/r Authorization: Bearer ghp_secret; workflows permission",
            Some("1111111111111111111111111111111111111111"),
            Some("2222222222222222222222222222222222222222"),
        );
        assert!(!escalation.contains("secret-token"));
        assert!(!escalation.contains("ghp_secret"));
        assert!(escalation.contains("1111111111111111111111111111111111111111"));
        assert!(escalation.contains("2222222222222222222222222222222222222222"));
        assert!(escalation.contains("explicit workflow cancellation"));
    }

    #[test]
    fn delegated_proof_is_revalidated_by_git_and_rejects_tampering() {
        let (root, remote, work) = fixture();
        let proof = publish_head_durably(work.to_str().unwrap(), "origin", "reviewed", "run-a", root.path()).unwrap();
        assert!(verify_publication_proof(work.to_str().unwrap(), &proof).unwrap());

        let mut forged = proof.clone();
        forged.tree = "0000000000000000000000000000000000000000".to_string();
        assert!(!verify_publication_proof(work.to_str().unwrap(), &forged).unwrap());

        forged = proof.clone();
        forged.remote = "/tmp/attacker-controlled.git".to_string();
        assert!(!verify_publication_proof(work.to_str().unwrap(), &forged).unwrap());

        let other = root.path().join("other");
        git(root.path(), &["clone", remote.to_str().unwrap(), other.to_str().unwrap()]);
        git(&other, &["config", "user.name", "Other"]);
        git(&other, &["config", "user.email", "other@example.invalid"]);
        git(&other, &["checkout", "--orphan", "replacement"]);
        git(&other, &["rm", "-rf", "--ignore-unmatch", "."]);
        std::fs::write(other.join("reviewed.txt"), "replacement\n").unwrap();
        git(&other, &["add", "."]);
        git(&other, &["commit", "-m", "replace reviewed head"]);
        git(&other, &["push", "--force", "origin", "HEAD:refs/heads/reviewed"]);

        assert!(
            !verify_publication_proof(work.to_str().unwrap(), &proof).unwrap(),
            "a proof is stale once its remote ref no longer reaches the reviewed commit"
        );
    }
}
