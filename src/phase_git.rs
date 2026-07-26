use anyhow::{Context, Result};
use std::process::{Command as ProcessCommand, Stdio};

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
