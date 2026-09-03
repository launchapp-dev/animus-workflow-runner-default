//! SPEC-001 (production incident TASK-001): sync the daemon-staged phase skill
//! definitions onto a remote run node BEFORE the first phase executes.
//!
//! The daemon (animus-cli rc.50) materializes the run's resolved agent skill
//! definitions as `<skill-name>.yaml` files in a PER-RUN staging dir and points
//! the runner at it via the [`PHASE_SKILLS_DIR_ENV`] env var. Local
//! (portal-local) runs read that dir directly in
//! [`crate::skill_dispatch::resolve_phase_skills`]. Runs that hold a REMOTE node
//! (brokered acquire, owned prepare, retained-publication reattach, or the
//! REQ-052 session-delegation path) additionally need those files ON the node,
//! where the node-side skill resolution picks them up through the SAME user-tier
//! loader (`${HOME}/.animus/config/skill_definitions/*.yaml`).
//!
//! This module owns the SYNC half: collect the staged files (basename-validated,
//! capped), build the node-side write command, and drive it over the environment
//! exec channel already in use. Sync is BEST-EFFORT by design: a failed write is
//! logged loudly but never fails the run here — the existing missing-skill
//! hard-fail at phase resolution is the enforcement point.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use tracing::warn;

use crate::phase_environment::HeldEnvironment;

/// Env var the daemon sets on the runner spawn: absolute path of the per-run
/// staging dir containing the run's `<skill-name>.yaml` skill definitions.
/// Present iff at least one file exists; absent otherwise.
///
/// This literal mirrors the kernel-side
/// `animus_runtime_shared::phase_skills::ANIMUS_PHASE_SKILLS_DIR_ENV`. The
/// runner pins the animus-cli crates by rev (see Cargo.toml) and the pinned rev
/// predates that const, so the literal is read here until the next kernel pin
/// bump lands the shared constant.
pub(crate) const PHASE_SKILLS_DIR_ENV: &str = "ANIMUS_PHASE_SKILLS_DIR";

/// Cap on staged files synced to a node (SPEC-001). Over-limit files are
/// warned about and skipped — never a run failure at this layer.
const MAX_SYNC_FILES: usize = 64;

/// Cap on TOTAL staged content bytes synced to a node (SPEC-001).
const MAX_SYNC_TOTAL_BYTES: usize = 1024 * 1024; // 1 MiB

/// Per-write exec timeout on the node. A `mkdir` + `base64 -d` of at most
/// 1 MiB is instant; this only bounds a wedged exec channel.
pub(crate) const SKILL_WRITE_TIMEOUT_SECS: u64 = 60;

/// One staged skill file to materialize on the node: `program`/`args` run the
/// node-side write and `stdin_b64` carries the base64-encoded file content on
/// stdin (matching the `base64 -d` in the command).
#[derive(Debug)]
pub(crate) struct SkillSyncWrite {
    /// Validated file basename, e.g. `review.yaml` (shell-safe by construction).
    pub name: String,
    pub program: String,
    pub args: Vec<String>,
    pub stdin_b64: String,
}

/// The collected sync work for one staging dir. `skipped` records every staged
/// `*.yaml` file that was NOT selected (already logged at selection time).
#[derive(Debug, Default)]
pub(crate) struct SkillSyncPlan {
    pub writes: Vec<SkillSyncWrite>,
    pub skipped: Vec<(String, &'static str)>,
}

/// The buffered outcome of one node-side write, normalized across the sync
/// exec channel ([`crate::phase_environment::EnvCommandOutput`]) and the
/// session path's raw `environment/exec` (`ExecResponse`).
pub(crate) struct SkillWriteOutcome {
    pub exit_code: Option<i32>,
    pub stderr: String,
    pub timed_out: bool,
}

/// Whether `name` matches the wire contract for staged skill files:
/// `^[a-z0-9][a-z0-9-]{0,63}\.yaml$`. Only such names are written to the node,
/// so the interpolated shell path is always safe.
fn is_valid_skill_basename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".yaml") else {
        return false;
    };
    let bytes = stem.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return false;
    }
    (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

/// The node-side write command: create the user-tier skill definitions dir and
/// decode stdin into `<name>`. Overwrites a same-name file (idempotent per run).
fn skill_write_command(name: &str) -> (String, Vec<String>) {
    (
        "sh".to_string(),
        vec![
            "-c".to_string(),
            format!(
                "d=\"${{HOME:-/root}}/.animus/config/skill_definitions\"; mkdir -p \"$d\" && base64 -d > \"$d/{name}\""
            ),
        ],
    )
}

/// Standard-base64 encode (RFC 4648, with padding). Hand-rolled to avoid a new
/// crate dependency; pinned against the RFC test vectors in this module's tests
/// and round-tripped through an independent decoder there.
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(chunk.get(1).copied().unwrap_or(0));
        let b2 = u32::from(chunk.get(2).copied().unwrap_or(0));
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(n >> 18) as usize & 0x3F] as char);
        out.push(ALPHABET[(n >> 12) as usize & 0x3F] as char);
        out.push(if chunk.len() > 1 { ALPHABET[(n >> 6) as usize & 0x3F] as char } else { '=' });
        out.push(if chunk.len() > 2 { ALPHABET[n as usize & 0x3F] as char } else { '=' });
    }
    out
}

/// Collect the sync work for staging dir `dir`: every `*.yaml` regular file
/// whose basename matches the wire regex, in lexicographic order, subject to
/// the file-count and total-size caps. Symlinks, bad basenames, unreadable
/// files, and over-cap extras are skipped with a warning (never fatal). A
/// missing/unreadable dir yields an empty plan.
pub(crate) fn plan_staged_skill_sync(dir: &Path) -> SkillSyncPlan {
    let mut plan = SkillSyncPlan::default();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            warn!(dir = %dir.display(), %error, "could not list the staged phase skills directory; skipping sync");
            return plan;
        }
    };
    let mut candidates: Vec<(String, PathBuf, std::fs::FileType)> = Vec::new();
    for entry in entries.filter_map(|entry| entry.ok()) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".yaml") {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            plan.skipped.push((name, "unreadable file type"));
            continue;
        };
        candidates.push((name, entry.path(), file_type));
    }
    candidates.sort_by(|left, right| left.0.cmp(&right.0));

    let mut total_bytes = 0usize;
    for (name, path, file_type) in candidates {
        let reason = if file_type.is_symlink() {
            Some("symlink")
        } else if !file_type.is_file() {
            Some("not a regular file")
        } else if !is_valid_skill_basename(&name) {
            Some("basename does not match ^[a-z0-9][a-z0-9-]{0,63}\\.yaml$")
        } else if plan.writes.len() >= MAX_SYNC_FILES {
            Some("over the 64-file sync cap")
        } else {
            None
        };
        if let Some(reason) = reason {
            warn!(file = %name, dir = %dir.display(), reason, "skipping staged phase skill file");
            plan.skipped.push((name, reason));
            continue;
        }
        let content = match std::fs::read(&path) {
            Ok(content) => content,
            Err(error) => {
                warn!(file = %name, dir = %dir.display(), %error, "could not read staged phase skill file; skipping");
                plan.skipped.push((name, "unreadable"));
                continue;
            }
        };
        if total_bytes + content.len() > MAX_SYNC_TOTAL_BYTES {
            warn!(file = %name, dir = %dir.display(), "skipping staged phase skill file: over the 1 MiB total sync cap");
            plan.skipped.push((name, "over the 1 MiB total sync cap"));
            continue;
        }
        total_bytes += content.len();
        let (program, args) = skill_write_command(&name);
        plan.writes.push(SkillSyncWrite { name, program, args, stdin_b64: base64_encode(&content) });
    }
    plan
}

/// Build the sync plan from the daemon-provided staging dir, or `None` when the
/// env var is unset/blank, the dir is missing, or nothing in it is syncable
/// (the caller's no-op case).
pub(crate) fn staged_skill_sync_plan_from_env() -> Option<SkillSyncPlan> {
    let raw = std::env::var(PHASE_SKILLS_DIR_ENV).ok()?;
    let dir = PathBuf::from(raw.trim());
    if dir.as_os_str().is_empty() {
        return None;
    }
    if !dir.is_dir() {
        warn!(dir = %dir.display(), "{PHASE_SKILLS_DIR_ENV} is set but the staging directory does not exist; skipping phase-skills sync");
        return None;
    }
    let plan = plan_staged_skill_sync(&dir);
    if plan.writes.is_empty() {
        return None;
    }
    Some(plan)
}

/// Log the result of one staged-skill write; returns true when the file landed.
/// A failed write is NOT fatal here by design (SPEC-001): the existing
/// missing-skill hard-fail at phase resolution is the enforcement point, so the
/// log line names that consequence explicitly.
pub(crate) fn log_skill_write_outcome(write: &SkillSyncWrite, result: Result<SkillWriteOutcome>) -> bool {
    match result {
        Ok(outcome) if !outcome.timed_out && outcome.exit_code == Some(0) => true,
        Ok(outcome) => {
            warn!(
                file = %write.name,
                exit_code = ?outcome.exit_code,
                timed_out = outcome.timed_out,
                stderr = %outcome.stderr.trim(),
                "phase skill definition was NOT written to the run node; if a phase requires it, \
                 phase resolution will fail with the missing-skill error"
            );
            false
        }
        Err(error) => {
            warn!(
                file = %write.name,
                %error,
                "phase skill definition write to the run node failed; if a phase requires it, \
                 phase resolution will fail with the missing-skill error"
            );
            false
        }
    }
}

/// Drive a sync plan through a synchronous exec channel, tolerating per-write
/// failures (logged via [`log_skill_write_outcome`]). Returns (written, failed).
pub(crate) fn execute_skill_sync_plan(
    plan: &SkillSyncPlan,
    mut exec: impl FnMut(&SkillSyncWrite) -> Result<SkillWriteOutcome>,
) -> (usize, usize) {
    let mut written = 0;
    let mut failed = 0;
    for write in &plan.writes {
        if log_skill_write_outcome(write, exec(write)) {
            written += 1;
        } else {
            failed += 1;
        }
    }
    (written, failed)
}

/// Sync the daemon-staged phase skill definitions onto a HELD remote node
/// (brokered acquire, owned prepare, or retained-publication reattach — all
/// surface as a [`HeldEnvironment`]) via its buffered `exec_command` channel.
/// No-op when [`PHASE_SKILLS_DIR_ENV`] is unset or the staging dir has nothing
/// syncable. Never fails the run: per-write failures are logged, and a missing
/// required skill still hard-fails later at phase resolution.
pub(crate) fn sync_staged_skills_to_held_environment(held: &dyn HeldEnvironment, project_root: &Path) {
    let Some(plan) = staged_skill_sync_plan_from_env() else {
        return;
    };
    let (written, failed) = execute_skill_sync_plan(&plan, |write| {
        held.exec_command(
            project_root,
            &write.program,
            &write.args,
            &BTreeMap::new(),
            None,
            Some(write.stdin_b64.clone()),
            Some(Duration::from_secs(SKILL_WRITE_TIMEOUT_SECS)),
        )
        .map(|output| SkillWriteOutcome {
            exit_code: Some(output.exit_code),
            stderr: output.stderr,
            timed_out: output.timed_out,
        })
    });
    tracing::info!(
        environment = held.id(),
        staged_files = plan.writes.len(),
        written,
        failed,
        "synced staged phase skill definitions to the run node"
    );
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Independent standard-base64 decoder (test-only) so the round-trip tests
    /// validate the hand-rolled encoder against separate logic.
    pub(crate) fn base64_decode(text: &str) -> Vec<u8> {
        fn value_of(byte: u8) -> u32 {
            match byte {
                b'A'..=b'Z' => u32::from(byte - b'A'),
                b'a'..=b'z' => u32::from(byte - b'a') + 26,
                b'0'..=b'9' => u32::from(byte - b'0') + 52,
                b'+' => 62,
                b'/' => 63,
                _ => panic!("invalid base64 byte {byte}"),
            }
        }
        let bytes: Vec<u8> = text.bytes().filter(|byte| *byte != b'=').collect();
        let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
        for chunk in bytes.chunks(4) {
            let mut n = 0u32;
            for (index, byte) in chunk.iter().enumerate() {
                n |= value_of(*byte) << (18 - 6 * index);
            }
            out.push((n >> 16) as u8);
            if chunk.len() > 2 {
                out.push((n >> 8) as u8);
            }
            if chunk.len() > 3 {
                out.push(n as u8);
            }
        }
        out
    }

    #[test]
    fn base64_encode_matches_rfc4648_vectors() {
        let vectors: &[(&[u8], &str)] = &[
            (b"", ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"fooba", "Zm9vYmE="),
            (b"foobar", "Zm9vYmFy"),
            (&[0x00, 0xFF, 0x10, 0x83], "AP8Qgw=="),
        ];
        for (input, expected) in vectors {
            assert_eq!(base64_encode(input), *expected, "encode {input:?}");
            assert_eq!(base64_decode(expected), *input, "decode {expected:?}");
        }
        // YAML-ish content with newlines and unicode round-trips byte-exactly.
        let content = "name: review\ndescription: \"prüfen\"\nprompt:\n  prefix: |\n    line\n";
        assert_eq!(base64_decode(&base64_encode(content.as_bytes())), content.as_bytes());
    }

    #[test]
    fn valid_skill_basenames_match_the_wire_regex() {
        for good in ["a.yaml", "0.yaml", "review.yaml", "my-skill-2.yaml", &format!("{}.yaml", "a".repeat(64))] {
            assert!(is_valid_skill_basename(good), "{good} must be accepted");
        }
        for bad in [
            "Foo.yaml",
            "-lead.yaml",
            "_lead.yaml",
            "has space.yaml",
            "dot.dot.yaml",
            "under_score.yaml",
            ".yaml",
            "a.yml",
            "a.yaml.yaml",
            "no-extension",
            &format!("{}.yaml", "a".repeat(65)),
        ] {
            assert!(!is_valid_skill_basename(bad), "{bad} must be rejected");
        }
    }

    #[test]
    fn plan_builds_the_node_write_command_with_roundtrippable_content() {
        let temp = tempfile::tempdir().expect("tempdir");
        let content = "name: review\ndescription: Staged review skill\n";
        std::fs::write(temp.path().join("review.yaml"), content).expect("write staged skill");

        let plan = plan_staged_skill_sync(temp.path());
        assert!(plan.skipped.is_empty(), "nothing skipped: {:?}", plan.skipped);
        assert_eq!(plan.writes.len(), 1);
        let write = &plan.writes[0];
        assert_eq!(write.name, "review.yaml");
        assert_eq!(write.program, "sh");
        assert_eq!(write.args.len(), 2);
        assert_eq!(write.args[0], "-c");
        assert!(
            write.args[1].contains("d=\"${HOME:-/root}/.animus/config/skill_definitions\"; mkdir -p \"$d\""),
            "command creates the user-tier skill dir: {}",
            write.args[1]
        );
        assert!(
            write.args[1].ends_with("base64 -d > \"$d/review.yaml\""),
            "command decodes stdin into the target file: {}",
            write.args[1]
        );
        assert_eq!(base64_decode(&write.stdin_b64), content.as_bytes(), "stdin base64 round-trips the file content");
    }

    #[test]
    fn plan_skips_bad_basenames_and_symlinks() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("ok-skill.yaml"), "name: ok-skill\n").expect("write good skill");
        std::fs::write(temp.path().join("Bad.yaml"), "name: bad\n").expect("write bad-name skill");
        // Non-yaml files are ignored entirely (not even counted as skipped).
        std::fs::write(temp.path().join("notes.txt"), "ignored").expect("write non-yaml");
        #[cfg(unix)]
        std::os::unix::fs::symlink(temp.path().join("ok-skill.yaml"), temp.path().join("link.yaml")).expect("symlink");

        let plan = plan_staged_skill_sync(temp.path());
        let written: Vec<&str> = plan.writes.iter().map(|write| write.name.as_str()).collect();
        assert_eq!(written, vec!["ok-skill.yaml"]);
        let skipped: Vec<(&str, &str)> = plan.skipped.iter().map(|(name, reason)| (name.as_str(), *reason)).collect();
        assert!(skipped.contains(&("Bad.yaml", "basename does not match ^[a-z0-9][a-z0-9-]{0,63}\\.yaml$")));
        #[cfg(unix)]
        assert!(skipped.contains(&("link.yaml", "symlink")));
    }

    #[test]
    fn plan_enforces_the_file_count_cap() {
        let temp = tempfile::tempdir().expect("tempdir");
        for index in 0..65 {
            std::fs::write(temp.path().join(format!("skill-{index:02}.yaml")), "name: x\n").expect("write skill");
        }
        let plan = plan_staged_skill_sync(temp.path());
        assert_eq!(plan.writes.len(), MAX_SYNC_FILES, "exactly 64 files sync");
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].0, "skill-64.yaml");
        assert_eq!(plan.skipped[0].1, "over the 64-file sync cap");
    }

    #[test]
    fn plan_enforces_the_total_size_cap() {
        let temp = tempfile::tempdir().expect("tempdir");
        let blob = "x".repeat(300 * 1024);
        for index in 0..4 {
            std::fs::write(temp.path().join(format!("big-{index}.yaml")), &blob).expect("write big skill");
        }
        let plan = plan_staged_skill_sync(temp.path());
        // 3 * 300 KiB = 900 KiB fits; the 4th would cross 1 MiB.
        assert_eq!(plan.writes.len(), 3);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].0, "big-3.yaml");
        assert_eq!(plan.skipped[0].1, "over the 1 MiB total sync cap");
    }

    #[test]
    fn execute_skill_sync_plan_tolerates_write_failures() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("one.yaml"), "name: one\n").expect("write skill");
        std::fs::write(temp.path().join("two.yaml"), "name: two\n").expect("write skill");
        let plan = plan_staged_skill_sync(temp.path());
        assert_eq!(plan.writes.len(), 2);

        // First write errors at the transport layer, second exits non-zero on
        // the node: neither panics nor aborts the loop, both are counted.
        let (written, failed) = execute_skill_sync_plan(&plan, |write| {
            if write.name == "one.yaml" {
                Err(anyhow::anyhow!("relay closed"))
            } else {
                Ok(SkillWriteOutcome { exit_code: Some(1), stderr: "disk full".to_string(), timed_out: false })
            }
        });
        assert_eq!((written, failed), (0, 2));

        let (written, failed) = execute_skill_sync_plan(&plan, |_write| {
            Ok(SkillWriteOutcome { exit_code: Some(0), stderr: String::new(), timed_out: false })
        });
        assert_eq!((written, failed), (2, 0));
    }

    #[test]
    fn staged_skill_sync_plan_from_env_gates_on_env_var_and_dir() {
        let _lock = crate::test_env::scoped_state_serializer();
        use protocol::test_utils::EnvVarGuard;

        let _unset = EnvVarGuard::set(PHASE_SKILLS_DIR_ENV, None);
        assert!(staged_skill_sync_plan_from_env().is_none(), "unset env var -> no sync");

        let _missing = EnvVarGuard::set(PHASE_SKILLS_DIR_ENV, Some("/definitely/missing/animus-skills-dir"));
        assert!(staged_skill_sync_plan_from_env().is_none(), "missing dir -> no sync");

        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(temp.path().join("review.yaml"), "name: review\n").expect("write skill");
        let _set = EnvVarGuard::set(PHASE_SKILLS_DIR_ENV, temp.path().to_str());
        let plan = staged_skill_sync_plan_from_env().expect("dir with a syncable file yields a plan");
        assert_eq!(plan.writes.len(), 1);
    }
}
