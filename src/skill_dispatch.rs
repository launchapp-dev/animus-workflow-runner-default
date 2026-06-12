//! Launch-contract injection for applied phase skills.
//!
//! v0.4.4: the skill resolution / application / capability-override layer
//! moved to `animus_runtime_shared::phase_skills` (the kernel and the
//! ad-hoc `animus agent run --skill` path share it). What remains here is
//! the launch-affecting injection of an applied skill's `extra_args`,
//! `env`, and `codex_config_overrides` onto the runtime contract's
//! `/cli/launch` block.

use animus_runtime_shared::runtime_support::{inject_cli_launch_env, inject_codex_config_overrides_list};
use orchestrator_config::SkillApplicationResult;
use serde_json::Value;

/// Returns the index where skill-supplied `extra_args` should be inserted in
/// `/cli/launch/args` so they land before the prompt and never split a
/// flag/value pair that wraps the prompt.
///
/// Heuristics:
///   - When the trailing two args are `-p <prompt>` (Gemini's prompt-flag
///     pair), insert before `-p` so the prompt stays attached to its flag.
///   - Otherwise insert at `len - 1`, matching `inject_cli_extra_args`'s
///     `launch_prompt_insert_index` heuristic.
///   - When args is empty, return 0 (callers won't read past that).
fn skill_extra_args_insert_index(args: &[Value]) -> usize {
    let len = args.len();
    if len >= 2 {
        let prompt_flag = args[len - 2].as_str();
        if prompt_flag == Some("-p") || prompt_flag == Some("--prompt") {
            return len - 2;
        }
    }
    len.saturating_sub(1)
}

pub fn inject_skill_overrides(runtime_contract: &mut Value, tool_id: &str, skill_result: &SkillApplicationResult) {
    // Codex P2 #3: for Claude/Codex/Gemini launch contracts the prompt is the
    // trailing positional argument, so skill-supplied flags must be inserted
    // BEFORE it (the CLI would otherwise read them as prompt text). The
    // shared `inject_cli_extra_args_list` inserts at `len - 1`, which would
    // split Gemini's trailing `-p <prompt>` flag/value pair — keep the
    // gemini-aware local index here until the shared helper learns the
    // `-p` tail case.
    if !skill_result.extra_args.is_empty() {
        if let Some(args) = runtime_contract.pointer_mut("/cli/launch/args").and_then(Value::as_array_mut) {
            let mut insert_at = skill_extra_args_insert_index(args);
            for arg in &skill_result.extra_args {
                if !args.iter().any(|existing| existing.as_str() == Some(arg)) {
                    args.insert(insert_at, Value::String(arg.clone()));
                    insert_at += 1;
                }
            }
        }
    }

    // Launch env + codex `-c` config overrides go through the shared
    // helpers (the same ones the kernel's ad-hoc `--skill` launch graft
    // uses): env entries never clobber existing keys, codex overrides
    // upsert an existing `-c key=...` pair in place.
    inject_cli_launch_env(runtime_contract, &skill_result.env);
    inject_codex_config_overrides_list(runtime_contract, tool_id, &skill_result.codex_config_overrides);
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_config::SkillApplicationResult;
    use serde_json::json;

    /// Codex P2 #3: skill-supplied `extra_args` must land before the trailing
    /// prompt positional so the CLI parses them as flags. Pre-fix, `extra_args`
    /// were `args.push`-ed after the prompt and the CLI treated them as
    /// additional prompt text.
    #[test]
    fn inject_skill_overrides_inserts_extra_args_before_prompt() {
        let mut runtime_contract = json!({
            "cli": {
                "launch": {
                    "args": ["exec", "the-prompt-text"]
                }
            }
        });
        let skill_result = SkillApplicationResult {
            extra_args: vec!["--skill-flag-1".to_string(), "--skill-flag-2".to_string()],
            ..Default::default()
        };

        inject_skill_overrides(&mut runtime_contract, "codex", &skill_result);

        let args = runtime_contract.pointer("/cli/launch/args").and_then(Value::as_array).expect("launch args");
        let arg_strings: Vec<&str> = args.iter().filter_map(|value| value.as_str()).collect();
        assert_eq!(
            arg_strings,
            vec!["exec", "--skill-flag-1", "--skill-flag-2", "the-prompt-text"],
            "skill extra_args must be inserted before the trailing prompt positional"
        );
    }

    /// Gemini-shaped launch contracts end with `-p <prompt>` (the prompt is
    /// the value of the `-p` flag, not a bare positional). Skill flags must
    /// land before `-p` so the flag/value pair stays intact.
    #[test]
    fn inject_skill_overrides_handles_gemini_prompt_flag_pair() {
        let mut runtime_contract = json!({
            "cli": {
                "launch": {
                    "args": ["--model", "gemini-2.5-pro", "-p", "the-prompt-text"]
                }
            }
        });
        let skill_result =
            SkillApplicationResult { extra_args: vec!["--skill-flag".to_string()], ..Default::default() };

        inject_skill_overrides(&mut runtime_contract, "gemini", &skill_result);

        let args = runtime_contract.pointer("/cli/launch/args").and_then(Value::as_array).expect("launch args");
        let arg_strings: Vec<&str> = args.iter().filter_map(|value| value.as_str()).collect();
        assert_eq!(
            arg_strings,
            vec!["--model", "gemini-2.5-pro", "--skill-flag", "-p", "the-prompt-text"],
            "skill extra_args must land before the Gemini `-p <prompt>` pair so the prompt stays attached to its flag"
        );
    }

    /// When the launch args contain only the prompt positional, the skill
    /// flags land at position 0 (before the prompt).
    #[test]
    fn inject_skill_overrides_with_single_prompt_inserts_before_it() {
        let mut runtime_contract = json!({
            "cli": { "launch": { "args": ["the-prompt-text"] } }
        });
        let skill_result = SkillApplicationResult { extra_args: vec!["--flag".to_string()], ..Default::default() };

        inject_skill_overrides(&mut runtime_contract, "codex", &skill_result);

        let args = runtime_contract.pointer("/cli/launch/args").and_then(Value::as_array).expect("launch args");
        let arg_strings: Vec<&str> = args.iter().filter_map(|value| value.as_str()).collect();
        assert_eq!(arg_strings, vec!["--flag", "the-prompt-text"]);
    }

    /// Skill env entries land on `/cli/launch/env` without clobbering keys
    /// that are already present (explicit launch env wins over the skill).
    #[test]
    fn inject_skill_overrides_env_does_not_clobber_existing_keys() {
        let mut runtime_contract = json!({
            "cli": { "launch": { "args": ["exec", "prompt"], "env": { "SKILL_MODE": "explicit" } } }
        });
        let skill_result = SkillApplicationResult {
            env: std::collections::BTreeMap::from([
                ("SKILL_MODE".to_string(), "review".to_string()),
                ("SKILL_EXTRA".to_string(), "1".to_string()),
            ]),
            ..Default::default()
        };

        inject_skill_overrides(&mut runtime_contract, "codex", &skill_result);

        assert_eq!(
            runtime_contract.pointer("/cli/launch/env/SKILL_MODE").and_then(Value::as_str),
            Some("explicit"),
            "existing launch env keys must win over the skill"
        );
        assert_eq!(runtime_contract.pointer("/cli/launch/env/SKILL_EXTRA").and_then(Value::as_str), Some("1"));
    }

    /// Codex config overrides ride the shared `-c key=value` upsert (the
    /// kernel's ad-hoc `--skill` graft uses the same helper), replacing an
    /// existing pair for the same key instead of appending a duplicate.
    #[test]
    fn inject_skill_overrides_codex_config_overrides_upsert_dash_c_pairs() {
        let mut runtime_contract = json!({
            "cli": { "launch": { "args": ["exec", "-c", "profile=base", "prompt"] } }
        });
        let skill_result = SkillApplicationResult {
            codex_config_overrides: vec!["profile=review".to_string(), "sandbox=workspace".to_string()],
            ..Default::default()
        };

        inject_skill_overrides(&mut runtime_contract, "codex", &skill_result);

        let args = runtime_contract.pointer("/cli/launch/args").and_then(Value::as_array).expect("launch args");
        let arg_strings: Vec<&str> = args.iter().filter_map(|value| value.as_str()).collect();
        assert!(
            arg_strings.windows(2).any(|pair| pair == ["-c", "profile=review"]),
            "existing -c pair must be upserted: {arg_strings:?}"
        );
        assert!(
            arg_strings.windows(2).any(|pair| pair == ["-c", "sandbox=workspace"]),
            "new -c pair must be inserted: {arg_strings:?}"
        );
        assert!(!arg_strings.contains(&"profile=base"), "stale override value must be replaced: {arg_strings:?}");

        // Non-codex tools are untouched.
        let mut gemini_contract = json!({ "cli": { "launch": { "args": ["-p", "prompt"] } } });
        inject_skill_overrides(&mut gemini_contract, "gemini", &skill_result);
        let gemini_args = gemini_contract.pointer("/cli/launch/args").and_then(Value::as_array).expect("args");
        assert_eq!(gemini_args.len(), 2, "codex overrides must not leak onto non-codex tools");
    }
}
