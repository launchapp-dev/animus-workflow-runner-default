use std::path::Path;

use anyhow::Result;
use orchestrator_config::{
    apply_skill_for_execution, merge_skill_applications, parse_skill_capability_key, preview_skill_application,
    skill_definition::SkillDefinition,
    skill_resolution::{resolve_skills_for_project, ResolvedSkill},
    skill_scoping::{load_skills_from_directory, SkillSourceOrigin},
    SkillApplicationResult, SkillCapabilityKey,
};
use protocol::PhaseCapabilities;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::config_context::RuntimeConfigContext;

fn collect_phase_skills(ctx: &RuntimeConfigContext, phase_id: &str) -> Vec<String> {
    let wf_phase_skills =
        ctx.workflow_config.config.phase_definitions.get(phase_id).map(|def| &def.skills).filter(|s| !s.is_empty());
    if let Some(skills) = wf_phase_skills {
        return skills.clone();
    }

    if let Some(phase_skills) =
        ctx.agent_runtime_config.phase_execution(phase_id).map(|def| &def.skills).filter(|s| !s.is_empty())
    {
        return phase_skills.clone();
    }

    let agent_id = ctx.phase_agent_id(phase_id);
    if let Some(id) = agent_id.as_deref() {
        let wf_profile_skills =
            ctx.workflow_config.config.agent_profiles.get(id).and_then(|p| p.skills.as_ref()).filter(|s| !s.is_empty());
        if let Some(skills) = wf_profile_skills {
            return skills.clone();
        }

        if let Some(profile_skills) =
            ctx.agent_runtime_config.agent_profile(id).map(|p| &p.skills).filter(|s| !s.is_empty())
        {
            return profile_skills.clone();
        }
    }

    Vec::new()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResolvedPhaseSkillSet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_skills: Vec<ResolvedSkill>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppliedPhaseSkills {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requested_skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_skills: Vec<ResolvedSkill>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applied_skills: Vec<ResolvedSkill>,
    #[serde(default, skip_serializing_if = "SkillApplicationResult::is_empty")]
    pub application: SkillApplicationResult,
}

/// Load the daemon-staged phase skill definitions from the per-run dir named by
/// [`crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV`] (SPEC-001 / TASK-001), using
/// the SAME YAML loader as the user-tier `~/.animus/config/skill_definitions`
/// sweep ([`load_skills_from_directory`]). `None` when the env var is unset or
/// blank, the dir is missing, or the load fails (each logged) — resolution then
/// falls back to the project chain byte-identically.
fn staged_phase_skill_definitions() -> Option<std::collections::BTreeMap<String, SkillDefinition>> {
    let raw = std::env::var(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV).ok()?;
    let dir = std::path::PathBuf::from(raw.trim());
    if dir.as_os_str().is_empty() {
        return None;
    }
    if !dir.is_dir() {
        warn!(dir = %dir.display(), "staged phase skills directory does not exist; falling back to project skill resolution");
        return None;
    }
    match load_skills_from_directory(&dir) {
        Ok(definitions) => Some(definitions),
        Err(error) => {
            warn!(dir = %dir.display(), %error, "failed to load staged phase skill definitions; falling back to project skill resolution");
            None
        }
    }
}

pub fn resolve_phase_skills(
    ctx: &RuntimeConfigContext,
    project_root: &Path,
    phase_id: &str,
) -> Result<ResolvedPhaseSkillSet> {
    let skills = collect_phase_skills(ctx, phase_id);
    if skills.is_empty() {
        return Ok(ResolvedPhaseSkillSet::default());
    }

    // SPEC-001 (TASK-001): the daemon stages the run's resolved skill
    // definitions into a per-run dir and points the runner at it via
    // `skill_dir_sync::PHASE_SKILLS_DIR_ENV`. Names defined there resolve from
    // the staged dir FIRST; names not staged fall back to the existing project
    // resolution, and missing-after-both keeps today's hard-fail semantics.
    let staged = staged_phase_skill_definitions();
    let resolved = match staged.as_ref().filter(|staged| !staged.is_empty()) {
        Some(staged) => {
            let missing: Vec<String> = skills.iter().filter(|name| !staged.contains_key(*name)).cloned().collect();
            let project_resolved = resolve_skills_for_project(&missing, project_root)?;
            let project_by_name: std::collections::HashMap<&str, ResolvedSkill> =
                missing.iter().map(String::as_str).zip(project_resolved).collect();
            skills
                .iter()
                .map(|name| {
                    if let Some(definition) = staged.get(name) {
                        // Staged definitions are the daemon-materialized
                        // equivalent of the user tier (same parser, same
                        // full-definition shape), so they are tagged `User`.
                        ResolvedSkill { definition: definition.clone(), source: SkillSourceOrigin::User }
                    } else {
                        project_by_name
                            .get(name.as_str())
                            .unwrap_or_else(|| panic!("skill '{name}' resolved above"))
                            .clone()
                    }
                })
                .collect()
        }
        None => resolve_skills_for_project(&skills, project_root)?,
    };

    Ok(ResolvedPhaseSkillSet { requested_skills: skills, resolved_skills: resolved })
}

pub fn preview_phase_capabilities(base: &PhaseCapabilities, resolved: &ResolvedPhaseSkillSet) -> PhaseCapabilities {
    let preview_results = resolved
        .resolved_skills
        .iter()
        .filter_map(|skill| preview_skill_application(&skill.definition))
        .collect::<Vec<_>>();
    if preview_results.is_empty() {
        return base.clone();
    }

    let preview = merge_skill_applications(&preview_results);
    apply_skill_capability_overrides(base, &preview.capabilities)
}

pub fn apply_phase_skills(resolved: &ResolvedPhaseSkillSet, tool_id: &str, model_id: &str) -> AppliedPhaseSkills {
    let mut applied_skills = Vec::new();
    let mut applications = Vec::new();

    for skill in &resolved.resolved_skills {
        if let Some(application) = apply_skill_for_execution(&skill.definition, tool_id, Some(model_id)) {
            applied_skills.push(skill.clone());
            applications.push(application);
        }
    }

    AppliedPhaseSkills {
        requested_skills: resolved.requested_skills.clone(),
        resolved_skills: resolved.resolved_skills.clone(),
        applied_skills,
        application: merge_skill_applications(&applications),
    }
}

pub fn apply_skill_capability_overrides(
    base: &PhaseCapabilities,
    overrides: &std::collections::BTreeMap<String, bool>,
) -> PhaseCapabilities {
    let mut caps = base.clone();
    for (name, enabled) in overrides {
        match parse_skill_capability_key(name) {
            Some(SkillCapabilityKey::WritesFiles) => caps.writes_files = *enabled,
            Some(SkillCapabilityKey::MutatesState) => caps.mutates_state = *enabled,
            Some(SkillCapabilityKey::RequiresCommit) => caps.requires_commit = *enabled,
            Some(SkillCapabilityKey::EnforceProductChanges) => caps.enforce_product_changes = *enabled,
            Some(SkillCapabilityKey::IsResearch) => caps.is_research = *enabled,
            Some(SkillCapabilityKey::IsUiUx) => caps.is_ui_ux = *enabled,
            Some(SkillCapabilityKey::IsReview) => caps.is_review = *enabled,
            Some(SkillCapabilityKey::IsTesting) => caps.is_testing = *enabled,
            Some(SkillCapabilityKey::IsRequirements) => caps.is_requirements = *enabled,
            None => {
                warn!(capability = %name, "Ignoring unknown skill capability override");
            }
        }
    }
    caps
}

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
    // trailing positional argument. `inject_cli_extra_args` already uses the
    // pre-prompt insertion index for its sibling overrides; do the same here
    // so skill-supplied flags are seen as flags by the CLI, not as part of
    // the prompt text.
    //
    // Tool-specific edge cases:
    //   - Gemini emits the prompt as `-p <prompt>` (the prompt is the value
    //     of the `-p` flag, not a bare trailing positional). Inserting at
    //     `len-1` would land between `-p` and its value. Detect the `-p`
    //     pair at the tail and insert one slot earlier.
    //   - When the launch args vector is empty (no prompt positional yet),
    //     insert at the end so the order is still deterministic.
    if !skill_result.extra_args.is_empty() {
        if let Some(args) = runtime_contract.pointer_mut("/cli/launch/args").and_then(Value::as_array_mut) {
            let mut insert_at = skill_extra_args_insert_index(args);
            for arg in &skill_result.extra_args {
                if !args.iter().any(|a| a.as_str() == Some(arg)) {
                    args.insert(insert_at, Value::String(arg.clone()));
                    insert_at += 1;
                }
            }
        }
    }

    if !skill_result.env.is_empty() {
        if runtime_contract.pointer("/cli/launch/env").is_none() {
            if let Some(launch) = runtime_contract.pointer_mut("/cli/launch").and_then(Value::as_object_mut) {
                launch.insert("env".to_string(), Value::Object(serde_json::Map::new()));
            }
        }
        if let Some(env) = runtime_contract.pointer_mut("/cli/launch/env").and_then(Value::as_object_mut) {
            for (key, value) in &skill_result.env {
                env.entry(key.clone()).or_insert(Value::String(value.clone()));
            }
        }
    }

    if !skill_result.codex_config_overrides.is_empty() && tool_id.eq_ignore_ascii_case("codex") {
        if let Some(args) = runtime_contract.pointer_mut("/cli/launch/args").and_then(Value::as_array_mut) {
            for override_val in &skill_result.codex_config_overrides {
                let flag = format!("--config-override={}", override_val);
                if !args.iter().any(|a| a.as_str() == Some(&flag)) {
                    args.push(Value::String(flag));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::build_runtime_contract_with_resume;
    use crate::phase_prompt::{render_phase_prompt_with_ctx_overrides, PhasePromptInputs, PhaseRenderParams};
    use crate::runtime_contract::{inject_named_mcp_servers, set_mcp_tool_policy};

    use orchestrator_config::workflow_config::McpServerDefinition;
    use orchestrator_core::{
        builtin_agent_runtime_config, builtin_workflow_config, workflow_config_hash, write_workflow_config,
        LoadedWorkflowConfig, WorkflowConfigMetadata, WorkflowConfigSource,
    };
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn write_installed_skill(temp: &TempDir) {
        let scoped = protocol::scoped_state_root(temp.path()).expect("scoped state root");
        let state_dir = scoped.join("state");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        let registry = json!({
            "installed": [{
                "name": "registry-review",
                "version": "1.2.3",
                "source": "acme/registry-review",
                "registry": "catalog",
                "integrity": "sha256:test",
                "artifact": "registry-review-1.2.3.tgz",
                "definition": {
                    "name": "registry-review",
                    "description": "Registry-backed review skill",
                    "activation": {
                        "tools": ["codex"]
                    },
                    "prompt": {
                        "system": "Registry system prompt",
                        "prefix": "Registry prefix",
                        "suffix": "Registry suffix",
                        "directives": ["Validate references"]
                    },
                    "tool_policy": {
                        "allow": ["Read"],
                        "deny": ["Write"]
                    },
                    "model": {
                        "preferred": "gemini-2.5-pro"
                    },
                    "mcp_servers": ["docs"],
                    "capabilities": {
                        "writes_files": false
                    },
                    "extra_args": ["--skill-flag"],
                    "env": {
                        "SKILL_MODE": "review"
                    },
                    "codex_config_overrides": ["profile=review"]
                }
            }]
        });
        std::fs::write(state_dir.join("skills-registry.v1.json"), serde_json::to_vec_pretty(&registry).expect("json"))
            .expect("registry state");
    }

    #[test]
    fn runtime_resolves_installed_registry_skills_into_prompt_and_contract() {
        use orchestrator_config::agent_runtime_config::{
            AgentProfile, Idempotency, PhaseExecutionDefinition, PhaseExecutionMode,
        };

        let _guard = crate::test_env::scoped_state_serializer();
        let temp = tempfile::tempdir().expect("tempdir");
        let mut workflow = builtin_workflow_config();
        workflow.mcp_servers.insert(
            "docs".to_string(),
            McpServerDefinition {
                command: "docs-mcp".to_string(),
                args: vec!["--serve".to_string()],
                transport: None,
                url: None,
                config: BTreeMap::new(),
                tools: Vec::new(),
                env: BTreeMap::from([("DOCS_TOKEN".to_string(), "abc123".to_string())]),
                oauth: None,
            },
        );
        write_workflow_config(temp.path(), &workflow).expect("workflow config");
        write_installed_skill(&temp);
        let mut runtime = builtin_agent_runtime_config();
        runtime.tools_allowlist = vec!["git".to_string()];
        let default_agent = AgentProfile {
            description: "test agent".to_string(),
            system_prompt: "Run the fixture phase.".to_string(),
            ..Default::default()
        };
        runtime.agents.insert("default".to_string(), default_agent);
        // v0.6 intentionally ships no built-in phase definitions. Define the
        // fixture phase explicitly instead of relying on the removed legacy
        // `implementation` default.
        runtime.phases.insert(
            "implementation".to_string(),
            PhaseExecutionDefinition {
                mode: PhaseExecutionMode::Agent,
                agent_id: Some("default".to_string()),
                directive: None,
                system_prompt: None,
                runtime: None,
                capabilities: None,
                output_contract: None,
                output_json_schema: None,
                decision_contract: None,
                retry: None,
                skills: vec!["registry-review".to_string()],
                command: None,
                manual: None,
                default_tool: None,
                idempotency: Idempotency::Unknown,
                evals: None,
                worktree: None,
            },
        );
        let project_root = temp.path().to_string_lossy().to_string();
        let ctx = RuntimeConfigContext {
            agent_runtime_config: runtime,
            workflow_config: LoadedWorkflowConfig {
                metadata: WorkflowConfigMetadata {
                    schema: workflow.schema.clone(),
                    version: workflow.version,
                    hash: workflow_config_hash(&workflow),
                    source: WorkflowConfigSource::Builtin,
                },
                config: workflow,
                path: PathBuf::from("fixture"),
            },
        };
        let resolved = resolve_phase_skills(&ctx, temp.path(), "implementation").expect("resolve skills");
        assert_eq!(resolved.requested_skills, vec!["registry-review"]);
        assert!(matches!(
            resolved.resolved_skills.first().map(|skill| &skill.source),
            Some(orchestrator_config::skill_scoping::SkillSourceOrigin::Installed { .. })
        ));

        let preview_caps = preview_phase_capabilities(&ctx.phase_capabilities("implementation"), &resolved);
        let applied = apply_phase_skills(&resolved, "codex", "gpt-5.3-codex");
        let effective_caps = apply_skill_capability_overrides(&preview_caps, &applied.application.capabilities);
        assert_eq!(applied.application.model.as_deref(), Some("gemini-2.5-pro"));
        assert!(!effective_caps.writes_files, "skill capability should override implementation write default");

        let rendered = render_phase_prompt_with_ctx_overrides(
            &ctx,
            &PhaseRenderParams {
                project_root: &project_root,
                execution_cwd: &project_root,
                workflow_id: "wf-test",
                subject_id: "TASK-620",
                subject_title: "Runtime skill integration",
                subject_description: "Verify runtime skill resolution",
                phase_id: "implementation",
            },
            PhasePromptInputs::default(),
            Some(effective_caps.clone()),
            Some(&applied.application),
        );
        assert!(
            rendered.system_prompt.as_deref().is_some_and(|value| value.contains("Registry system prompt")),
            "expected skill system prompt in rendered prompt: {rendered:?}"
        );
        assert!(rendered.final_prompt.contains("Registry prefix"));
        assert!(rendered.final_prompt.contains("Skill directives:"));
        assert!(rendered.final_prompt.contains("Validate references"));
        assert!(rendered.final_prompt.contains("Registry suffix"));

        let mut runtime_contract =
            build_runtime_contract_with_resume("codex", "gpt-5.3-codex", &rendered.final_prompt, None)
                .expect("runtime contract");
        set_mcp_tool_policy(
            &mut runtime_contract,
            applied.application.tool_policy.as_ref().expect("skill tool policy"),
        );
        inject_named_mcp_servers(
            &mut runtime_contract,
            &project_root,
            &ctx,
            "implementation",
            &applied.application.mcp_servers,
        )
        .expect("skill mcp servers");
        inject_skill_overrides(&mut runtime_contract, "codex", &applied.application);

        assert_eq!(runtime_contract.pointer("/mcp/tool_policy/allow/0").and_then(Value::as_str), Some("Read"));
        assert_eq!(
            runtime_contract.pointer("/mcp/additional_servers/docs/command").and_then(Value::as_str),
            Some("docs-mcp")
        );
        assert_eq!(
            runtime_contract.pointer("/mcp/additional_servers/docs/env/DOCS_TOKEN").and_then(Value::as_str),
            Some("abc123")
        );
        assert_eq!(runtime_contract.pointer("/cli/launch/env/SKILL_MODE").and_then(Value::as_str), Some("review"));
        let args = runtime_contract.pointer("/cli/launch/args").and_then(Value::as_array).expect("launch args");
        assert!(args.iter().any(|value| value.as_str() == Some("--skill-flag")));
        assert!(args.iter().any(|value| value.as_str() == Some("--config-override=profile=review")));
    }

    #[test]
    fn resolve_phase_skills_reports_missing_skill_in_runtime_path() {
        use orchestrator_config::agent_runtime_config::{Idempotency, PhaseExecutionDefinition, PhaseExecutionMode};

        let temp = tempfile::tempdir().expect("tempdir");
        // v0.6 ships zero built-in phase content, so define the "implementation"
        // phase explicitly on the agent-runtime config to exercise the runtime
        // skill-resolution path with a missing skill.
        let mut runtime = builtin_agent_runtime_config();
        runtime.phases.insert(
            "implementation".to_string(),
            PhaseExecutionDefinition {
                mode: PhaseExecutionMode::Agent,
                agent_id: Some("default".to_string()),
                directive: None,
                system_prompt: None,
                runtime: None,
                capabilities: None,
                output_contract: None,
                output_json_schema: None,
                decision_contract: None,
                retry: None,
                skills: vec!["missing-runtime-skill".to_string()],
                command: None,
                manual: None,
                default_tool: None,
                idempotency: Idempotency::Unknown,
                evals: None,
                worktree: None,
            },
        );
        let workflow = builtin_workflow_config();
        let ctx = RuntimeConfigContext {
            agent_runtime_config: runtime,
            workflow_config: LoadedWorkflowConfig {
                metadata: WorkflowConfigMetadata {
                    schema: workflow.schema.clone(),
                    version: workflow.version,
                    hash: workflow_config_hash(&workflow),
                    source: WorkflowConfigSource::Builtin,
                },
                config: workflow,
                path: PathBuf::from("builtin"),
            },
        };
        let error =
            resolve_phase_skills(&ctx, temp.path(), "implementation").expect_err("missing skill should fail loudly");
        assert!(error.to_string().contains("missing-runtime-skill"), "error should name missing skill: {error}");
    }

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

    #[test]
    fn skill_capability_overrides_can_enable_state_mutations() {
        let overrides = BTreeMap::from([("mutates_state".to_string(), true)]);
        let effective = apply_skill_capability_overrides(&PhaseCapabilities::default(), &overrides);
        assert!(effective.mutates_state);
        assert!(!effective.is_strictly_read_only());
    }

    // -----------------------------------------------------------------
    // SPEC-001 (TASK-001): daemon-staged phase skills dir resolution
    // -----------------------------------------------------------------

    /// Build a `RuntimeConfigContext` whose `implementation` phase requests
    /// exactly `skills` (mirrors the fixture shape of
    /// `resolve_phase_skills_reports_missing_skill_in_runtime_path`).
    fn ctx_with_phase_skills(skills: Vec<String>) -> RuntimeConfigContext {
        use orchestrator_config::agent_runtime_config::{Idempotency, PhaseExecutionDefinition, PhaseExecutionMode};

        let mut runtime = builtin_agent_runtime_config();
        runtime.phases.insert(
            "implementation".to_string(),
            PhaseExecutionDefinition {
                mode: PhaseExecutionMode::Agent,
                agent_id: Some("default".to_string()),
                directive: None,
                system_prompt: None,
                runtime: None,
                capabilities: None,
                output_contract: None,
                output_json_schema: None,
                decision_contract: None,
                retry: None,
                skills,
                command: None,
                manual: None,
                default_tool: None,
                idempotency: Idempotency::Unknown,
                evals: None,
                worktree: None,
            },
        );
        let workflow = builtin_workflow_config();
        RuntimeConfigContext {
            agent_runtime_config: runtime,
            workflow_config: LoadedWorkflowConfig {
                metadata: WorkflowConfigMetadata {
                    schema: workflow.schema.clone(),
                    version: workflow.version,
                    hash: workflow_config_hash(&workflow),
                    source: WorkflowConfigSource::Builtin,
                },
                config: workflow,
                path: PathBuf::from("fixture"),
            },
        }
    }

    fn write_skill_yaml(dir: &std::path::Path, name: &str, description: &str) {
        std::fs::create_dir_all(dir).expect("skill dir");
        std::fs::write(dir.join(format!("{name}.yaml")), format!("name: {name}\ndescription: {description}\n"))
            .expect("skill yaml");
    }

    /// SPEC-001: a requested skill defined in the staged dir resolves from it
    /// FIRST (tagged as the user tier it mirrors), without touching the project
    /// chain.
    #[test]
    fn resolve_phase_skills_prefers_the_staged_dir() {
        use protocol::test_utils::EnvVarGuard;

        let _lock = crate::test_env::scoped_state_serializer();
        let project = tempfile::tempdir().expect("tempdir");
        let staged = tempfile::tempdir().expect("staged dir");
        write_skill_yaml(staged.path(), "staged-review", "Staged review skill");
        let _gate = EnvVarGuard::set(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV, staged.path().to_str());

        let ctx = ctx_with_phase_skills(vec!["staged-review".to_string()]);
        let resolved = resolve_phase_skills(&ctx, project.path(), "implementation").expect("resolve from staged dir");
        assert_eq!(resolved.requested_skills, vec!["staged-review"]);
        assert_eq!(resolved.resolved_skills.len(), 1);
        assert_eq!(resolved.resolved_skills[0].definition.name, "staged-review");
        assert_eq!(resolved.resolved_skills[0].definition.description, "Staged review skill");
        assert!(
            matches!(resolved.resolved_skills[0].source, SkillSourceOrigin::User),
            "staged definitions are tagged as the user tier they mirror: {:?}",
            resolved.resolved_skills[0].source
        );
    }

    /// SPEC-001: names NOT in the staged dir fall through to the existing
    /// project resolution in the same call, preserving the requested order.
    #[test]
    fn resolve_phase_skills_falls_back_to_project_resolution_for_unstaged_names() {
        use protocol::test_utils::EnvVarGuard;

        let _lock = crate::test_env::scoped_state_serializer();
        let project = tempfile::tempdir().expect("tempdir");
        let staged = tempfile::tempdir().expect("staged dir");
        write_skill_yaml(staged.path(), "staged-only", "Staged-only skill");
        write_skill_yaml(&project.path().join(".animus/config/skill_definitions"), "project-skill", "Project skill");
        let _gate = EnvVarGuard::set(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV, staged.path().to_str());

        let ctx = ctx_with_phase_skills(vec!["project-skill".to_string(), "staged-only".to_string()]);
        let resolved = resolve_phase_skills(&ctx, project.path(), "implementation").expect("mixed resolution");
        assert_eq!(resolved.resolved_skills.len(), 2);
        assert_eq!(resolved.resolved_skills[0].definition.name, "project-skill");
        assert!(matches!(resolved.resolved_skills[0].source, SkillSourceOrigin::Project));
        assert_eq!(resolved.resolved_skills[1].definition.name, "staged-only");
        assert!(matches!(resolved.resolved_skills[1].source, SkillSourceOrigin::User));
    }

    /// SPEC-001: missing-after-both keeps today's hard-fail semantics (the
    /// staged dir does not mask the missing-skill error).
    #[test]
    fn resolve_phase_skills_hard_fails_when_missing_from_staged_dir_and_project() {
        use protocol::test_utils::EnvVarGuard;

        let _lock = crate::test_env::scoped_state_serializer();
        let project = tempfile::tempdir().expect("tempdir");
        let staged = tempfile::tempdir().expect("staged dir");
        write_skill_yaml(staged.path(), "staged-only", "Staged-only skill");
        let _gate = EnvVarGuard::set(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV, staged.path().to_str());

        let ctx = ctx_with_phase_skills(vec!["staged-only".to_string(), "missing-everywhere".to_string()]);
        let error =
            resolve_phase_skills(&ctx, project.path(), "implementation").expect_err("missing skill must hard-fail");
        assert!(error.to_string().contains("missing-everywhere"), "error names the missing skill: {error}");
    }

    /// SPEC-001: with the env var unset (or pointing at a missing dir) the
    /// behavior is byte-identical to the pre-staging project-only path.
    #[test]
    fn resolve_phase_skills_without_staged_dir_env_keeps_project_only_behavior() {
        use protocol::test_utils::EnvVarGuard;

        let _lock = crate::test_env::scoped_state_serializer();
        let project = tempfile::tempdir().expect("tempdir");
        write_skill_yaml(&project.path().join(".animus/config/skill_definitions"), "project-skill", "Project skill");

        let _unset = EnvVarGuard::set(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV, None);
        let ctx = ctx_with_phase_skills(vec!["project-skill".to_string()]);
        let resolved = resolve_phase_skills(&ctx, project.path(), "implementation").expect("project resolution");
        assert!(matches!(resolved.resolved_skills[0].source, SkillSourceOrigin::Project));

        let _missing =
            EnvVarGuard::set(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV, Some("/definitely/missing/animus-skills"));
        let resolved = resolve_phase_skills(&ctx, project.path(), "implementation").expect("project resolution");
        assert!(matches!(resolved.resolved_skills[0].source, SkillSourceOrigin::Project));
    }

    /// SPEC-001 end-to-end-ish over a fake held environment: the sync writes
    /// the staged files through the environment exec channel (the fake node
    /// decodes the base64 stdin exactly like the `sh -c ... base64 -d` command
    /// would), and phase resolution against the node's skill dir then finds the
    /// skill.
    #[test]
    fn staged_skills_sync_to_a_held_node_and_then_resolve() {
        use std::sync::Mutex;

        use animus_session_backend::session::{SessionRequest, SessionRun};
        use protocol::test_utils::EnvVarGuard;

        use crate::phase_environment::{EnvCommandOutput, HeldEnvironment};

        struct CapturedWrite {
            program: String,
            args: Vec<String>,
            stdin: Option<String>,
        }

        struct FakeNode {
            writes: Mutex<Vec<CapturedWrite>>,
        }

        impl HeldEnvironment for FakeNode {
            fn id(&self) -> &str {
                "fake-node"
            }

            fn exec_session(&self, _project_root: &std::path::Path, _request: &SessionRequest) -> Result<SessionRun> {
                anyhow::bail!("unused in this test")
            }

            fn exec_command(
                &self,
                _project_root: &std::path::Path,
                program: &str,
                args: &[String],
                _env: &BTreeMap<String, String>,
                _cwd: Option<&str>,
                stdin: Option<String>,
                _timeout: Option<std::time::Duration>,
            ) -> Result<EnvCommandOutput> {
                self.writes.lock().expect("writes mutex").push(CapturedWrite {
                    program: program.to_string(),
                    args: args.to_vec(),
                    stdin,
                });
                Ok(EnvCommandOutput { exit_code: 0, stdout: String::new(), stderr: String::new(), timed_out: false })
            }
        }

        let _lock = crate::test_env::scoped_state_serializer();
        let project = tempfile::tempdir().expect("tempdir");
        let staged = tempfile::tempdir().expect("staged dir");
        write_skill_yaml(staged.path(), "node-skill", "Skill synced to the node");
        let node_skills = tempfile::tempdir().expect("node skill dir");

        // 1. The handle exists; the sync writes the staged files through it.
        let node = FakeNode { writes: Mutex::new(Vec::new()) };
        {
            let _gate = EnvVarGuard::set(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV, staged.path().to_str());
            crate::skill_dir_sync::sync_staged_skills_to_held_environment(&node, project.path());
        }
        let writes = node.writes.lock().expect("writes mutex");
        assert_eq!(writes.len(), 1, "one staged file -> one node write");
        let write = &writes[0];
        assert_eq!(write.program, "sh");
        assert!(write.args[1].contains("base64 -d > \"$d/node-skill.yaml\""), "write command: {}", write.args[1]);

        // 2. Simulate the node side: decode stdin into the node's user-tier
        //    skill_definitions dir (what `base64 -d > "$d/<name>"` does).
        let stdin_b64 = write.stdin.as_deref().expect("write carries base64 stdin");
        let content = crate::skill_dir_sync::tests::base64_decode(stdin_b64);
        std::fs::write(node_skills.path().join("node-skill.yaml"), &content).expect("materialize on node");
        drop(writes);

        // 3. Phase resolution against the node's dir finds the skill.
        let _gate = EnvVarGuard::set(crate::skill_dir_sync::PHASE_SKILLS_DIR_ENV, node_skills.path().to_str());
        let ctx = ctx_with_phase_skills(vec!["node-skill".to_string()]);
        let resolved = resolve_phase_skills(&ctx, project.path(), "implementation").expect("resolve on node");
        assert_eq!(resolved.resolved_skills.len(), 1);
        assert_eq!(resolved.resolved_skills[0].definition.description, "Skill synced to the node");
    }
}
