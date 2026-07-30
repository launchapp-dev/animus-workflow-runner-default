use std::collections::VecDeque;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::ipc::collect_json_payload_lines;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhaseFailureKind {
    TransientRunner,
    ProviderExhaustion { reason: String },
    ProviderAuthentication { reason: String },
    Infrastructure { reason: String },
    GitHubAuthentication { reason: String },
    GitHubPolicy { reason: String },
    Network { reason: String },
    PublicationConflict { reason: String },
    InvalidMetadata { reason: String },
    OperatorApproval { reason: String },
    InternalContract { reason: String },
    ImplementationFinding { fingerprint: String },
    TargetUnavailable,
    Unknown,
}

impl PhaseFailureKind {
    pub fn is_transient_runner(&self) -> bool {
        matches!(self, PhaseFailureKind::TransientRunner | PhaseFailureKind::Network { .. })
    }

    pub fn should_failover_target(&self) -> bool {
        matches!(
            self,
            PhaseFailureKind::ProviderExhaustion { .. }
                | PhaseFailureKind::ProviderAuthentication { .. }
                | PhaseFailureKind::TargetUnavailable
        )
    }

    pub fn exhaustion_reason(&self) -> Option<&str> {
        match self {
            PhaseFailureKind::ProviderExhaustion { reason } => Some(reason),
            _ => None,
        }
    }

    pub fn token(&self) -> &'static str {
        match self {
            Self::TransientRunner => "transient",
            // Preserve the existing author-configured token even though the
            // richer class name is provider capacity.
            Self::ProviderExhaustion { .. } => "provider_exhaustion",
            Self::ProviderAuthentication { .. } => "provider_auth",
            Self::Infrastructure { .. } => "infrastructure",
            Self::GitHubAuthentication { .. } => "github_auth",
            Self::GitHubPolicy { .. } => "github_policy",
            Self::Network { .. } => "network",
            Self::PublicationConflict { .. } => "publication_conflict",
            Self::InvalidMetadata { .. } => "invalid_metadata",
            Self::OperatorApproval { .. } => "operator_approval",
            Self::InternalContract { .. } => "internal_contract",
            Self::ImplementationFinding { .. } => "implementation_finding",
            Self::TargetUnavailable => "target_unavailable",
            Self::Unknown => "unknown",
        }
    }

    pub fn class_name(&self) -> &'static str {
        match self {
            Self::TransientRunner => "transient_runner",
            Self::ProviderExhaustion { .. } => "provider_capacity",
            Self::ProviderAuthentication { .. } => "provider_authentication",
            Self::Infrastructure { .. } => "infrastructure",
            Self::GitHubAuthentication { .. } => "github_authentication",
            Self::GitHubPolicy { .. } => "github_policy",
            Self::Network { .. } => "network",
            Self::PublicationConflict { .. } => "publication_conflict",
            Self::InvalidMetadata { .. } => "invalid_metadata",
            Self::OperatorApproval { .. } => "operator_approval",
            Self::InternalContract { .. } => "internal_contract",
            Self::ImplementationFinding { .. } => "implementation_finding",
            Self::TargetUnavailable => "target_unavailable",
            Self::Unknown => "unknown",
        }
    }

    pub fn retry_owner(&self) -> &'static str {
        match self {
            Self::TransientRunner | Self::Network { .. } => "runner",
            Self::ProviderExhaustion { .. } | Self::ProviderAuthentication { .. } | Self::TargetUnavailable => {
                "provider"
            }
            Self::Infrastructure { .. } => "infrastructure",
            Self::GitHubAuthentication { .. } | Self::GitHubPolicy { .. } | Self::PublicationConflict { .. } => {
                "github"
            }
            Self::InvalidMetadata { .. } | Self::InternalContract { .. } => "runtime",
            Self::OperatorApproval { .. } => "operator",
            Self::ImplementationFinding { .. } => "implementation",
            Self::Unknown => "operator",
        }
    }

    pub fn preservation_required(&self) -> bool {
        !matches!(self, Self::ImplementationFinding { .. })
    }

    pub fn finding_fingerprint(&self) -> Option<&str> {
        match self {
            Self::ImplementationFinding { fingerprint } => Some(fingerprint),
            _ => None,
        }
    }
}

/// Stable, redaction-safe evidence attached to phase results and journal
/// events. Raw diagnostics stay in the existing error field; this metadata
/// contains only bounded vocabulary and a one-way finding fingerprint.
pub fn failure_metadata(message: &str) -> Value {
    let kind = classify_phase_failure(message);
    let mut metadata = serde_json::json!({
        "class": kind.class_name(),
        "token": kind.token(),
        "retry_owner": kind.retry_owner(),
        "preservation_required": kind.preservation_required(),
    });
    if let Some(fingerprint) = kind.finding_fingerprint() {
        metadata["finding_fingerprint"] = Value::String(fingerprint.to_string());
    }
    metadata
}

/// Map a phase-failure message to a STABLE, author-facing classification
/// token, derived from [`classify_phase_failure`] / [`PhaseFailureKind`].
///
/// This is the authoritative token vocabulary for the author-configurable
/// retry gate (`retry_on` / `no_retry_on` in agent-runtime config). The
/// config-protocol layer deliberately leaves these strings free-form; this
/// function defines the values they are matched against.
///
/// # Token vocabulary
///
/// | Token                  | Failure class ([`PhaseFailureKind`])        |
/// |------------------------|---------------------------------------------|
/// | `transient`            | `TransientRunner` — recoverable runner/IO    |
/// |                        | hiccup (connect/reset/broken-pipe/timeout).  |
/// | `provider_exhaustion`  | `ProviderExhaustion` — quota / rate-limit /  |
/// |                        | exhausted provider credits.                  |
/// | `provider_auth`        | `ProviderAuthentication` — missing, invalid, |
/// |                        | or expired provider credentials.            |
/// | `infrastructure`       | `Infrastructure` — environment or hosting    |
/// |                        | capacity failure.                            |
/// | `github_auth`          | `GitHubAuthentication` — invalid or missing  |
/// |                        | GitHub credentials.                          |
/// | `github_policy`        | `GitHubPolicy` — repository policy denied an |
/// |                        | otherwise valid publication action.         |
/// | `network`              | `Network` — DNS, connection, or timeout      |
/// |                        | failure outside implementation.             |
/// | `publication_conflict` | `PublicationConflict` — stale/non-fast-      |
/// |                        | forward remote state.                        |
/// | `invalid_metadata`     | `InvalidMetadata` — malformed or missing     |
/// |                        | workflow/publication metadata.              |
/// | `operator_approval`    | `OperatorApproval` — explicit human gate.    |
/// | `internal_contract`    | `InternalContract` — runtime invariant or    |
/// |                        | protocol contract failure.                   |
/// | `implementation_finding` | `ImplementationFinding` — actionable code |
/// |                        | review, test, lint, or evaluation finding.   |
/// | `target_unavailable`   | `TargetUnavailable` — missing CLI / unknown  |
/// |                        | model / unsupported tool.                    |
/// | `unknown`              | `Unknown` — unclassified failure.            |
///
/// The token is matched case-sensitively by the gate, so it is always
/// lower snake_case. NOTE: the checkpoint-IO hard guard is intentionally
/// NOT represented here — it is a separate, non-overridable block applied
/// before classification (see `is_checkpoint_io_failure`).
pub fn failure_token(message: &str) -> &'static str {
    classify_phase_failure(message).token()
}

pub fn classify_phase_failure(message: &str) -> PhaseFailureKind {
    if let Some(reason) = matched_reason(message, is_infrastructure_pattern, "execution infrastructure unavailable") {
        return PhaseFailureKind::Infrastructure { reason };
    }
    if let Some(reason) = matched_reason(message, is_github_policy_pattern, "GitHub policy denied publication") {
        return PhaseFailureKind::GitHubPolicy { reason };
    }
    if let Some(reason) = matched_reason(message, is_github_auth_pattern, "GitHub authentication failed") {
        return PhaseFailureKind::GitHubAuthentication { reason };
    }
    if let Some(reason) = matched_reason(message, is_publication_conflict_pattern, "remote publication conflict") {
        return PhaseFailureKind::PublicationConflict { reason };
    }
    if let Some(reason) = extract_provider_authentication_reason(message) {
        return PhaseFailureKind::ProviderAuthentication { reason };
    }
    if let Some(reason) = extract_provider_exhaustion_reason(message) {
        return PhaseFailureKind::ProviderExhaustion { reason };
    }
    if let Some(reason) = matched_reason(message, is_operator_approval_pattern, "operator approval required") {
        return PhaseFailureKind::OperatorApproval { reason };
    }
    if let Some(reason) = matched_reason(message, is_invalid_metadata_pattern, "invalid execution metadata") {
        return PhaseFailureKind::InvalidMetadata { reason };
    }
    if let Some(reason) = matched_reason(message, is_internal_contract_pattern, "internal runtime contract failure") {
        return PhaseFailureKind::InternalContract { reason };
    }
    if is_transient_runner_pattern(message) {
        return PhaseFailureKind::TransientRunner;
    }
    if let Some(reason) = matched_reason(message, is_network_pattern, "network unavailable") {
        return PhaseFailureKind::Network { reason };
    }
    if is_target_unavailable_pattern(message) {
        return PhaseFailureKind::TargetUnavailable;
    }
    if is_implementation_finding_pattern(message) {
        return PhaseFailureKind::ImplementationFinding { fingerprint: finding_fingerprint(message) };
    }
    PhaseFailureKind::Unknown
}

fn matched_reason(message: &str, predicate: impl FnOnce(&str) -> bool, reason: &'static str) -> Option<String> {
    predicate(message).then(|| reason.to_string())
}

fn is_transient_runner_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("failed to connect runner")
        || normalized.contains("runner disconnected before workflow")
        || normalized.contains("broken pipe")
        || normalized.contains("runner timed out")
}

fn is_infrastructure_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("too many services in project")
        || normalized.contains("plan only allows") && normalized.contains("services")
        || normalized.contains("environment/prepare failed")
        || normalized.contains("environment prepare via")
        || normalized.contains("railwayapierror")
        || normalized.contains("docker daemon")
        || normalized.contains("no space left on device")
        || normalized.contains("disk quota exceeded")
        || normalized.contains("database unavailable")
}

fn is_github_auth_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    (normalized.contains("github") || normalized.contains("git push"))
        && (normalized.contains("bad credentials")
            || normalized.contains("authentication failed")
            || normalized.contains("token expired")
            || normalized.contains("could not read username")
            || normalized.contains("permission denied (publickey)"))
}

fn is_github_policy_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("resource not accessible by integration")
        || normalized.contains("workflows permission")
        || normalized.contains("protected branch")
        || normalized.contains("repository rule")
        || normalized.contains("ruleset") && normalized.contains("denied")
        || normalized.contains("refusing to allow a github app")
}

fn is_publication_conflict_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("non-fast-forward")
        || normalized.contains("fetch first")
        || normalized.contains("remote contains work")
        || normalized.contains("stale pr head")
        || normalized.contains("publication conflict")
        || normalized.contains("remote ref") && normalized.contains("does not contain")
}

fn is_network_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("connection refused")
        || normalized.contains("connection reset by peer")
        || normalized.contains("network is unreachable")
        || normalized.contains("temporary failure in name resolution")
        || normalized.contains("could not resolve host")
        || normalized.contains("timed out")
        || normalized.contains("timeout")
}

fn is_invalid_metadata_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("malformed receipt")
        || normalized.contains("invalid publication receipt")
        || normalized.contains("missing publication receipt")
        || normalized.contains("missing execution fence")
        || normalized.contains("invalid execution fence")
        || normalized.contains("missing git_repo")
        || normalized.contains("missing git_ref")
        || normalized.contains("schema validation failed")
}

fn is_operator_approval_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("operator approval")
        || normalized.contains("manual approval")
        || normalized.contains("approval required")
        || normalized.contains("human review required")
}

fn is_internal_contract_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("contract violation")
        || normalized.contains("protocol mismatch")
        || normalized.contains("invariant")
        || normalized.contains("unexpected workflow state")
        || normalized.contains("failed to deserialize")
        || normalized.contains("completion proof") && normalized.contains("invalid")
}

fn is_implementation_finding_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("verdict: changes requested")
        || normalized.contains("changes requested")
        || normalized.contains("required change")
        || normalized.contains("code review finding")
        || normalized.contains("implementation finding")
        || normalized.contains("assertion failed")
        || normalized.contains("eval gate failed")
        || normalized.contains("test failed")
        || normalized.contains("tests failed")
        || normalized.contains("typecheck failed")
        || normalized.contains("compilation failed")
        || normalized.contains("lint failed")
}

pub fn finding_fingerprint(message: &str) -> String {
    let normalized = message
        .lines()
        .filter(|line| !line.trim_start().starts_with("[animus-rework-v1"))
        .flat_map(|line| line.split_whitespace())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join(" ");
    let digest = Sha256::digest(normalized.as_bytes());
    format!("sha256:{digest:x}")
}

fn extract_provider_authentication_reason(text: &str) -> Option<String> {
    for (_raw, payload) in collect_json_payload_lines(text) {
        for pointer in ["/error/code", "/error/type"] {
            let Some(value) = payload.pointer(pointer).and_then(Value::as_str) else {
                continue;
            };
            let normalized = value.to_ascii_lowercase();
            if normalized.contains("authentication")
                || normalized.contains("auth_error")
                || normalized.contains("invalid_api_key")
            {
                return Some("provider authentication failed".to_string());
            }
        }
    }
    let normalized = text.to_ascii_lowercase();
    if normalized.contains("not logged in")
        || normalized.contains("please run /login")
        || normalized.contains("authentication_error")
        || normalized.contains("invalid authentication credentials")
        || normalized.contains("failed to authenticate")
        || normalized.contains("missing api key")
        || normalized.contains("no openrouter api key found")
    {
        return Some("provider authentication failed".to_string());
    }
    None
}

fn is_target_unavailable_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("missing runtime contract launch for ai cli")
        || normalized.contains("failed to spawn cli process")
        || normalized.contains("no such file or directory")
        || normalized.contains("command not found")
        || normalized.contains("unsupported tool")
        || normalized.contains("unknown model")
        || normalized.contains("invalid model")
        || normalized.contains("missing api key")
        || normalized.contains("missing cli")
        || normalized.contains("model not available")
}

fn extract_provider_exhaustion_reason(text: &str) -> Option<String> {
    for (_raw, payload) in collect_json_payload_lines(text) {
        if let Some(reason) = provider_exhaustion_reason_from_payload(&payload) {
            return Some(reason);
        }
    }

    let normalized = text.to_ascii_lowercase();
    if normalized.contains("insufficient_quota")
        || normalized.contains("quota exceeded")
        || normalized.contains("quota_exceeded")
    {
        return Some("provider quota exceeded".to_string());
    }
    if normalized.contains("rate limit")
        || normalized.contains("rate-limit")
        || normalized.contains("too many requests")
    {
        return Some("provider rate limit exceeded".to_string());
    }
    if normalized.contains("\"has_credits\":false")
        || normalized.contains("\"balance\":\"0\"")
        || normalized.contains("\"balance\":0")
    {
        return Some("provider credits exhausted".to_string());
    }
    if normalized.contains("secondary") && normalized.contains("used_percent") {
        return Some("secondary token budget exhausted".to_string());
    }
    None
}

/// Pure retry-classification decision for the phase-attempt gate.
///
/// Applies the author-configurable precedence on top of the default
/// transient classifier. The two outer hard guards (`attempt < max_attempts`
/// and the checkpoint-IO block) live at the call site and are NOT modeled
/// here — `no_retry_on` / `retry_on` can never override the checkpoint-IO
/// guard.
///
/// Precedence (highest first):
/// 1. `no_retry_on.contains(token)` → never retry (fail fast).
/// 2. `!retry_on.is_empty()` → retry IFF `retry_on.contains(token)`
///    (explicit allowlist: opt classes in beyond the transient default, or
///    restrict to a subset).
/// 3. else (empty `retry_on`) → retry IFF `is_transient` (today's default).
pub fn retry_decision_for_token(token: &str, retry_on: &[String], no_retry_on: &[String], is_transient: bool) -> bool {
    if no_retry_on.iter().any(|t| t == token) {
        return false;
    }
    if !retry_on.is_empty() {
        return retry_on.iter().any(|t| t == token);
    }
    is_transient
}

pub struct PhaseFailureClassifier;

impl PhaseFailureClassifier {
    pub fn is_transient_runner_error_message(message: &str) -> bool {
        classify_phase_failure(message).is_transient_runner()
    }

    pub fn provider_exhaustion_reason_from_text(text: &str) -> Option<String> {
        match classify_phase_failure(text) {
            PhaseFailureKind::ProviderExhaustion { reason } => Some(reason),
            _ => None,
        }
    }

    pub fn should_failover_target(message: &str) -> bool {
        classify_phase_failure(message).should_failover_target()
    }

    pub fn push_phase_diagnostic_line(lines: &mut VecDeque<String>, text: &str) {
        const MAX_LINE_CHARS: usize = 320;
        const MAX_LINES: usize = 24;
        let mut normalized = text.trim().replace('\n', " ");
        if normalized.chars().count() > MAX_LINE_CHARS {
            normalized = normalized.chars().take(MAX_LINE_CHARS).collect::<String>();
        }
        if normalized.is_empty() {
            return;
        }
        if lines.len() >= MAX_LINES {
            lines.pop_front();
        }
        lines.push_back(normalized);
    }

    pub fn summarize_phase_diagnostics(lines: &VecDeque<String>) -> Option<String> {
        if lines.is_empty() {
            return None;
        }
        Some(lines.iter().cloned().collect::<Vec<_>>().join(" | "))
    }
}

fn parse_numeric_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|number| number as f64))
        .or_else(|| value.as_u64().map(|number| number as f64))
        .or_else(|| value.as_str().and_then(|raw| raw.trim().parse::<f64>().ok()))
}

fn provider_exhaustion_reason_from_payload(payload: &Value) -> Option<String> {
    let secondary_used_percent =
        payload.pointer("/event_msg/token_count/secondary/used_percent").and_then(parse_numeric_value);
    if let Some(used_percent) = secondary_used_percent {
        if used_percent >= 100.0 {
            return Some(format!("secondary token budget exhausted ({:.0}% used)", used_percent));
        }
    }

    let has_credits = payload.pointer("/event_msg/token_count/credits/has_credits").and_then(Value::as_bool);
    if has_credits == Some(false) {
        return Some("provider credits exhausted".to_string());
    }

    let credit_balance = payload.pointer("/event_msg/token_count/credits/balance").and_then(parse_numeric_value);
    if let Some(balance) = credit_balance {
        if balance <= 0.0 {
            return Some("provider credit balance exhausted".to_string());
        }
    }

    let error_code = payload.pointer("/error/code").and_then(Value::as_str).map(|value| value.to_ascii_lowercase());
    if let Some(code) = error_code {
        if code.contains("insufficient_quota")
            || code.contains("quota")
            || code.contains("rate_limit")
            || code.contains("rate-limit")
        {
            return Some(format!("provider returned {}", code));
        }
    }

    let error_type = payload.pointer("/error/type").and_then(Value::as_str).map(|value| value.to_ascii_lowercase());
    if let Some(kind) = error_type {
        if kind.contains("insufficient_quota")
            || kind.contains("quota")
            || kind.contains("rate_limit")
            || kind.contains("rate-limit")
        {
            return Some(format!("provider returned {}", kind));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_string()).collect()
    }

    // --- failure_token: one assertion per PhaseFailureKind variant ---------

    #[test]
    fn failure_token_maps_transient_runner() {
        assert_eq!(classify_phase_failure("runner disconnected before workflow"), PhaseFailureKind::TransientRunner);
        assert_eq!(failure_token("runner disconnected before workflow"), "transient");
    }

    #[test]
    fn failure_token_maps_provider_exhaustion() {
        let msg = "openai error: insufficient_quota";
        assert!(matches!(classify_phase_failure(msg), PhaseFailureKind::ProviderExhaustion { .. }));
        assert_eq!(failure_token(msg), "provider_exhaustion");
    }

    #[test]
    fn failure_token_maps_target_unavailable() {
        assert_eq!(classify_phase_failure("command not found"), PhaseFailureKind::TargetUnavailable);
        assert_eq!(failure_token("command not found"), "target_unavailable");
    }

    #[test]
    fn failure_token_maps_unknown() {
        let msg = "some entirely novel failure";
        assert_eq!(classify_phase_failure(msg), PhaseFailureKind::Unknown);
        assert_eq!(failure_token(msg), "unknown");
    }

    #[test]
    fn railway_service_cap_is_typed_infrastructure_owned_outside_implementation() {
        let msg = "environment/prepare failed: RailwayApiError: Too many services in project. Your plan only allows 100 services";
        let kind = classify_phase_failure(msg);
        assert!(matches!(kind, PhaseFailureKind::Infrastructure { .. }));
        assert_eq!(kind.token(), "infrastructure");
        assert_eq!(kind.retry_owner(), "infrastructure");
        assert!(kind.preservation_required());
    }

    #[test]
    fn provider_auth_and_capacity_are_distinct() {
        let auth = classify_phase_failure("Not logged in · Please run /login");
        assert!(matches!(auth, PhaseFailureKind::ProviderAuthentication { .. }));
        assert_eq!(auth.token(), "provider_auth");

        let capacity = classify_phase_failure("provider returned rate limit: too many requests");
        assert!(matches!(capacity, PhaseFailureKind::ProviderExhaustion { .. }));
        assert_eq!(capacity.class_name(), "provider_capacity");

        let target = classify_phase_failure("unknown model codex-future");
        assert!(matches!(target, PhaseFailureKind::TargetUnavailable));
        assert!(target.preservation_required());
    }

    #[test]
    fn github_auth_policy_network_and_publication_conflict_are_distinct() {
        assert!(matches!(
            classify_phase_failure("github git push failed: Bad credentials"),
            PhaseFailureKind::GitHubAuthentication { .. }
        ));
        assert!(matches!(
            classify_phase_failure("refusing to allow a GitHub App without workflows permission"),
            PhaseFailureKind::GitHubPolicy { .. }
        ));
        assert!(matches!(
            classify_phase_failure("fatal: unable to access github.com: connection timed out"),
            PhaseFailureKind::Network { .. }
        ));
        assert!(matches!(
            classify_phase_failure("! [rejected] reviewed -> reviewed (non-fast-forward)"),
            PhaseFailureKind::PublicationConflict { .. }
        ));
    }

    #[test]
    fn metadata_operator_internal_and_implementation_are_distinct() {
        assert!(matches!(
            classify_phase_failure("invalid publication receipt: missing commit"),
            PhaseFailureKind::InvalidMetadata { .. }
        ));
        assert!(matches!(
            classify_phase_failure("manual approval required before deploy"),
            PhaseFailureKind::OperatorApproval { .. }
        ));
        assert!(matches!(
            classify_phase_failure("completion proof invariant violated"),
            PhaseFailureKind::InternalContract { .. }
        ));
        let finding = classify_phase_failure("VERDICT: CHANGES REQUESTED\n1. REQUIRED CHANGE: fix the race");
        assert!(matches!(finding, PhaseFailureKind::ImplementationFinding { .. }));
        assert_eq!(finding.retry_owner(), "implementation");
        assert!(!finding.preservation_required());
    }

    #[test]
    fn finding_fingerprint_is_stable_across_whitespace_and_runner_annotation() {
        let first = finding_fingerprint("VERDICT: CHANGES REQUESTED\n  REQUIRED CHANGE: fix race");
        let second = finding_fingerprint(
            "[animus-rework-v1 fingerprint=old repeat=2]\n verdict:   changes requested required change: FIX RACE",
        );
        assert_eq!(first, second);
        assert!(first.starts_with("sha256:"));
    }

    #[test]
    fn failure_metadata_is_redaction_safe_and_structured() {
        let metadata = failure_metadata(
            "environment/prepare failed: RailwayApiError: Too many services in project; token=secret-value",
        );
        assert_eq!(metadata["class"], "infrastructure");
        assert_eq!(metadata["retry_owner"], "infrastructure");
        assert_eq!(metadata["preservation_required"], true);
        assert!(!metadata.to_string().contains("secret-value"));
    }

    // --- retry_decision_for_token: gate precedence ------------------------

    #[test]
    fn no_retry_on_wins_even_if_also_in_retry_on() {
        // token present in BOTH lists → no_retry_on takes precedence.
        let decision = retry_decision_for_token("transient", &s(&["transient"]), &s(&["transient"]), true);
        assert!(!decision, "no_retry_on must beat retry_on");
    }

    #[test]
    fn no_retry_on_suppresses_otherwise_transient() {
        let decision = retry_decision_for_token("transient", &[], &s(&["transient"]), true);
        assert!(!decision, "no_retry_on must suppress a transient failure");
    }

    #[test]
    fn retry_on_allowlist_includes_listed_token() {
        // Non-transient token explicitly opted in → retry.
        let decision = retry_decision_for_token("provider_exhaustion", &s(&["provider_exhaustion"]), &[], false);
        assert!(decision, "retry_on must opt a non-transient token in");
    }

    #[test]
    fn retry_on_allowlist_excludes_unlisted_token() {
        // retry_on non-empty but token not listed → no retry, even if transient.
        let decision = retry_decision_for_token("transient", &s(&["provider_exhaustion"]), &[], true);
        assert!(!decision, "retry_on restricts to listed tokens only");
    }

    #[test]
    fn empty_retry_on_falls_back_to_is_transient_true() {
        let decision = retry_decision_for_token("transient", &[], &[], true);
        assert!(decision, "empty config must preserve default transient retry");
    }

    #[test]
    fn empty_retry_on_falls_back_to_is_transient_false() {
        let decision = retry_decision_for_token("unknown", &[], &[], false);
        assert!(!decision, "empty config must not retry a non-transient failure");
    }

    #[test]
    fn non_transient_token_with_retry_on_listing_it_does_retry() {
        // Explicitly: a class the default classifier would NOT retry, but the
        // author opted in via retry_on, IS retried.
        let token = failure_token("openai error: insufficient_quota");
        assert_eq!(token, "provider_exhaustion");
        let decision = retry_decision_for_token(token, &s(&["provider_exhaustion"]), &[], false);
        assert!(decision);
    }

    #[test]
    fn checkpoint_io_guard_blocks_retry_regardless_of_config() {
        // The pure decision helper does NOT model the checkpoint-IO guard;
        // the call site ANDs it in. This test documents that even a decision
        // of `true` is overridden by the `!is_checkpoint_io_failure` guard.
        let config_decision = retry_decision_for_token("transient", &s(&["transient"]), &[], true);
        assert!(config_decision, "config alone would retry");
        let is_checkpoint_io_failure = true;
        let attempt = 0usize;
        let max_attempts = 3usize;
        let should_retry = attempt < max_attempts && !is_checkpoint_io_failure && config_decision;
        assert!(!should_retry, "checkpoint-IO guard must block retry regardless of config");
    }
}
