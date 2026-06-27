use std::collections::VecDeque;

use serde_json::Value;

use crate::ipc::collect_json_payload_lines;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhaseFailureKind {
    TransientRunner,
    ProviderExhaustion { reason: String },
    TargetUnavailable,
    Unknown,
}

impl PhaseFailureKind {
    pub fn is_transient_runner(&self) -> bool {
        matches!(self, PhaseFailureKind::TransientRunner)
    }

    pub fn should_failover_target(&self) -> bool {
        matches!(self, PhaseFailureKind::ProviderExhaustion { .. } | PhaseFailureKind::TargetUnavailable)
    }

    pub fn exhaustion_reason(&self) -> Option<&str> {
        match self {
            PhaseFailureKind::ProviderExhaustion { reason } => Some(reason),
            _ => None,
        }
    }
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
/// |                        | credits / auth provider exhaustion.          |
/// | `target_unavailable`   | `TargetUnavailable` — missing CLI / unknown  |
/// |                        | model / missing key / unsupported tool.      |
/// | `unknown`              | `Unknown` — unclassified failure.            |
///
/// The token is matched case-sensitively by the gate, so it is always
/// lower snake_case. NOTE: the checkpoint-IO hard guard is intentionally
/// NOT represented here — it is a separate, non-overridable block applied
/// before classification (see `is_checkpoint_io_failure`).
pub fn failure_token(message: &str) -> &'static str {
    match classify_phase_failure(message) {
        PhaseFailureKind::TransientRunner => "transient",
        PhaseFailureKind::ProviderExhaustion { .. } => "provider_exhaustion",
        PhaseFailureKind::TargetUnavailable => "target_unavailable",
        PhaseFailureKind::Unknown => "unknown",
    }
}

pub fn classify_phase_failure(message: &str) -> PhaseFailureKind {
    if is_transient_runner_pattern(message) {
        return PhaseFailureKind::TransientRunner;
    }
    if let Some(reason) = extract_provider_exhaustion_reason(message) {
        return PhaseFailureKind::ProviderExhaustion { reason };
    }
    if is_target_unavailable_pattern(message) {
        return PhaseFailureKind::TargetUnavailable;
    }
    PhaseFailureKind::Unknown
}

fn is_transient_runner_pattern(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("failed to connect runner")
        || normalized.contains("runner disconnected before workflow")
        || normalized.contains("connection refused")
        || normalized.contains("connection reset by peer")
        || normalized.contains("broken pipe")
        || normalized.contains("timed out")
        || normalized.contains("timeout")
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
    if normalized.contains("authentication_error")
        || normalized.contains("invalid authentication credentials")
        || normalized.contains("failed to authenticate")
    {
        return Some("provider authentication failed".to_string());
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
            || kind.contains("authentication_error")
            || kind.contains("auth_error")
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
        assert_eq!(classify_phase_failure("connection reset by peer"), PhaseFailureKind::TransientRunner);
        assert_eq!(failure_token("connection reset by peer"), "transient");
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
        let msg = "schema validation failed: missing field foo";
        assert_eq!(classify_phase_failure(msg), PhaseFailureKind::Unknown);
        assert_eq!(failure_token(msg), "unknown");
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
