//! Normalize mechanically-derivable `phase_decision` fields before terminal
//! schema validation (TASK-1299).
//!
//! The runner validates an agent phase's terminal payload AFTER the agent's
//! external side effects are complete, and hard-fails the whole run on ANY
//! mismatch — so a verified external success (pr-review run `2581ce29`:
//! approved and squash-merged with a correct terminal object) was bookkept
//! FAILED because its `phase_decision` omitted fields the runner itself
//! already knows (`kind`, `phase_id`), fields the schema derives from the
//! reported outcome (`verdict`, via the composed schema's oneOf branches that
//! pin a verdict per outcome), or bookkeeping the payload already carries in
//! another field (`reason` from `summary`). This pass fills exactly those
//! mechanical fields on the validation payload and reports what it filled so
//! the caller can journal a contract warning.
//!
//! Substantive violations are deliberately untouched and still hard-fail:
//! a wrong result `kind`, an invalid `outcome`, missing outcome artifacts
//! (e.g. `merge_sha` on a merged outcome), a non-object `phase_decision`, or
//! an emitted verdict that contradicts the outcome mapping.

use serde_json::{Map, Value};

/// Rank risks so "conservative" means the riskiest value the schema allows.
fn risk_rank(risk: &str) -> u8 {
    match risk {
        "low" => 0,
        "medium" => 1,
        "high" => 2,
        _ => 0,
    }
}

/// Read a pinned string from a property schema: `{"const": "x"}` or a
/// single-element `{"enum": ["x"]}`.
fn pinned_string(property: &Value) -> Option<&str> {
    if let Some(value) = property.get("const").and_then(Value::as_str) {
        return Some(value);
    }
    match property.get("enum").and_then(Value::as_array) {
        Some(values) if values.len() == 1 => values[0].as_str(),
        _ => None,
    }
}

/// Derive the verdict the composed schema itself pins for the payload's
/// reported outcome. Only derives when the mapping is unambiguous: every
/// oneOf/anyOf branch pins an outcome, exactly one branch matches the
/// payload's outcome, and that branch pins a verdict. Anything else — no
/// branches, an unpinned branch, zero or multiple matches, or a matching
/// branch without a verdict pin — refuses to derive.
fn derive_verdict_from_outcome(payload: &Value, schema: &Value) -> Option<String> {
    let outcome = payload.get("outcome").and_then(Value::as_str)?;
    let branches = schema.get("oneOf").or_else(|| schema.get("anyOf")).and_then(Value::as_array)?;
    let mut matched: Option<&Value> = None;
    for branch in branches {
        let pinned_outcome = pinned_string(branch.pointer("/properties/outcome")?)?;
        if pinned_outcome == outcome {
            if matched.is_some() {
                return None;
            }
            matched = Some(branch);
        }
    }
    matched
        .and_then(|branch| branch.pointer("/properties/phase_decision/properties/verdict"))
        .and_then(pinned_string)
        .map(ToOwned::to_owned)
}

/// The riskiest risk value the decision schema allows (its `risk` enum is
/// generated lowest-to-highest from the contract's max_risk); "high" when the
/// schema does not constrain it.
fn conservative_risk(decision_schema: &Value) -> String {
    decision_schema
        .pointer("/properties/risk/enum")
        .and_then(Value::as_array)
        .and_then(|values| {
            values.iter().filter_map(Value::as_str).max_by_key(|risk| risk_rank(risk)).map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "high".to_string())
}

/// Fill mechanically-derivable `phase_decision` fields on `payload` before it
/// is validated against `schema` (the composed phase-response schema).
/// Returns the names of the fields that were filled — empty when nothing was
/// touched. Existing values are never overwritten; a `phase_decision` that is
/// present but not an object is left alone for validation to reject.
///
/// A `phase_decision` that is missing entirely is created only when the fill
/// would be fully mechanical (a schema-derived verdict AND a reason from
/// `summary`); otherwise the payload is left untouched so validation reports
/// the missing envelope.
pub fn normalize_phase_decision(payload: &mut Value, phase_id: &str, schema: &Value) -> Vec<String> {
    let mut filled: Vec<String> = Vec::new();

    let Some(decision_schema) = schema.pointer("/properties/phase_decision") else {
        return filled;
    };
    if !payload.is_object() {
        return filled;
    }

    let derived_verdict = derive_verdict_from_outcome(payload, schema);
    let summary_reason = payload
        .get("summary")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|summary| !summary.is_empty())
        .map(ToOwned::to_owned);
    let minimum_confidence =
        decision_schema.pointer("/properties/confidence/minimum").and_then(Value::as_f64).unwrap_or(0.0);
    let fallback_risk = conservative_risk(decision_schema);

    let object = payload.as_object_mut().expect("checked is_object above");
    match object.get("phase_decision") {
        Some(Value::Object(_)) => {}
        Some(_) => return filled,
        None => {
            if derived_verdict.is_none() || summary_reason.is_none() {
                return filled;
            }
            object.insert("phase_decision".to_string(), Value::Object(Map::new()));
            filled.push("phase_decision".to_string());
        }
    }
    let decision = object
        .get_mut("phase_decision")
        .and_then(Value::as_object_mut)
        .expect("phase_decision ensured to be an object above");

    if !decision.contains_key("kind") {
        decision.insert("kind".to_string(), Value::String("phase_decision".to_string()));
        filled.push("kind".to_string());
    }
    if !decision.contains_key("phase_id") {
        decision.insert("phase_id".to_string(), Value::String(phase_id.to_string()));
        filled.push("phase_id".to_string());
    }
    if !decision.contains_key("verdict") {
        if let Some(verdict) = derived_verdict {
            decision.insert("verdict".to_string(), Value::String(verdict));
            filled.push("verdict".to_string());
        }
    }
    if !decision.contains_key("reason") {
        if let Some(reason) = summary_reason {
            decision.insert("reason".to_string(), Value::String(reason));
            filled.push("reason".to_string());
        }
    }
    if !decision.contains_key("confidence") {
        decision.insert("confidence".to_string(), serde_json::json!(minimum_confidence));
        filled.push("confidence".to_string());
    }
    if !decision.contains_key("risk") {
        decision.insert("risk".to_string(), Value::String(fallback_risk));
        filled.push("risk".to_string());
    }

    filled
}

#[cfg(test)]
mod tests {
    use super::normalize_phase_decision;
    use crate::phase_executor::validate_basic_json_schema;
    use serde_json::{json, Value};

    // Mirrors the composed pr-review phase-response schema: the portal's
    // explicit terminal schema (system-pr-orchestration.ts) with the generic
    // decision schema substituted for phase_decision and the pr-review
    // decision contract's confidence floor.
    fn pr_review_like_schema() -> Value {
        json!({
            "type": "object",
            "required": ["kind", "outcome", "expected_head_sha", "summary", "phase_decision"],
            "properties": {
                "kind": { "const": "animus.pr-review-terminal.v1" },
                "outcome": {
                    "enum": ["review_recorded", "merged", "pending_ci", "stale_head", "blocked_error", "invalid_input"]
                },
                "expected_head_sha": { "type": "string" },
                "summary": { "type": "string", "minLength": 1 },
                "review_id": { "type": "string" },
                "merge_sha": { "type": "string" },
                "phase_decision": {
                    "type": "object",
                    "required": ["kind", "phase_id", "verdict", "confidence", "risk", "reason"],
                    "properties": {
                        "kind": { "const": "phase_decision" },
                        "phase_id": { "const": "pr-review" },
                        "verdict": { "enum": ["advance", "rework", "fail", "skip"] },
                        "confidence": { "type": "number", "minimum": 0.99, "maximum": 1.0 },
                        "risk": { "enum": ["low", "medium", "high"] },
                        "reason": { "type": "string", "minLength": 1 }
                    },
                    "additionalProperties": true
                }
            },
            "oneOf": [
                {
                    "properties": {
                        "outcome": { "const": "review_recorded" },
                        "phase_decision": { "properties": { "verdict": { "const": "advance" } } }
                    },
                    "required": ["review_id"]
                },
                {
                    "properties": {
                        "outcome": { "const": "merged" },
                        "phase_decision": { "properties": { "verdict": { "const": "advance" } } }
                    },
                    "required": ["merge_sha"]
                },
                {
                    "properties": {
                        "outcome": { "const": "blocked_error" },
                        "phase_decision": { "properties": { "verdict": { "const": "fail" } } }
                    }
                }
            ],
            "additionalProperties": true
        })
    }

    // The exact failure shape from run 2581ce29: a fully verified merged
    // outcome whose phase_decision carries only {verdict, reason, evidence}.
    fn run_2581ce29_payload() -> Value {
        json!({
            "kind": "animus.pr-review-terminal.v1",
            "outcome": "merged",
            "expected_head_sha": "0c470f4e8adbecf4d40d7bb904e39d05fabf83a5",
            "summary": "exact-head APPROVE posted and PR squash-merged",
            "merge_sha": "8f4a6f8e6f2f4f0b9d3d0f0e8b7a5c4d3e2f1a0b",
            "phase_decision": {
                "verdict": "advance",
                "reason": "exact-head APPROVE posted and PR squash-merged",
                "evidence": []
            }
        })
    }

    #[test]
    fn verified_merged_payload_with_partial_decision_validates_after_normalization() {
        let schema = pr_review_like_schema();
        let mut payload = run_2581ce29_payload();
        assert!(
            validate_basic_json_schema(&payload, &schema).is_err(),
            "un-normalized payload must fail (the TASK-1291 false-failure)"
        );

        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert_eq!(filled, vec!["kind", "phase_id", "confidence", "risk"]);
        validate_basic_json_schema(&payload, &schema).expect("normalized payload must validate");
    }

    #[test]
    fn existing_decision_fields_are_never_overwritten() {
        let schema = pr_review_like_schema();
        let mut payload = run_2581ce29_payload();
        let decision = payload["phase_decision"].as_object_mut().unwrap();
        decision.insert("kind".to_string(), json!("phase_decision"));
        decision.insert("phase_id".to_string(), json!("pr-review"));
        decision.insert("confidence".to_string(), json!(1.0));
        decision.insert("risk".to_string(), json!("low"));

        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(filled.is_empty(), "nothing to fill: {filled:?}");
        assert_eq!(payload["phase_decision"]["confidence"], json!(1.0));
        assert_eq!(payload["phase_decision"]["risk"], json!("low"));
    }

    #[test]
    fn verdict_derives_from_unambiguous_outcome_mapping() {
        let schema = pr_review_like_schema();
        let mut payload = run_2581ce29_payload();
        payload["phase_decision"].as_object_mut().unwrap().remove("verdict");

        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(filled.contains(&"verdict".to_string()));
        assert_eq!(payload["phase_decision"]["verdict"], json!("advance"));
        validate_basic_json_schema(&payload, &schema).expect("derived verdict must validate");
    }

    #[test]
    fn verdict_derivation_refuses_ambiguous_mappings() {
        let mut schema = pr_review_like_schema();
        // Two branches pin the same outcome: the mapping is ambiguous.
        let branches = schema["oneOf"].as_array_mut().unwrap();
        let duplicate = branches[1].clone();
        branches.push(duplicate);

        let mut payload = run_2581ce29_payload();
        payload["phase_decision"].as_object_mut().unwrap().remove("verdict");

        normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(payload["phase_decision"].get("verdict").is_none(), "ambiguous mapping must not derive a verdict");
        assert!(validate_basic_json_schema(&payload, &schema).is_err());
    }

    #[test]
    fn verdict_derivation_refuses_branches_without_outcome_pins() {
        let mut schema = pr_review_like_schema();
        schema["oneOf"].as_array_mut().unwrap().push(json!({
            "properties": { "phase_decision": { "properties": { "verdict": { "const": "fail" } } } }
        }));

        let mut payload = run_2581ce29_payload();
        payload["phase_decision"].as_object_mut().unwrap().remove("verdict");

        normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(payload["phase_decision"].get("verdict").is_none(), "an unpinned branch makes every mapping ambiguous");
    }

    #[test]
    fn conservative_defaults_use_schema_confidence_floor_and_riskiest_allowed_risk() {
        let schema = pr_review_like_schema();
        let mut payload = run_2581ce29_payload();

        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(filled.contains(&"confidence".to_string()));
        assert!(filled.contains(&"risk".to_string()));
        assert_eq!(payload["phase_decision"]["confidence"], json!(0.99));
        assert_eq!(payload["phase_decision"]["risk"], json!("high"));
    }

    #[test]
    fn reason_defaults_from_summary() {
        let schema = pr_review_like_schema();
        let mut payload = run_2581ce29_payload();
        payload["phase_decision"].as_object_mut().unwrap().remove("reason");

        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(filled.contains(&"reason".to_string()));
        assert_eq!(payload["phase_decision"]["reason"], json!("exact-head APPROVE posted and PR squash-merged"));
        validate_basic_json_schema(&payload, &schema).expect("summary-derived reason must validate");
    }

    #[test]
    fn missing_decision_is_created_only_when_fully_mechanical() {
        let schema = pr_review_like_schema();

        // Derivable verdict + summary: the envelope is created and validates.
        let mut payload = run_2581ce29_payload();
        payload.as_object_mut().unwrap().remove("phase_decision");
        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(filled.contains(&"phase_decision".to_string()));
        validate_basic_json_schema(&payload, &schema).expect("mechanical envelope must validate");

        // No summary: the envelope is NOT invented and validation still fails.
        let mut payload = run_2581ce29_payload();
        {
            let object = payload.as_object_mut().unwrap();
            object.remove("phase_decision");
            object.remove("summary");
        }
        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(filled.is_empty());
        assert!(validate_basic_json_schema(&payload, &schema).is_err());
    }

    #[test]
    fn substantive_violations_still_fail() {
        let schema = pr_review_like_schema();

        // merged without merge_sha: the outcome artifact is missing.
        let mut payload = run_2581ce29_payload();
        payload.as_object_mut().unwrap().remove("merge_sha");
        normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(validate_basic_json_schema(&payload, &schema).is_err());

        // A verdict that contradicts the outcome mapping.
        let mut payload = run_2581ce29_payload();
        payload["outcome"] = json!("blocked_error");
        normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(validate_basic_json_schema(&payload, &schema).is_err());

        // The wrong result kind.
        let mut payload = run_2581ce29_payload();
        payload["kind"] = json!("something_else");
        normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(validate_basic_json_schema(&payload, &schema).is_err());

        // A phase_decision that is not an object is left alone.
        let mut payload = run_2581ce29_payload();
        payload["phase_decision"] = json!("advance");
        let filled = normalize_phase_decision(&mut payload, "pr-review", &schema);
        assert!(filled.is_empty());
        assert_eq!(payload["phase_decision"], json!("advance"));
        assert!(validate_basic_json_schema(&payload, &schema).is_err());
    }

    #[test]
    fn schemas_without_a_decision_envelope_are_untouched() {
        let schema = json!({
            "type": "object",
            "required": ["kind"],
            "properties": { "kind": { "const": "implementation_result" } }
        });
        let mut payload = json!({ "kind": "implementation_result", "summary": "done" });
        let before = payload.clone();

        let filled = normalize_phase_decision(&mut payload, "implementation", &schema);
        assert!(filled.is_empty());
        assert_eq!(payload, before);
    }
}
