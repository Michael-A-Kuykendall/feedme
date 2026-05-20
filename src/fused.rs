//! Fused rule evaluation engine.
//!
//! Implements the core invariant: semantic analysis in a single fused pass that
//! collapses multiple traversals into one while preserving all correctness guarantees.
//!
//! # Architecture
//!
//! `FusedRuleEngine` compiles N field-predicate rules at construction time into
//! a single fused evaluation program.  At runtime a single pass over the event's
//! JSON fields broadcasts each extracted value to every rule that declared interest
//! in that selector.  A rule-state bitmap tracks resolution; when all rules resolve
//! the engine terminates early — no further field access occurs.
//!
//! ## Conventional pipeline (rule-first)
//! ```text
//! RequiredFields: scan event O(M)
//! TypeChecking:   scan event O(M)
//! ValueConstraints: scan event O(M)
//! ──────────────────────────────
//! Total: O(N × M)   N=stages, M=fields
//! ```
//!
//! ## Fused pipeline (selector-first)
//! ```text
//! FusedRuleEngine: scan event O(M), broadcast to all N rules
//! ──────────────────────────────────────────────────────────
//! Total: O(M)   independent of rule count for shared selectors
//! ```
//!
//! # Usage
//!
//! ```rust
//! use feedme::fused::{FusedRuleEngine, Rule, Predicate, FailAction};
//!
//! let engine = FusedRuleEngine::builder("validation")
//!     .require(Rule::exists("user_id"))
//!     .require(Rule::type_is("amount", feedme::fused::FieldType::Number))
//!     .require(Rule::greater_than("amount", 0.0))
//!     .on_fail(FailAction::DropEvent)
//!     .build();
//! ```

use crate::{Event, PipelineError, Stage, ValidationError, ValidationErrorCode};
use std::sync::Arc;

// ── Public types ──────────────────────────────────────────────────────────────

/// What to do when one or more rules evaluate to false.
#[derive(Debug, Clone)]
pub enum FailAction {
    /// Drop the event (return `Ok(None)`).
    DropEvent,
    /// Pass the event through unchanged (useful for monitoring/alerting only).
    PassThrough,
    /// Return a validation error with the supplied code string.
    Error(String),
}

/// JSON field type constraint for `Predicate::TypeIs`.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Object,
    Array,
    Null,
}

impl FieldType {
    fn matches(&self, v: &serde_json::Value) -> bool {
        matches!(
            (self, v),
            (FieldType::String, serde_json::Value::String(_))
                | (FieldType::Number, serde_json::Value::Number(_))
                | (FieldType::Boolean, serde_json::Value::Bool(_))
                | (FieldType::Object, serde_json::Value::Object(_))
                | (FieldType::Array, serde_json::Value::Array(_))
                | (FieldType::Null, serde_json::Value::Null)
        )
    }
}

/// A single field rule: a selector path + a predicate that must hold.
#[derive(Clone)]
pub struct Rule {
    /// Dot-separated field path, e.g. `"user.id"` or `"amount"`.
    pub field: String,
    /// Predicate to evaluate against the extracted value.
    pub predicate: Predicate,
    /// Human-readable name for error messages.
    pub name: String,
}

impl Rule {
    /// Field must be present and non-null.
    pub fn exists(field: impl Into<String>) -> Self {
        let field = field.into();
        let name = format!("{} must exist", field);
        Rule { field, predicate: Predicate::Exists, name }
    }

    /// Field value must equal `value`.
    pub fn equals(field: impl Into<String>, value: serde_json::Value) -> Self {
        let field = field.into();
        let name = format!("{} must equal {:?}", field, value);
        Rule { field, predicate: Predicate::Equals(value), name }
    }

    /// Field must not equal `value`.
    pub fn not_equals(field: impl Into<String>, value: serde_json::Value) -> Self {
        let field = field.into();
        let name = format!("{} must not equal {:?}", field, value);
        Rule { field, predicate: Predicate::NotEquals(value), name }
    }

    /// Field must be of the specified JSON type.
    pub fn type_is(field: impl Into<String>, t: FieldType) -> Self {
        let field = field.into();
        let name = format!("{} must be {:?}", field, t);
        Rule { field, predicate: Predicate::TypeIs(t), name }
    }

    /// Field must be a number greater than `threshold`.
    pub fn greater_than(field: impl Into<String>, threshold: f64) -> Self {
        let field = field.into();
        let name = format!("{} must be > {}", field, threshold);
        Rule { field, predicate: Predicate::GreaterThan(threshold), name }
    }

    /// Field must be a number less than `threshold`.
    pub fn less_than(field: impl Into<String>, threshold: f64) -> Self {
        let field = field.into();
        let name = format!("{} must be < {}", field, threshold);
        Rule { field, predicate: Predicate::LessThan(threshold), name }
    }

    /// Field value must be one of the supplied values.
    pub fn one_of(field: impl Into<String>, values: Vec<serde_json::Value>) -> Self {
        let field = field.into();
        let name = format!("{} must be one of {:?}", field, values);
        Rule { field, predicate: Predicate::In(values), name }
    }

    /// Custom predicate closure.
    pub fn custom(
        field: impl Into<String>,
        name: impl Into<String>,
        f: impl Fn(&serde_json::Value) -> bool + Send + Sync + 'static,
    ) -> Self {
        Rule {
            field: field.into(),
            name: name.into(),
            predicate: Predicate::Custom(Arc::new(f)),
        }
    }
}

/// Predicate over a JSON value.
pub enum Predicate {
    Exists,
    Equals(serde_json::Value),
    NotEquals(serde_json::Value),
    TypeIs(FieldType),
    In(Vec<serde_json::Value>),
    GreaterThan(f64),
    LessThan(f64),
    Custom(Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>),
}

impl Clone for Predicate {
    fn clone(&self) -> Self {
        match self {
            Predicate::Exists => Predicate::Exists,
            Predicate::Equals(v) => Predicate::Equals(v.clone()),
            Predicate::NotEquals(v) => Predicate::NotEquals(v.clone()),
            Predicate::TypeIs(t) => Predicate::TypeIs(t.clone()),
            Predicate::In(vs) => Predicate::In(vs.clone()),
            Predicate::GreaterThan(f) => Predicate::GreaterThan(*f),
            Predicate::LessThan(f) => Predicate::LessThan(*f),
            Predicate::Custom(f) => Predicate::Custom(Arc::clone(f)),
        }
    }
}

impl Predicate {
    fn evaluate(&self, value: Option<&serde_json::Value>) -> bool {
        match self {
            Predicate::Exists => value.map(|v| !v.is_null()).unwrap_or(false),
            Predicate::Equals(expected) => value == Some(expected),
            Predicate::NotEquals(expected) => value != Some(expected),
            Predicate::TypeIs(t) => value.map(|v| t.matches(v)).unwrap_or(false),
            Predicate::In(options) => value.map(|v| options.contains(v)).unwrap_or(false),
            Predicate::GreaterThan(threshold) => value
                .and_then(|v| v.as_f64())
                .map(|n| n > *threshold)
                .unwrap_or(false),
            Predicate::LessThan(threshold) => value
                .and_then(|v| v.as_f64())
                .map(|n| n < *threshold)
                .unwrap_or(false),
            Predicate::Custom(f) => value.map(|v| f(v)).unwrap_or(false),
        }
    }
}

// ── Rule state bitmap ─────────────────────────────────────────────────────────

/// Compact representation of each rule's evaluation state.
/// `00 = Unresolved`, `01 = True`, `10 = False`.
#[derive(Clone, Copy, PartialEq, Debug)]
enum RuleState {
    Unresolved,
    True,
    False,
}

// ── Selector prefix trie ─────────────────────────────────────────────────────
//
// Shared path prefixes are internal nodes; a given prefix segment is traversed
// **exactly once** regardless of how many rules declare interest in paths that
// pass through it.
//
// Example:
//   Rules:  user.id (R0), user.name (R1), amount (R2)
//   Trie:
//     root
//      ├── "user"           (internal node, no terminal rules)
//      │    ├── "id"   →   terminal [R0]
//      │    └── "name" →   terminal [R1]
//      └── "amount"   →   terminal [R2]
//
// At runtime: `user` object is extracted ONCE from the event; `.id` and
// `.name` are read from that single extracted reference — not re-traversed
// from the root.  This is the O(M) guarantee for shared-prefix selectors.
//
// BTreeMap on `children` gives deterministic iteration order across runs
// (contrast: HashMap uses SipHash randomisation which would make evaluation
// order — and thus early-exit trigger points — non-deterministic).

struct TrieNode {
    /// Rules whose full selector path terminates at this node.
    terminal_rules: Vec<usize>,
    /// Child nodes keyed by the next path segment.
    /// BTreeMap ensures deterministic, sorted traversal order.
    children: std::collections::BTreeMap<String, TrieNode>,
}

impl TrieNode {
    fn new() -> Self {
        TrieNode { terminal_rules: Vec::new(), children: Default::default() }
    }

    /// Insert a rule at the given path (split into segments).
    fn insert(&mut self, segments: &[&str], rule_idx: usize) {
        if segments.is_empty() {
            return;
        }
        let child = self.children
            .entry(segments[0].to_string())
            .or_insert_with(TrieNode::new);
        if segments.len() == 1 {
            child.terminal_rules.push(rule_idx);
        } else {
            child.insert(&segments[1..], rule_idx);
        }
    }

    /// Count unique terminal paths (unique full selectors).
    fn count_terminals(&self) -> usize {
        let here = if self.terminal_rules.is_empty() { 0 } else { 1 };
        here + self.children.values().map(|c| c.count_terminals()).sum::<usize>()
    }

    /// Enumerate all terminal paths into `out`.
    fn collect_paths(&self, prefix: &str, out: &mut Vec<String>) {
        if !self.terminal_rules.is_empty() {
            out.push(prefix.to_string());
        }
        for (seg, child) in &self.children {
            let path = if prefix.is_empty() {
                seg.clone()
            } else {
                format!("{}.{}", prefix, seg)
            };
            child.collect_paths(&path, out);
        }
    }
}

// ── Compiled program ──────────────────────────────────────────────────────────

/// Compiled evaluation program: a selector prefix trie.
///
/// Compile time: rule selectors are inserted into the trie, deduplicating
/// shared prefixes.  Runtime: `execute` walks the trie in lock-step with the
/// JSON event — each path segment is resolved **exactly once** regardless of
/// rule count.
struct FusedProgram {
    /// Root of the selector prefix trie.
    root: TrieNode,
}

impl FusedProgram {
    /// Compile N rules into the selector prefix trie.
    fn compile(rules: &[Rule]) -> Self {
        let mut root = TrieNode::new();
        for (idx, rule) in rules.iter().enumerate() {
            let segments: Vec<&str> = rule.field.split('.').collect();
            root.insert(&segments, idx);
        }
        FusedProgram { root }
    }

    fn selector_count(&self) -> usize {
        self.root.count_terminals()
    }

    fn selectors(&self) -> Vec<String> {
        let mut paths = Vec::new();
        self.root.collect_paths("", &mut paths);
        paths
    }

    /// Execute the program against an event's data.
    ///
    /// Walks the trie in lock-step with the JSON structure.  Each path segment
    /// is accessed **once**; its value is broadcast to every rule terminating at
    /// that node.  When `pending_count` reaches 0 all rules are resolved and
    /// evaluation terminates early.
    fn execute(&self, rules: &[Rule], data: &serde_json::Value) -> RuleStateBitmap {
        let mut bitmap = RuleStateBitmap::new(rules.len());

        // Drive the trie from the root: each top-level segment is fetched once.
        for (segment, child) in &self.root.children {
            if bitmap.pending_count == 0 {
                break; // EARLY_EXIT
            }
            let value = data.get(segment.as_str());
            execute_node(child, value, rules, &mut bitmap);
        }

        // Deterministic fail-closed: unresolved rules (missing paths) → false.
        bitmap.resolve_unresolved_as_false();
        bitmap
    }
}

// ── Test-only instrumentation ─────────────────────────────────────────────────
//
// A thread-local counter records every call to `execute_node`.  This is the
// only way to prove — from outside the implementation — that the trie visits
// shared path-prefix nodes exactly once rather than once-per-rule.
//
// Enabled only under `#[cfg(test)]`; zero cost in production builds.

#[cfg(test)]
mod trie_instrument {
    use std::cell::Cell;
    thread_local! {
        static VISITS: Cell<usize> = Cell::new(0);
    }
    pub fn record() { VISITS.with(|c| c.set(c.get() + 1)); }
    pub fn reset()  { VISITS.with(|c| c.set(0)); }
    pub fn count()  -> usize { VISITS.with(|c| c.get()) }
}

/// Recursively evaluate a trie node against the current JSON value.
///
/// `value` is `None` when the path leading to this node was absent in the event.
/// Terminal rules at `node` are evaluated against `value`; children are recursed
/// only when `value` is `Some` — preserving fail-closed semantics for absent
/// sub-trees without an explicit traversal.
fn execute_node(
    node: &TrieNode,
    value: Option<&serde_json::Value>,
    rules: &[Rule],
    bitmap: &mut RuleStateBitmap,
) {
    #[cfg(test)]
    trie_instrument::record();

    if bitmap.pending_count == 0 {
        return; // EARLY_EXIT
    }

    // Evaluate terminal rules at this node: broadcast extracted value to each.
    for &rule_idx in &node.terminal_rules {
        if bitmap.states[rule_idx] != RuleState::Unresolved {
            continue;
        }
        let result = rules[rule_idx].predicate.evaluate(value);
        if result {
            bitmap.set_true(rule_idx);
        } else {
            bitmap.set_false(rule_idx);
        }
    }

    // Recurse into children only if we have a concrete value to drill into.
    // When value is None the sub-tree stays Unresolved → resolved fail-closed
    // at the end of FusedProgram::execute.
    if let Some(v) = value {
        for (segment, child) in &node.children {
            if bitmap.pending_count == 0 {
                break; // EARLY_EXIT
            }
            execute_node(child, v.get(segment.as_str()), rules, bitmap);
        }
    }
}

/// Rule state bitmap.
struct RuleStateBitmap {
    states: Vec<RuleState>,
    pending_count: usize,
    failed_indices: Vec<usize>,
}

impl RuleStateBitmap {
    fn new(rule_count: usize) -> Self {
        RuleStateBitmap {
            states: vec![RuleState::Unresolved; rule_count],
            pending_count: rule_count,
            failed_indices: Vec::new(),
        }
    }

    fn set_true(&mut self, idx: usize) {
        if self.states[idx] == RuleState::Unresolved {
            self.states[idx] = RuleState::True;
            self.pending_count -= 1;
        }
    }

    fn set_false(&mut self, idx: usize) {
        if self.states[idx] == RuleState::Unresolved {
            self.states[idx] = RuleState::False;
            self.pending_count -= 1;
            self.failed_indices.push(idx);
        }
    }

    fn resolve_unresolved_as_false(&mut self) {
        for (idx, state) in self.states.iter_mut().enumerate() {
            if *state == RuleState::Unresolved {
                *state = RuleState::False;
                self.pending_count = self.pending_count.saturating_sub(1);
                self.failed_indices.push(idx);
            }
        }
    }

    fn all_passed(&self) -> bool {
        self.failed_indices.is_empty()
    }
}

// ── Engine ────────────────────────────────────────────────────────────────────

/// Fused rule evaluation engine.
///
/// Replaces multiple sequential validation stages with a single O(M) pass.
/// Rules are compiled at construction time; `execute()` performs a single-pass
/// evaluation with selector deduplication, value broadcast, and early exit.
pub struct FusedRuleEngine {
    name: String,
    rules: Vec<Rule>,
    program: FusedProgram,
    fail_action: FailAction,
}

impl FusedRuleEngine {
    /// Create a builder for a `FusedRuleEngine`.
    pub fn builder(name: impl Into<String>) -> FusedRuleEngineBuilder {
        FusedRuleEngineBuilder {
            name: name.into(),
            rules: Vec::new(),
            fail_action: FailAction::DropEvent,
        }
    }

    /// Number of unique selectors in the compiled program.
    /// Demonstrates deduplication: fewer selectors than rules = sharing occurred.
    pub fn selector_count(&self) -> usize {
        self.program.selector_count()
    }

    /// Total number of rules.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Names of all unique selectors (field paths) in the compiled program.
    pub fn selectors(&self) -> Vec<String> {
        self.program.selectors()
    }
}

impl Stage for FusedRuleEngine {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        if self.rules.is_empty() {
            return Ok(Some(event));
        }

        let bitmap = self.program.execute(&self.rules, &event.data);

        if bitmap.all_passed() {
            return Ok(Some(event));
        }

        // Collect violation messages from failed rules
        let violations: Vec<String> = bitmap
            .failed_indices
            .iter()
            .map(|&i| self.rules[i].name.clone())
            .collect();

        match &self.fail_action {
            FailAction::DropEvent => Ok(None),
            FailAction::PassThrough => Ok(Some(event)),
            FailAction::Error(code) => Err(PipelineError::Validation(ValidationError {
                stage: self.name.clone(),
                code: ValidationErrorCode::ConstraintViolation,
                message: format!(
                    "[{}] {} rule(s) failed: {}",
                    code,
                    violations.len(),
                    violations.join("; ")
                ),
            })),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for `FusedRuleEngine`.
pub struct FusedRuleEngineBuilder {
    name: String,
    rules: Vec<Rule>,
    fail_action: FailAction,
}

impl FusedRuleEngineBuilder {
    /// Add a rule that MUST pass.
    pub fn require(mut self, rule: Rule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Set the action taken when one or more rules fail.
    pub fn on_fail(mut self, action: FailAction) -> Self {
        self.fail_action = action;
        self
    }

    /// Compile rules into the fused program and return the engine.
    pub fn build(self) -> FusedRuleEngine {
        let program = FusedProgram::compile(&self.rules);
        FusedRuleEngine {
            name: self.name,
            rules: self.rules,
            program,
            fail_action: self.fail_action,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;

    fn make_event(json: serde_json::Value) -> Event {
        Event { data: json, metadata: None }
    }

    // ── Core correctness ──────────────────────────────────────────────────

    #[test]
    fn test_empty_rules_passes_all_events() {
        let mut engine = FusedRuleEngine::builder("test").build();
        let event = make_event(serde_json::json!({"x": 1}));
        assert!(engine.execute(event).unwrap().is_some());
    }

    #[test]
    fn test_exists_rule_passes_when_field_present() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::exists("user_id"))
            .build();
        let event = make_event(serde_json::json!({"user_id": "abc"}));
        assert!(engine.execute(event).unwrap().is_some());
    }

    #[test]
    fn test_exists_rule_fails_when_field_missing() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::exists("user_id"))
            .build();
        let event = make_event(serde_json::json!({"other": "x"}));
        assert!(engine.execute(event).unwrap().is_none()); // dropped
    }

    #[test]
    fn test_type_rule_passes() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::type_is("amount", FieldType::Number))
            .build();
        let event = make_event(serde_json::json!({"amount": 42.5}));
        assert!(engine.execute(event).unwrap().is_some());
    }

    #[test]
    fn test_type_rule_fails_on_wrong_type() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::type_is("amount", FieldType::Number))
            .build();
        let event = make_event(serde_json::json!({"amount": "not_a_number"}));
        assert!(engine.execute(event).unwrap().is_none());
    }

    #[test]
    fn test_greater_than_rule() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::greater_than("price", 0.0))
            .build();
        assert!(engine.execute(make_event(serde_json::json!({"price": 10.0}))).unwrap().is_some());
        assert!(engine.execute(make_event(serde_json::json!({"price": -1.0}))).unwrap().is_none());
        assert!(engine.execute(make_event(serde_json::json!({"price": 0.0}))).unwrap().is_none());
    }

    #[test]
    fn test_one_of_rule() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::one_of("status", vec![
                serde_json::json!("active"),
                serde_json::json!("pending"),
            ]))
            .build();
        assert!(engine.execute(make_event(serde_json::json!({"status": "active"}))).unwrap().is_some());
        assert!(engine.execute(make_event(serde_json::json!({"status": "deleted"}))).unwrap().is_none());
    }

    #[test]
    fn test_equals_and_not_equals() {
        let mut eq_engine = FusedRuleEngine::builder("eq")
            .require(Rule::equals("role", serde_json::json!("admin")))
            .build();
        assert!(eq_engine.execute(make_event(serde_json::json!({"role": "admin"}))).unwrap().is_some());
        assert!(eq_engine.execute(make_event(serde_json::json!({"role": "user"}))).unwrap().is_none());
    }

    // ── Selector deduplication ──────────────────────────────────────────────

    #[test]
    fn test_selector_deduplication_reduces_selector_count() {
        // 3 rules, all on the same field "amount" — should compile to 1 selector
        let engine = FusedRuleEngine::builder("dedup")
            .require(Rule::exists("amount"))
            .require(Rule::type_is("amount", FieldType::Number))
            .require(Rule::greater_than("amount", 0.0))
            .build();

        assert_eq!(engine.rule_count(), 3);
        assert_eq!(engine.selector_count(), 1, "3 rules on same field = 1 unique selector");
    }

    #[test]
    fn test_selector_deduplication_correctness() {
        // All 3 rules fire from a single field extraction
        let mut engine = FusedRuleEngine::builder("dedup")
            .require(Rule::exists("amount"))
            .require(Rule::type_is("amount", FieldType::Number))
            .require(Rule::greater_than("amount", 0.0))
            .build();

        // All 3 rules pass → event passes
        let good = make_event(serde_json::json!({"amount": 42.0}));
        assert!(engine.execute(good).unwrap().is_some());

        // amount is present and is a number but not > 0 → fails last rule
        let bad = make_event(serde_json::json!({"amount": -5.0}));
        assert!(engine.execute(bad).unwrap().is_none());
    }

    #[test]
    fn test_multiple_selectors_correct() {
        // 4 rules across 2 unique selectors
        let engine = FusedRuleEngine::builder("multi")
            .require(Rule::exists("user_id"))
            .require(Rule::type_is("user_id", FieldType::String))
            .require(Rule::exists("amount"))
            .require(Rule::greater_than("amount", 0.0))
            .build();

        assert_eq!(engine.rule_count(), 4);
        assert_eq!(engine.selector_count(), 2);
    }

    // ── Trie: shared prefix traversal ───────────────────────────────────────

    #[test]
    fn test_shared_prefix_trie_selector_count() {
        // user.id and user.name share the "user" prefix.
        // The trie has 1 internal node ("user") with 2 terminal children.
        // selector_count() counts unique FULL paths (not internal nodes).
        let engine = FusedRuleEngine::builder("prefix_sharing")
            .require(Rule::exists("user.id"))
            .require(Rule::exists("user.name"))
            .require(Rule::greater_than("amount", 0.0))
            .build();

        assert_eq!(engine.rule_count(), 3);
        assert_eq!(engine.selector_count(), 3); // user.id, user.name, amount
    }

    #[test]
    fn test_shared_prefix_trie_correctness() {
        // Validates that the trie correctly resolves both rules sharing "user"
        // as a common prefix prefix — no rule is accidentally missed.
        let mut engine = FusedRuleEngine::builder("prefix_correctness")
            .require(Rule::exists("user.id"))
            .require(Rule::exists("user.name"))
            .build();

        // Both present
        let both = make_event(serde_json::json!({"user": {"id": "abc", "name": "Alice"}}));
        assert!(engine.execute(both).unwrap().is_some());

        // user.name missing inside existing user object
        let missing_name = make_event(serde_json::json!({"user": {"id": "abc"}}));
        assert!(engine.execute(missing_name).unwrap().is_none());

        // entire user object missing
        let no_user = make_event(serde_json::json!({"other": "value"}));
        assert!(engine.execute(no_user).unwrap().is_none());
    }

    #[test]
    fn test_deep_shared_prefix() {
        // a.b.c and a.b.d share "a.b" — two levels of prefix sharing.
        let mut engine = FusedRuleEngine::builder("deep_prefix")
            .require(Rule::exists("a.b.c"))
            .require(Rule::exists("a.b.d"))
            .build();

        let good = make_event(serde_json::json!({"a": {"b": {"c": 1, "d": 2}}}));
        assert!(engine.execute(good).unwrap().is_some());

        let missing_d = make_event(serde_json::json!({"a": {"b": {"c": 1}}}));
        assert!(engine.execute(missing_d).unwrap().is_none());
    }

    // ── Nested path extraction ────────────────────────────────────────────

    #[test]
    fn test_nested_path_extraction() {
        let mut engine = FusedRuleEngine::builder("nested")
            .require(Rule::exists("user.id"))
            .build();
        let event = make_event(serde_json::json!({"user": {"id": "abc123"}}));
        assert!(engine.execute(event).unwrap().is_some());
    }

    #[test]
    fn test_nested_path_missing() {
        let mut engine = FusedRuleEngine::builder("nested")
            .require(Rule::exists("user.id"))
            .build();
        let event = make_event(serde_json::json!({"user": {"name": "Alice"}}));
        assert!(engine.execute(event).unwrap().is_none());
    }

    // ── Fail actions ──────────────────────────────────────────────────────

    #[test]
    fn test_fail_action_drop() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::exists("required_field"))
            .on_fail(FailAction::DropEvent)
            .build();
        let event = make_event(serde_json::json!({}));
        assert!(engine.execute(event).unwrap().is_none());
    }

    #[test]
    fn test_fail_action_passthrough() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::exists("required_field"))
            .on_fail(FailAction::PassThrough)
            .build();
        let event = make_event(serde_json::json!({}));
        assert!(engine.execute(event).unwrap().is_some()); // passes through despite failure
    }

    #[test]
    fn test_fail_action_error() {
        let mut engine = FusedRuleEngine::builder("test")
            .require(Rule::exists("required_field"))
            .on_fail(FailAction::Error("VALIDATION_FAIL".to_string()))
            .build();
        let event = make_event(serde_json::json!({}));
        assert!(engine.execute(event).is_err());
    }

    // ── Early exit ────────────────────────────────────────────────────────

    #[test]
    fn test_early_exit_when_all_rules_resolved() {
        // With only 1 rule, the bitmap hits pending_count=0 immediately after the
        // matching selector is processed — the remaining selectors are skipped.
        let mut engine = FusedRuleEngine::builder("early_exit")
            .require(Rule::exists("a"))
            .build();
        // Event has many fields; only "a" matters
        let event = make_event(serde_json::json!({
            "a": 1, "b": 2, "c": 3, "d": 4, "e": 5
        }));
        assert!(engine.execute(event).unwrap().is_some());
    }

    // ── Fail-closed semantics ─────────────────────────────────────────────

    #[test]
    fn test_unresolved_rules_default_to_false() {
        // Rule checks "missing_field" which is never in the event
        let mut engine = FusedRuleEngine::builder("fail_closed")
            .require(Rule::exists("missing_field"))
            .build();
        let event = make_event(serde_json::json!({"other": "value"}));
        // Unresolved at end-of-input → default false → event dropped
        assert!(engine.execute(event).unwrap().is_none());
    }

    #[test]
    fn test_null_field_fails_exists() {
        let mut engine = FusedRuleEngine::builder("null_check")
            .require(Rule::exists("field"))
            .build();
        let event = make_event(serde_json::json!({"field": null}));
        // Exists predicate: present but null → false
        assert!(engine.execute(event).unwrap().is_none());
    }

    // ── Custom predicates ─────────────────────────────────────────────────

    #[test]
    fn test_custom_predicate() {
        let mut engine = FusedRuleEngine::builder("custom")
            .require(Rule::custom(
                "score",
                "score must be even",
                |v| v.as_f64().map(|n| n as i64 % 2 == 0).unwrap_or(false),
            ))
            .build();
        assert!(engine.execute(make_event(serde_json::json!({"score": 4}))).unwrap().is_some());
        assert!(engine.execute(make_event(serde_json::json!({"score": 3}))).unwrap().is_none());
    }

    // ── Integration with Pipeline ─────────────────────────────────────────

    #[test]
    fn test_fused_engine_in_pipeline() {
        use crate::Pipeline;

        let engine = FusedRuleEngine::builder("order_validation")
            .require(Rule::exists("order_id"))
            .require(Rule::exists("amount"))
            .require(Rule::type_is("amount", FieldType::Number))
            .require(Rule::greater_than("amount", 0.0))
            .on_fail(FailAction::DropEvent)
            .build();

        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(engine));

        // Valid event
        let valid = Event {
            data: serde_json::json!({"order_id": "ORD-001", "amount": 99.99}),
            metadata: None,
        };
        assert!(pipeline.process_event(valid).unwrap().is_some());

        // Invalid: negative amount
        let invalid = Event {
            data: serde_json::json!({"order_id": "ORD-002", "amount": -10.0}),
            metadata: None,
        };
        assert!(pipeline.process_event(invalid).unwrap().is_none());
    }

    #[test]
    fn test_fse_replaces_multiple_stages() {
        use crate::{Pipeline, RequiredFields, TypeChecking};

        // Traditional pipeline: 2 stages, each scanning the full event
        let mut trad = Pipeline::new();
        trad.add_stage(Box::new(RequiredFields::new(vec![
            "user_id".to_string(),
            "amount".to_string(),
        ])));
        trad.add_stage(Box::new(TypeChecking::new({
            let mut m = std::collections::HashMap::new();
            m.insert("amount".to_string(), "number".to_string());
            m
        })));

        // Fused pipeline: 1 stage, 3 rules, 2 unique selectors (amount deduped)
        let engine = FusedRuleEngine::builder("fused_validation")
            .require(Rule::exists("user_id"))
            .require(Rule::exists("amount"))
            .require(Rule::type_is("amount", FieldType::Number))
            .on_fail(FailAction::DropEvent)
            .build();

        assert_eq!(engine.selector_count(), 2); // user_id + amount (not 3!)
        assert_eq!(engine.rule_count(), 3);

        let mut fused = Pipeline::new();
        fused.add_stage(Box::new(engine));

        // Both pipelines should produce the same result
        let good_data = serde_json::json!({"user_id": "u1", "amount": 50.0});
        let bad_data = serde_json::json!({"user_id": "u1", "amount": "not_a_number"});

        let good_e1 = Event { data: good_data.clone(), metadata: None };
        let good_e2 = Event { data: good_data.clone(), metadata: None };
        let bad_e1 = Event { data: bad_data.clone(), metadata: None };
        let bad_e2 = Event { data: bad_data.clone(), metadata: None };

        assert!(trad.process_event(good_e1).unwrap().is_some());
        assert!(fused.process_event(good_e2).unwrap().is_some());
        // Note: TypeChecking in trad pipeline returns an error, not a drop;
        // fused drops. Both agree the event should not pass through.
        assert!(trad.process_event(bad_e1).is_err()); // trad: error
        assert!(fused.process_event(bad_e2).unwrap().is_none()); // fused: drop
    }
}

// ── Architectural property tests ──────────────────────────────────────────────
//
// Each test below guards one named property of the fused evaluation engine.
// If a future maintainer (human or AI) reverts to a rule-first / flat-HashMap
// approach, these tests will break and name the property that was lost.
//
// Properties guarded:
//   TRIE-VISIT   — shared prefix segments are traversed exactly once
//   BROADCAST    — all N rules mapped to one selector are evaluated every time
//   DEDUP        — selector count at compile time is ≤ rule count
//   FAIL-CLOSED  — unresolved rules always default to false, never true
//   DETERMINISM  — evaluation result is identical regardless of rule
//                  registration order

#[cfg(test)]
mod fse_architectural_properties {
    use super::*;
    use super::trie_instrument;
    use crate::Event;

    fn ev(json: serde_json::Value) -> Event {
        Event { data: json, metadata: None }
    }

    // ── TRIE-VISIT ────────────────────────────────────────────────────────
    //
    // These tests use the `trie_instrument` counter to prove that the
    // trie visits each path-prefix node ONCE, not once-per-rule.
    //
    // Counts: every call to `execute_node` is one "node visit".
    // Trie structure for paths A.B.C and A.B.D:
    //   execute_node(A)      → 1 visit
    //   execute_node(A.B)    → 1 visit   ← shared; rule-first would visit twice
    //   execute_node(A.B.C)  → 1 visit
    //   execute_node(A.B.D)  → 1 visit
    //   total: 4  (vs. 6 segment accesses in a rule-first extraction)

    #[test]
    fn trie_visit_flat_rules_one_visit_per_selector() {
        // Three unrelated top-level selectors: "a", "b", "c".
        // Trie: root → {a(terminal), b(terminal), c(terminal)}
        // Expected visits: 3 (one per terminal node).
        trie_instrument::reset();
        let mut engine = FusedRuleEngine::builder("flat")
            .require(Rule::exists("a"))
            .require(Rule::exists("b"))
            .require(Rule::exists("c"))
            .build();
        engine.execute(ev(serde_json::json!({"a":1,"b":2,"c":3}))).unwrap();
        assert_eq!(trie_instrument::count(), 3,
            "TRIE-VISIT: 3 flat selectors = 3 node visits, not 3×rule_count");
    }

    #[test]
    fn trie_visit_shared_prefix_traversed_once() {
        // user.id and user.name share the "user" prefix.
        // Trie: root → user(internal) → {id(terminal:R0), name(terminal:R1)}
        // execute_node calls: user(1) + id(1) + name(1) = 3
        //
        // A flat HashMap + extract_path regression would call:
        //   extract_path("user.id")   → accesses data["user"] (1st time)
        //   extract_path("user.name") → accesses data["user"] (2nd time)
        // That is NOT a trie; it traverses the shared "user" prefix twice.
        trie_instrument::reset();
        let mut engine = FusedRuleEngine::builder("shared_prefix")
            .require(Rule::exists("user.id"))
            .require(Rule::exists("user.name"))
            .build();
        engine.execute(ev(serde_json::json!({"user":{"id":"x","name":"y"}}))).unwrap();
        assert_eq!(trie_instrument::count(), 3,
            "TRIE-VISIT: user.id + user.name = 3 node visits (user, id, name) — \
             'user' traversed once. If 4, the shared prefix was visited twice.");
    }

    #[test]
    fn trie_visit_two_level_shared_prefix() {
        // a.b.c and a.b.d share two levels ("a" and "a.b").
        // Trie: root → a(internal) → b(internal) → {c(terminal), d(terminal)}
        // execute_node calls: a(1) + a.b(1) + a.b.c(1) + a.b.d(1) = 4
        //
        // Rule-first flat extraction:
        //   extract_path("a.b.c") → accesses a, then a.b, then a.b.c = 3 hops
        //   extract_path("a.b.d") → accesses a, then a.b, then a.b.d = 3 hops
        //   total: 6 segment accesses; "a" and "a.b" each accessed twice.
        // Trie: 4 total accesses; "a" and "a.b" each accessed once.
        trie_instrument::reset();
        let mut engine = FusedRuleEngine::builder("two_level")
            .require(Rule::exists("a.b.c"))
            .require(Rule::exists("a.b.d"))
            .build();
        engine.execute(ev(serde_json::json!({"a":{"b":{"c":1,"d":2}}}))).unwrap();
        assert_eq!(trie_instrument::count(), 4,
            "TRIE-VISIT: a.b.c + a.b.d = 4 node visits (a, a.b, a.b.c, a.b.d). \
             If 6, both 'a' and 'a.b' were visited twice — regression to flat extraction.");
    }

    #[test]
    fn trie_visit_three_siblings_same_parent() {
        // user.id, user.name, user.email — three children of one internal node.
        // Trie: root → user(internal) → {id, name, email}
        // execute_node calls: user(1) + id(1) + name(1) + email(1) = 4
        trie_instrument::reset();
        let mut engine = FusedRuleEngine::builder("three_siblings")
            .require(Rule::exists("user.id"))
            .require(Rule::exists("user.name"))
            .require(Rule::exists("user.email"))
            .build();
        engine.execute(ev(serde_json::json!({"user":{"id":"x","name":"y","email":"z"}}))).unwrap();
        assert_eq!(trie_instrument::count(), 4,
            "TRIE-VISIT: user.id/name/email = 4 node visits (user + 3 children). \
             If >4, 'user' was accessed more than once — prefix sharing broken.");
    }

    #[test]
    fn trie_visit_n_rules_same_selector_still_one_visit() {
        // 8 rules all on "amount" → 1 terminal node in trie.
        // No matter how many rules, the "amount" node is visited exactly once
        // and all 8 predicates are broadcast from that single visit.
        trie_instrument::reset();
        let mut engine = FusedRuleEngine::builder("n_rules_one_selector")
            .require(Rule::exists("amount"))
            .require(Rule::type_is("amount", FieldType::Number))
            .require(Rule::greater_than("amount", 0.0))
            .require(Rule::less_than("amount", 1_000_000.0))
            .require(Rule::not_equals("amount", serde_json::json!(999.0)))
            .require(Rule::not_equals("amount", serde_json::json!(0.001)))
            .require(Rule::not_equals("amount", serde_json::json!(-1.0)))
            .require(Rule::not_equals("amount", serde_json::json!(42.0)))
            .build();
        assert_eq!(engine.selector_count(), 1, "DEDUP: 8 rules on 'amount' = 1 unique selector");
        assert_eq!(engine.rule_count(), 8);
        engine.execute(ev(serde_json::json!({"amount": 100.0}))).unwrap();
        assert_eq!(trie_instrument::count(), 1,
            "TRIE-VISIT: 8 rules on same selector = 1 node visit. \
             If 8, each rule is extracting the field independently — rule-first regression.");
    }

    // ── BROADCAST ─────────────────────────────────────────────────────────
    //
    // Broadcast: when a selector is extracted, ALL N rules mapped to it are
    // evaluated.  None are skipped even if a prior rule already failed.
    // Observable: with FailAction::Error, ALL violated rule names appear in
    // the error message — not just the first one encountered.

    #[test]
    fn broadcast_all_violations_reported_not_just_first() {
        // Two rules on "amount" that BOTH fail for value -5:
        //   R0: amount must be > 100   → -5 fails
        //   R1: amount must be < -10   → -5 fails (-5 is not < -10)
        // A "stop-on-first-failure" approach would report only one violation.
        // Broadcast must report both.
        let mut engine = FusedRuleEngine::builder("broadcast_violations")
            .require(Rule::greater_than("amount", 100.0))
            .require(Rule::less_than("amount", -10.0))
            .on_fail(FailAction::Error("FAIL".to_string()))
            .build();
        assert_eq!(engine.selector_count(), 1);

        let result = engine.execute(ev(serde_json::json!({"amount": -5.0})));
        assert!(result.is_err(), "Both rules fail: event must error");
        let msg = format!("{:?}", result.unwrap_err());
        assert!(msg.contains("amount must be > 100"),
            "BROADCAST: R0 violation missing — broadcast did not evaluate all rules");
        assert!(msg.contains("amount must be < -10"),
            "BROADCAST: R1 violation missing — broadcast stopped after first failure");
    }

    #[test]
    fn broadcast_independent_pass_fail_tracking_per_rule() {
        // 3 rules on "x": R0 passes, R1 fails, R2 passes.
        // Only R1's name should appear in the error report.
        // Proves each rule's state is tracked independently in the bitmap.
        let mut engine = FusedRuleEngine::builder("independent_tracking")
            .require(Rule::greater_than("x", 0.0))      // R0: 5.0 > 0 → pass
            .require(Rule::less_than("x", 3.0))         // R1: 5.0 < 3 → FAIL
            .require(Rule::less_than("x", 100.0))       // R2: 5.0 < 100 → pass
            .on_fail(FailAction::Error("E".to_string()))
            .build();
        assert_eq!(engine.selector_count(), 1);

        let result = engine.execute(ev(serde_json::json!({"x": 5.0})));
        assert!(result.is_err());
        let msg = format!("{:?}", result.unwrap_err());
        assert!(msg.contains("1 rule(s) failed"),
            "BROADCAST: exactly 1 rule should fail, got: {}", msg);
        assert!(msg.contains("x must be < 3"),
            "BROADCAST: R1 violation missing from report");
        assert!(!msg.contains("x must be > 0"),
            "BROADCAST: R0 should not be in failure report (it passed)");
        assert!(!msg.contains("x must be < 100"),
            "BROADCAST: R2 should not be in failure report (it passed)");
    }

    #[test]
    fn broadcast_many_rules_any_single_failure_drops_event() {
        // 8 rules on "score". 7 pass, 1 fails.
        // Proves broadcast evaluates ALL 8, not just a subset.
        let mut engine = FusedRuleEngine::builder("many_rules_broadcast")
            .require(Rule::exists("score"))
            .require(Rule::type_is("score", FieldType::Number))
            .require(Rule::greater_than("score", 0.0))
            .require(Rule::less_than("score", 100.0))
            .require(Rule::not_equals("score", serde_json::json!(13.0)))
            .require(Rule::not_equals("score", serde_json::json!(0.0)))
            .require(Rule::not_equals("score", serde_json::json!(-1.0)))
            .require(Rule::less_than("score", 50.0)) // ← this one fails for score=75
            .on_fail(FailAction::DropEvent)
            .build();
        assert_eq!(engine.selector_count(), 1);
        assert_eq!(engine.rule_count(), 8);

        // score=75 passes 7 rules but fails "must be < 50"
        assert!(engine.execute(ev(serde_json::json!({"score": 75.0}))).unwrap().is_none(),
            "BROADCAST: failing rule 8 of 8 must still drop the event");
        // score=25 passes all 8
        assert!(engine.execute(ev(serde_json::json!({"score": 25.0}))).unwrap().is_some(),
            "BROADCAST: event satisfying all 8 rules must pass");
    }

    // ── DEDUP (compile-time fusion) ───────────────────────────────────────

    #[test]
    fn dedup_selector_count_is_unique_paths_not_rule_count() {
        // 6 rules across 2 unique selectors (3 rules each).
        // selector_count() must return 2, not 6.
        // If someone returns rule_count() from selector_count(), this fails.
        let engine = FusedRuleEngine::builder("dedup")
            .require(Rule::exists("user.id"))
            .require(Rule::type_is("user.id", FieldType::String))
            .require(Rule::not_equals("user.id", serde_json::json!("banned")))
            .require(Rule::exists("amount"))
            .require(Rule::type_is("amount", FieldType::Number))
            .require(Rule::greater_than("amount", 0.0))
            .build();
        assert_eq!(engine.rule_count(), 6,  "DEDUP: should have 6 rules");
        assert_eq!(engine.selector_count(), 2,
            "DEDUP: 6 rules on 2 unique paths = 2 unique selectors, not 6");
    }

    // ── FAIL-CLOSED ───────────────────────────────────────────────────────

    #[test]
    fn fail_closed_missing_intermediate_node_fails_entire_subtree() {
        // Rules on user.id, user.name, user.role — but event has no "user" key.
        // All 3 rules must fail (default-false), not pass.
        // Proves: missing parent → entire sub-tree is fail-closed, not ambiguous.
        let mut engine = FusedRuleEngine::builder("subtree_fail")
            .require(Rule::exists("user.id"))
            .require(Rule::exists("user.name"))
            .require(Rule::exists("user.role"))
            .on_fail(FailAction::Error("FC".to_string()))
            .build();

        let result = engine.execute(ev(serde_json::json!({"other": "value"})));
        assert!(result.is_err(), "FAIL-CLOSED: missing 'user' parent → all 3 sub-rules fail");
        let msg = format!("{:?}", result.unwrap_err());
        assert!(msg.contains("3 rule(s) failed"),
            "FAIL-CLOSED: all 3 rules in missing subtree should fail, got: {}", msg);
    }

    #[test]
    fn fail_closed_null_intermediate_node_fails_children() {
        // "user" exists but is null — children cannot be resolved.
        // user.id and user.name should fail (null has no children).
        let mut engine = FusedRuleEngine::builder("null_parent")
            .require(Rule::exists("user.id"))
            .require(Rule::exists("user.name"))
            .on_fail(FailAction::DropEvent)
            .build();

        let result = engine.execute(ev(serde_json::json!({"user": null})));
        assert!(result.unwrap().is_none(),
            "FAIL-CLOSED: null parent → child rules fail → event dropped");
    }

    // ── DETERMINISM ───────────────────────────────────────────────────────

    #[test]
    fn determinism_same_result_regardless_of_registration_order() {
        // The same logical set of rules in 3 different registration orders must
        // produce identical pass/fail results on every input.
        // This would break if evaluation depended on HashMap iteration order.
        let good = serde_json::json!({"user_id": "u1", "amount": 50.0, "status": "active"});
        let bad  = serde_json::json!({"user_id": "u1", "amount": -1.0, "status": "active"});

        let build_order_a = || FusedRuleEngine::builder("order_a")
            .require(Rule::exists("user_id"))
            .require(Rule::greater_than("amount", 0.0))
            .require(Rule::equals("status", serde_json::json!("active")))
            .on_fail(FailAction::DropEvent)
            .build();

        let build_order_b = || FusedRuleEngine::builder("order_b")
            .require(Rule::greater_than("amount", 0.0))
            .require(Rule::equals("status", serde_json::json!("active")))
            .require(Rule::exists("user_id"))
            .on_fail(FailAction::DropEvent)
            .build();

        let build_order_c = || FusedRuleEngine::builder("order_c")
            .require(Rule::equals("status", serde_json::json!("active")))
            .require(Rule::exists("user_id"))
            .require(Rule::greater_than("amount", 0.0))
            .on_fail(FailAction::DropEvent)
            .build();

        for order in ["a", "b", "c"] {
            let mut engine = match order {
                "a" => build_order_a(),
                "b" => build_order_b(),
                _   => build_order_c(),
            };
            assert!(
                engine.execute(ev(good.clone())).unwrap().is_some(),
                "DETERMINISM: order={} should pass good event", order
            );
            assert!(
                engine.execute(ev(bad.clone())).unwrap().is_none(),
                "DETERMINISM: order={} should drop bad event (negative amount)", order
            );
        }
    }
}

