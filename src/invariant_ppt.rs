//! Predictive Property-Based Testing (PPT) Framework
//!
//! Runtime invariant checking with logging for comprehensive contract testing.

use std::cell::RefCell;
use std::collections::HashSet;

thread_local! {
    static INVARIANT_LOG: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

/// Clear the invariant log for a new test
pub fn clear_invariant_log() {
    INVARIANT_LOG.with(|log| log.borrow_mut().clear());
}

/// Assert an invariant and log it if true
#[macro_export]
macro_rules! assert_invariant {
    ($condition:expr, $message:expr) => {
        if $condition {
            $crate::invariant_ppt::log_invariant($message);
        }
    };
}

// Re-export the macro from this module so callers can `pub use invariant_ppt::assert_invariant`.
pub use crate::assert_invariant;

/// Log an invariant message (internal)
pub fn log_invariant(message: &str) {
    INVARIANT_LOG.with(|log| {
        log.borrow_mut().insert(message.to_string());
    });
}

/// Verify that all expected invariants were exercised
pub fn contract_test(test_name: &str, expected_invariants: &[&str]) -> Result<(), String> {
    INVARIANT_LOG.with(|log| {
        let logged = log.borrow();
        let mut missing = Vec::new();

        for &expected in expected_invariants {
            if !logged.contains(expected) {
                missing.push(expected.to_string());
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "Contract test '{}' failed: missing invariants: {:?}",
                test_name, missing
            ))
        }
    })
}
