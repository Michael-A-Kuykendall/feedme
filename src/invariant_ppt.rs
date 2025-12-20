use std::collections::BTreeSet;

#[cfg(any(test, feature = "invariant-ppt"))]
use std::sync::{Mutex, OnceLock};

#[cfg(any(test, feature = "invariant-ppt"))]
#[derive(Debug, Clone)]
struct InvariantRecord {
    message: &'static str,
    context: Option<&'static str>,
}

#[cfg(any(test, feature = "invariant-ppt"))]
static INVARIANT_LOG: OnceLock<Mutex<Vec<InvariantRecord>>> = OnceLock::new();

#[cfg(any(test, feature = "invariant-ppt"))]
fn log() -> &'static Mutex<Vec<InvariantRecord>> {
    INVARIANT_LOG.get_or_init(|| Mutex::new(Vec::new()))
}

/// Asserts an invariant (a semantic law) and records that it was checked.
///
/// Behavior:
/// - In `test` builds or when the `invariant-ppt` feature is enabled: records checks and panics on violation.
/// - Otherwise: uses `debug_assert!` and does not record (zero overhead in release).
#[inline]
pub fn assert_invariant(condition: bool, message: &'static str, context: Option<&'static str>) {
    #[cfg(any(test, feature = "invariant-ppt"))]
    {
        let mut guard = log().lock().expect("invariant log poisoned");
        guard.push(InvariantRecord { message, context });

        if !condition {
            panic!(
                "Invariant violated: {}{}",
                message,
                context
                    .map(|c| format!(" (context: {})", c))
                    .unwrap_or_default()
            );
        }
        return;
    }

    #[cfg(not(any(test, feature = "invariant-ppt")))]
    {
        let _ = context;
        debug_assert!(condition, "Invariant violated: {}", message);
    }
}

/// Clears the invariant log (useful to isolate contract tests).
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn clear_invariant_log() {
    log().lock().expect("invariant log poisoned").clear();
}

/// Returns all distinct invariant messages that were exercised.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn exercised_invariants() -> BTreeSet<&'static str> {
    log()
        .lock()
        .expect("invariant log poisoned")
        .iter()
        .map(|r| {
            let _ = r.context;
            r.message
        })
        .collect()
}

/// Contract test: fails if any expected invariants were not exercised.
#[cfg(any(test, feature = "invariant-ppt"))]
pub fn contract_test(contract_name: &str, expected_invariants: &[&'static str]) {
    let exercised = exercised_invariants();

    let missing: Vec<&'static str> = expected_invariants
        .iter()
        .copied()
        .filter(|inv| !exercised.contains(inv))
        .collect();

    if !missing.is_empty() {
        let mut present: Vec<&'static str> = exercised.into_iter().collect();
        present.sort();
        panic!(
            "Contract '{}' missing invariants: {:?}. Present: {:?}",
            contract_name, missing, present
        );
    }
}

#[macro_export]
macro_rules! assert_invariant {
    ($cond:expr, $msg:expr) => {
        $crate::invariant_ppt::assert_invariant($cond, $msg, None)
    };
    ($cond:expr, $msg:expr, $ctx:expr) => {
        $crate::invariant_ppt::assert_invariant($cond, $msg, Some($ctx))
    };
}
