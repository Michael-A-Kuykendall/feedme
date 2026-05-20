//! Fault Injection — controlled resilience testing for pipelines.
//!
//! Wrap any `Stage` in a `FaultAwareStage` at pipeline construction time.
//! Retain the shared `FaultHandle` to activate / clear faults later — from
//! any thread — without touching the pipeline.
//!
//! # Example
//!
//! ```rust
//! use feedme::{Pipeline, Event};
//! use feedme::fault_injection::{FaultInjector, FaultType};
//!
//! # struct MyStage;
//! # impl feedme::Stage for MyStage {
//! #     fn execute(&mut self, e: Event) -> Result<Option<Event>, feedme::PipelineError> { Ok(Some(e)) }
//! #     fn name(&self) -> &str { "my_stage" }
//! # }
//! let mut injector = FaultInjector::new();
//! let mut pipeline = Pipeline::new();
//!
//! let wrapped = injector.wrap_and_register("payment", Box::new(MyStage));
//! pipeline.add_stage(Box::new(wrapped.stage));
//!
//! // Activate a failure for the next 3 events
//! injector.activate_failure("payment", "simulated timeout", Some(3));
//! ```

use crate::{Event, PipelineError, Stage, ValidationError, ValidationErrorCode};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

// ── Shared fault state ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FaultState {
    active: Option<ActiveFaultConfig>,
    total_triggers: u64,
}

#[derive(Debug, Clone)]
struct ActiveFaultConfig {
    fault_type: FaultType,
    /// `None` = trigger indefinitely; `Some(n)` = auto-clear after n events.
    remaining: Option<u32>,
}

impl FaultState {
    fn new() -> Self {
        Self { active: None, total_triggers: 0 }
    }

    fn is_active(&self) -> bool {
        self.active.is_some()
    }

    fn fire(&mut self) -> Option<FaultType> {
        let config = self.active.as_mut()?;
        let ft = config.fault_type.clone();
        self.total_triggers += 1;
        if let Some(ref mut remaining) = config.remaining {
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                self.active = None;
            }
        }
        Some(ft)
    }
}

// ── FaultAwareStage ───────────────────────────────────────────────────────────

/// Wraps any `Stage`, intercepting `execute()` to apply an active fault.
///
/// Construct via [`FaultInjector::wrap_and_register`] to have the injector
/// manage the shared state handle, or use [`FaultAwareStage::wrap`] directly
/// for finer-grained control.
pub struct FaultAwareStage {
    inner: Box<dyn Stage>,
    state: Arc<Mutex<FaultState>>,
    stage_name: String,
}

impl FaultAwareStage {
    /// Wrap a stage.  Returns `(wrapped_stage, shared_state_handle)`.
    pub fn wrap(stage: Box<dyn Stage>) -> (Self, Arc<Mutex<FaultState>>) {
        let state = Arc::new(Mutex::new(FaultState::new()));
        let name = stage.name().to_string();
        let wrapped = Self { inner: stage, state: Arc::clone(&state), stage_name: name };
        (wrapped, state)
    }
}

impl Stage for FaultAwareStage {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError> {
        let fault = {
            let mut state = self.state.lock().expect("fault state lock poisoned");
            state.fire()
        };

        match fault {
            None => self.inner.execute(event),
            Some(FaultType::StageFailure { error_message, .. }) => {
                Err(PipelineError::Validation(ValidationError {
                    stage: self.stage_name.clone(),
                    code: ValidationErrorCode::FaultInjected,
                    message: format!("[FAULT_INJECTED] {}", error_message),
                }))
            }
            Some(FaultType::StageTimeout { timeout_ms, .. }) => {
                std::thread::sleep(std::time::Duration::from_millis(timeout_ms));
                self.inner.execute(event)
            }
            Some(FaultType::ResourceExhaustion { resource_type }) => {
                Err(PipelineError::Validation(ValidationError {
                    stage: self.stage_name.clone(),
                    code: ValidationErrorCode::FaultInjected,
                    message: format!("[FAULT_INJECTED] resource exhausted: {}", resource_type),
                }))
            }
            Some(FaultType::NetworkPartition { .. }) => {
                Err(PipelineError::Validation(ValidationError {
                    stage: self.stage_name.clone(),
                    code: ValidationErrorCode::FaultInjected,
                    message: "[FAULT_INJECTED] network partition — connection refused".to_string(),
                }))
            }
        }
    }

    fn name(&self) -> &str {
        &self.stage_name
    }

    fn is_output(&self) -> bool {
        self.inner.is_output()
    }
}

/// Return value from [`FaultInjector::wrap_and_register`].
pub struct WrappedStage {
    pub stage: FaultAwareStage,
}

// ── FaultInjector ─────────────────────────────────────────────────────────────

/// Manages shared fault handles for multiple wrapped stages.
#[derive(Default)]
pub struct FaultInjector {
    handles: HashMap<String, Arc<Mutex<FaultState>>>,
    history: Vec<FaultRecord>,
}

impl FaultInjector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap a stage and register it under `name`.
    ///
    /// The returned [`WrappedStage`]`.stage` should be added to the pipeline.
    /// The injector retains the shared state handle so you can activate faults
    /// by name after the pipeline is built.
    pub fn wrap_and_register(
        &mut self,
        name: impl Into<String>,
        stage: Box<dyn Stage>,
    ) -> WrappedStage {
        let name = name.into();
        let (aware, state_handle) = FaultAwareStage::wrap(stage);
        self.handles.insert(name, state_handle);
        WrappedStage { stage: aware }
    }

    /// Activate a stage-failure fault.  The stage returns an error on the
    /// next `count` events (`None` = indefinitely).
    pub fn activate_failure(
        &mut self,
        stage_name: &str,
        error_message: impl Into<String>,
        count: Option<u32>,
    ) -> Result<FaultId, String> {
        self.activate(
            stage_name,
            FaultType::StageFailure { stage_index: 0, error_message: error_message.into() },
            count,
        )
    }

    /// Activate a stage-timeout fault (simulated via `thread::sleep`).
    pub fn activate_timeout(
        &mut self,
        stage_name: &str,
        timeout_ms: u64,
        count: Option<u32>,
    ) -> Result<FaultId, String> {
        self.activate(
            stage_name,
            FaultType::StageTimeout { stage_index: 0, timeout_ms },
            count,
        )
    }

    /// Activate a resource-exhaustion fault.
    pub fn activate_resource_exhaustion(
        &mut self,
        stage_name: &str,
        resource_type: impl Into<String>,
        count: Option<u32>,
    ) -> Result<FaultId, String> {
        self.activate(
            stage_name,
            FaultType::ResourceExhaustion { resource_type: resource_type.into() },
            count,
        )
    }

    /// Activate a network-partition fault.
    pub fn activate_network_partition(
        &mut self,
        stage_name: &str,
        count: Option<u32>,
    ) -> Result<FaultId, String> {
        self.activate(
            stage_name,
            FaultType::NetworkPartition { affected_stages: vec![0] },
            count,
        )
    }

    /// Clear all active faults across every registered stage.
    pub fn clear_all(&mut self) {
        for state in self.handles.values() {
            state.lock().expect("lock poisoned").active = None;
        }
    }

    /// Clear the active fault on a single stage.
    pub fn clear_stage(&mut self, stage_name: &str) -> Result<(), String> {
        match self.handles.get(stage_name) {
            Some(state) => {
                state.lock().expect("lock poisoned").active = None;
                Ok(())
            }
            None => Err(format!("No stage registered as '{}'", stage_name)),
        }
    }

    /// Snapshot of current fault state across all registered stages.
    pub fn get_report(&self) -> FaultInjectionReport {
        let active_count = self
            .handles
            .values()
            .filter(|s| s.lock().expect("lock poisoned").is_active())
            .count();

        let fault_types: HashSet<FaultType> =
            self.history.iter().map(|r| r.fault_type.clone()).collect();

        FaultInjectionReport {
            active_faults: active_count,
            total_faults_injected: self.history.len(),
            fault_types_used: fault_types.into_iter().collect(),
            recent_faults: self.history.iter().rev().take(10).cloned().collect(),
        }
    }

    fn activate(
        &mut self,
        stage_name: &str,
        fault_type: FaultType,
        count: Option<u32>,
    ) -> Result<FaultId, String> {
        let state = self.handles.get(stage_name).ok_or_else(|| {
            format!("No stage registered as '{}'. Call wrap_and_register() first.", stage_name)
        })?;

        state.lock().expect("lock poisoned").active =
            Some(ActiveFaultConfig { fault_type: fault_type.clone(), remaining: count });

        let fault_id = FaultId::new();
        self.history.push(FaultRecord {
            id: fault_id.clone(),
            stage_name: stage_name.to_string(),
            fault_type,
            injected_at: SystemTime::now(),
            cleared_at: None,
        });
        Ok(fault_id)
    }
}

// ── Domain types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FaultId(String);

impl FaultId {
    fn new() -> Self {
        let ns = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self(format!("fault_{}", ns))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FaultType {
    StageTimeout { stage_index: usize, timeout_ms: u64 },
    StageFailure { stage_index: usize, error_message: String },
    ResourceExhaustion { resource_type: String },
    NetworkPartition { affected_stages: Vec<usize> },
}

#[derive(Debug, Clone)]
pub struct FaultRecord {
    pub id: FaultId,
    pub stage_name: String,
    pub fault_type: FaultType,
    pub injected_at: SystemTime,
    pub cleared_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct FaultInjectionReport {
    pub active_faults: usize,
    pub total_faults_injected: usize,
    pub fault_types_used: Vec<FaultType>,
    pub recent_faults: Vec<FaultRecord>,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Event, Pipeline};

    struct Passthrough;
    impl Stage for Passthrough {
        fn execute(&mut self, e: Event) -> Result<Option<Event>, PipelineError> { Ok(Some(e)) }
        fn name(&self) -> &str { "passthrough" }
    }

    fn evt() -> Event {
        Event { data: serde_json::json!({"x": 1}), metadata: None }
    }

    #[test]
    fn test_injector_new() {
        let inj = FaultInjector::new();
        assert!(inj.handles.is_empty());
        assert!(inj.history.is_empty());
    }

    #[test]
    fn test_wrap_and_register() {
        let mut inj = FaultInjector::new();
        let wrapped = inj.wrap_and_register("s", Box::new(Passthrough));
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(wrapped.stage));
        assert_eq!(pipeline.stage_count(), 1);
        assert!(inj.handles.contains_key("s"));
    }

    #[test]
    fn test_no_fault_passes_through() {
        let (mut aware, _) = FaultAwareStage::wrap(Box::new(Passthrough));
        assert!(aware.execute(evt()).unwrap().is_some());
    }

    #[test]
    fn test_failure_fault_fires_then_auto_clears() {
        let (mut aware, handle) = FaultAwareStage::wrap(Box::new(Passthrough));
        handle.lock().unwrap().active = Some(ActiveFaultConfig {
            fault_type: FaultType::StageFailure { stage_index: 0, error_message: "oops".into() },
            remaining: Some(1),
        });
        assert!(aware.execute(evt()).is_err());
        assert!(aware.execute(evt()).unwrap().is_some()); // auto-cleared
    }

    #[test]
    fn test_network_partition_fault() {
        let (mut aware, handle) = FaultAwareStage::wrap(Box::new(Passthrough));
        handle.lock().unwrap().active = Some(ActiveFaultConfig {
            fault_type: FaultType::NetworkPartition { affected_stages: vec![] },
            remaining: None,
        });
        assert!(aware.execute(evt()).is_err());
    }

    #[test]
    fn test_resource_exhaustion_fault() {
        let (mut aware, handle) = FaultAwareStage::wrap(Box::new(Passthrough));
        handle.lock().unwrap().active = Some(ActiveFaultConfig {
            fault_type: FaultType::ResourceExhaustion { resource_type: "memory".into() },
            remaining: Some(1),
        });
        let err = aware.execute(evt()).unwrap_err().to_string();
        assert!(err.contains("memory"));
    }

    #[test]
    fn test_injector_activate_failure_via_api() {
        let mut inj = FaultInjector::new();
        let wrapped = inj.wrap_and_register("stage", Box::new(Passthrough));
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(wrapped.stage));

        inj.activate_failure("stage", "test error", Some(2)).unwrap();
        assert!(pipeline.process_event(evt()).is_err());
        assert!(pipeline.process_event(evt()).is_err());
        assert!(pipeline.process_event(evt()).unwrap().is_some()); // auto-cleared
    }

    #[test]
    fn test_injector_clear_all() {
        let mut inj = FaultInjector::new();
        let wrapped = inj.wrap_and_register("stage", Box::new(Passthrough));
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(wrapped.stage));

        inj.activate_failure("stage", "err", None).unwrap();
        assert!(pipeline.process_event(evt()).is_err());
        inj.clear_all();
        assert!(pipeline.process_event(evt()).unwrap().is_some());
    }

    #[test]
    fn test_unknown_stage_returns_error() {
        let mut inj = FaultInjector::new();
        assert!(inj.activate_failure("does_not_exist", "err", None).is_err());
    }

    #[test]
    fn test_fault_aware_in_pipeline() {
        let (aware, handle) = FaultAwareStage::wrap(Box::new(Passthrough));
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(Box::new(aware));

        let e1 = Event { data: serde_json::json!({"n": 1}), metadata: None };
        assert!(pipeline.process_event(e1).unwrap().is_some());

        handle.lock().unwrap().active = Some(ActiveFaultConfig {
            fault_type: FaultType::StageFailure { stage_index: 0, error_message: "t".into() },
            remaining: Some(2),
        });
        assert!(pipeline.process_event(evt()).is_err());
        assert!(pipeline.process_event(evt()).is_err());
        assert!(pipeline.process_event(evt()).unwrap().is_some());
    }
}
