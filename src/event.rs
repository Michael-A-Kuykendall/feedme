use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
/// Represents a structured event in the pipeline.
/// Owned, mutable data, supports JSON-like types, typed field access, optional metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The main data payload, JSON-like.
    pub data: serde_json::Value,
    /// Optional metadata associated with the event.
    pub metadata: Option<BTreeMap<String, serde_json::Value>>,
}

impl Event {
    /// Create a new event from raw input (JSON assumed).
    pub fn from_raw_input(input: &str) -> anyhow::Result<Self> {
        let data: serde_json::Value = serde_json::from_str(input)?;
        Ok(Event {
            data,
            metadata: None,
        })
    }

    /// Typed field access for strings.
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.data.get(key)?.as_str()
    }

    /// Typed field access for numbers.
    pub fn get_number(&self, key: &str) -> Option<f64> {
        self.data.get(key)?.as_f64()
    }

    /// Builder helper to attach/override metadata (for traces, audit ids, routing).
    /// Promoted as first-class extension point for the "bells and whistles" (audit/replay).
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        if self.metadata.is_none() {
            self.metadata = Some(BTreeMap::new());
        }
        self.metadata.as_mut().unwrap().insert(key.into(), value);
        self
    }

    // Add more typed access as needed.
}
