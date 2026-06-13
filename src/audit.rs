//! Audit — pipeline execution attestation and compliance reporting.
//!
//! Provides [`AuditManager`] for recording pipeline executions, evaluating
//! compliance policies, and generating attestation bundles and audit trails.
//!
//! # Example
//!
//! ```rust
//! use feedme::{Pipeline, audit::{AuditManager, CompliancePolicy, ComplianceCheck, CheckType}};
//!
//! let mut manager = AuditManager::new();
//! let mut pipeline = Pipeline::new();
//!
//! manager.add_compliance_policy(
//!     "error_rate".to_string(),
//!     CompliancePolicy {
//!         name: "Error Rate Policy".to_string(),
//!         description: "Max 5% errors".to_string(),
//!         checks: vec![ComplianceCheck {
//!             name: "error_rate".to_string(),
//!             description: "error rate <= 5%".to_string(),
//!             check_type: CheckType::MaxErrorRate,
//!             threshold: 0.05,
//!         }],
//!     },
//! );
//!
//! let bundle = manager.generate_attestation_bundle(&pipeline, "exec-001").unwrap();
//! assert_eq!(bundle.execution_id, "exec-001");
//! ```

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

// ── Serialisable metrics snapshot ─────────────────────────────────────────────

/// Serialisable snapshot of pipeline metrics collected at attestation time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditMetricsSnapshot {
    pub stage_count: usize,
    pub total_events_processed: u64,
    pub avg_latency_ms: f64,
    pub error_rate: f64,
    /// Fraction of events dropped (filtered) out of total processed.
    pub drop_rate: f64,
}

impl AuditMetricsSnapshot {
    fn from_pipeline(pipeline: &crate::Pipeline) -> Self {
        let total = pipeline.events_processed();
        Self {
            stage_count: pipeline.stage_count(),
            total_events_processed: total,
            avg_latency_ms: pipeline.avg_latency_ms(),
            error_rate: pipeline.error_rate(),
            drop_rate: if total == 0 {
                0.0
            } else {
                pipeline.events_dropped() as f64 / total as f64
            },
        }
    }
}

// ── Audit manager ─────────────────────────────────────────────────────────────

/// Manages pipeline execution attestation, compliance policies, and audit trail.
pub struct AuditManager {
    audit_trail: Vec<AuditEvent>,
    compliance_policies: HashMap<String, CompliancePolicy>,
    evidence_store: HashMap<String, EvidenceBundle>,
}

impl AuditManager {
    /// Create a new audit manager.
    pub fn new() -> Self {
        Self {
            audit_trail: Vec::new(),
            compliance_policies: HashMap::new(),
            evidence_store: HashMap::new(),
        }
    }

    /// Generate a comprehensive attestation bundle for a pipeline execution.
    pub fn generate_attestation_bundle(
        &mut self,
        pipeline: &crate::Pipeline,
        execution_id: &str,
    ) -> Result<AttestationBundle, String> {
        let metrics = AuditMetricsSnapshot::from_pipeline(pipeline);
        #[cfg(any(test, feature = "invariant-ppt"))]
        let health_issues = crate::invariant_ppt::analyze_pipeline_composition(pipeline);
        #[cfg(not(any(test, feature = "invariant-ppt")))]
        let health_issues: Vec<String> = Vec::new();
        let compliance_checks = self.run_compliance_checks(&metrics);
        let pipeline_hash = compute_pipeline_hash(pipeline);

        let bundle = AttestationBundle {
            execution_id: execution_id.to_string(),
            timestamp: std::time::SystemTime::now(),
            pipeline_hash,
            metrics: metrics.clone(),
            health_issues,
            evidence_hashes: Vec::new(),
            compliance_checks,
            attested_by: "FeedMe Audit Manager".to_string(),
        };

        self.evidence_store.insert(
            execution_id.to_string(),
            EvidenceBundle {
                attestation: bundle.clone(),
            },
        );

        self.audit_trail.push(AuditEvent {
            event_type: AuditEventType::AttestationGenerated,
            timestamp: std::time::SystemTime::now(),
            details: format!(
                "Generated attestation bundle for execution {}",
                execution_id
            ),
            user_id: None,
            compliance_impact: ComplianceImpact::High,
        });

        Ok(bundle)
    }

    /// Add a compliance policy.
    pub fn add_compliance_policy(&mut self, name: String, policy: CompliancePolicy) {
        self.compliance_policies.insert(name, policy);
    }

    /// Run all registered compliance checks against a metrics snapshot.
    pub fn run_compliance_checks(&self, metrics: &AuditMetricsSnapshot) -> Vec<ComplianceResult> {
        self.compliance_policies
            .values()
            .map(|policy| policy.evaluate(metrics))
            .collect()
    }

    /// Generate a compliance summary report.
    ///
    /// Counts pass/fail by inspecting the `compliance_checks` stored in every
    /// recorded [`AttestationBundle`]. This is accurate regardless of whether
    /// any `ComplianceCheckPassed` audit events were emitted.
    pub fn generate_compliance_report(&self) -> ComplianceReport {
        let total_policies = self.compliance_policies.len();

        let all_results: Vec<&ComplianceResult> = self
            .evidence_store
            .values()
            .flat_map(|b| b.attestation.compliance_checks.iter())
            .collect();

        let passed_checks = all_results.iter().filter(|r| r.overall_pass).count();
        let failed_checks = all_results.iter().filter(|r| !r.overall_pass).count();

        let compliance_score = if total_policies > 0 {
            passed_checks as f64 / total_policies as f64
        } else {
            1.0
        };

        ComplianceReport {
            compliance_score,
            total_policies,
            passed_checks,
            failed_checks,
            last_audit: std::time::SystemTime::now(),
            recommendations: self.generate_compliance_recommendations(),
        }
    }

    /// Retrieve the attestation bundle recorded for a specific execution, if any.
    pub fn get_attestation_bundle(&self, execution_id: &str) -> Option<AttestationBundle> {
        self.evidence_store
            .get(execution_id)
            .map(|b| b.attestation.clone())
    }

    /// Export the audit trail in the requested format.
    pub fn export_audit_trail(&self, format: ExportFormat) -> Result<String, String> {
        match format {
            ExportFormat::JSON => serde_json::to_string_pretty(&self.audit_trail)
                .map_err(|e| format!("Failed to serialize audit trail: {}", e)),
            ExportFormat::CSV => {
                let mut csv =
                    String::from("timestamp,event_type,details,user_id,compliance_impact\n");
                for event in &self.audit_trail {
                    csv.push_str(&format!(
                        "{:?},{:?},{},{:?},{:?}\n",
                        event.timestamp,
                        event.event_type,
                        event.details.replace(',', ";"),
                        event.user_id,
                        event.compliance_impact
                    ));
                }
                Ok(csv)
            }
        }
    }

    /// Generate evidence package for a specific regulation.
    pub fn generate_regulatory_evidence(&self, regulation: &str) -> RegulatoryEvidence {
        RegulatoryEvidence {
            regulation: regulation.to_string(),
            evidence_type: "Pipeline Execution Attestation".to_string(),
            generated_at: std::time::SystemTime::now(),
            validity_period: std::time::Duration::from_secs(365 * 24 * 60 * 60),
            evidence_items: self.evidence_store.keys().cloned().collect(),
        }
    }

    fn generate_compliance_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        let failed = self
            .audit_trail
            .iter()
            .filter(|e| matches!(e.event_type, AuditEventType::ComplianceCheckFailed))
            .count();

        if failed > 0 {
            recommendations.push(format!("Address {} failed compliance checks.", failed));
        }

        if self.compliance_policies.is_empty() {
            recommendations
                .push("Define compliance policies for your regulatory requirements.".to_string());
        }

        recommendations
    }
}

impl Default for AuditManager {
    fn default() -> Self {
        Self::new()
    }
}

// Domain types

/// Comprehensive attestation bundle for a pipeline execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestationBundle {
    pub execution_id: String,
    pub timestamp: std::time::SystemTime,
    pub pipeline_hash: String,
    pub metrics: AuditMetricsSnapshot,
    pub health_issues: Vec<String>,
    pub evidence_hashes: Vec<String>,
    pub compliance_checks: Vec<ComplianceResult>,
    pub attested_by: String,
}

/// Compliance policy definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompliancePolicy {
    pub name: String,
    pub description: String,
    pub checks: Vec<ComplianceCheck>,
}

impl CompliancePolicy {
    /// Evaluate the policy against a metrics snapshot.
    pub fn evaluate(&self, metrics: &AuditMetricsSnapshot) -> ComplianceResult {
        let mut passed = 0usize;
        let mut failed = 0usize;
        let mut violations = Vec::new();

        for check in &self.checks {
            if check.evaluate(metrics) {
                passed += 1;
            } else {
                failed += 1;
                violations.push(check.violation_message());
            }
        }

        ComplianceResult {
            policy_name: self.name.clone(),
            passed,
            failed,
            violations,
            overall_pass: failed == 0,
        }
    }
}

/// Individual compliance check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceCheck {
    pub name: String,
    pub description: String,
    pub check_type: CheckType,
    pub threshold: f64,
}

impl ComplianceCheck {
    /// Evaluate the check against a metrics snapshot.
    pub fn evaluate(&self, metrics: &AuditMetricsSnapshot) -> bool {
        match self.check_type {
            CheckType::MaxErrorRate => metrics.error_rate <= self.threshold,
            CheckType::MaxDropRate => metrics.drop_rate <= self.threshold,
            CheckType::MinThroughput => metrics.total_events_processed as f64 >= self.threshold,
            CheckType::MaxLatency => metrics.avg_latency_ms <= self.threshold,
        }
    }

    fn violation_message(&self) -> String {
        format!("{}: threshold {:.4} violated", self.name, self.threshold)
    }
}

/// Types of compliance checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CheckType {
    MaxErrorRate,
    MaxDropRate,
    MinThroughput,
    MaxLatency,
}

/// Result of evaluating a compliance policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceResult {
    pub policy_name: String,
    pub passed: usize,
    pub failed: usize,
    pub violations: Vec<String>,
    pub overall_pass: bool,
}

/// High-level compliance report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReport {
    pub compliance_score: f64,
    pub total_policies: usize,
    pub passed_checks: usize,
    pub failed_checks: usize,
    pub last_audit: std::time::SystemTime,
    pub recommendations: Vec<String>,
}

/// Audit event types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditEventType {
    AttestationGenerated,
    ComplianceCheckPassed,
    ComplianceCheckFailed,
    PolicyUpdated,
    EvidenceExported,
}

/// Individual audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_type: AuditEventType,
    pub timestamp: std::time::SystemTime,
    pub details: String,
    pub user_id: Option<String>,
    pub compliance_impact: ComplianceImpact,
}

/// Compliance impact levels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComplianceImpact {
    Low,
    Medium,
    High,
    Critical,
}

/// Export formats for audit data.
#[derive(Debug, Clone)]
pub enum ExportFormat {
    JSON,
    CSV,
}

/// Regulatory evidence package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegulatoryEvidence {
    pub regulation: String,
    pub evidence_type: String,
    pub generated_at: std::time::SystemTime,
    pub validity_period: std::time::Duration,
    pub evidence_items: Vec<String>,
}

// Private helpers

struct EvidenceBundle {
    attestation: AttestationBundle,
}

fn compute_pipeline_hash(pipeline: &crate::Pipeline) -> String {
    let mut hasher = Sha256::new();
    for name in pipeline.stage_names() {
        hasher.update(name.as_bytes());
        hasher.update(b"|");
    }
    format!("{:x}", hasher.finalize())
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_manager_creation() {
        let manager = AuditManager::new();
        assert!(manager.audit_trail.is_empty());
        assert!(manager.compliance_policies.is_empty());
    }

    #[test]
    fn test_attestation_generation() {
        let mut manager = AuditManager::new();
        let pipeline = crate::Pipeline::new();
        let bundle = manager
            .generate_attestation_bundle(&pipeline, "test-exec")
            .unwrap();
        assert_eq!(bundle.execution_id, "test-exec");
        assert_eq!(bundle.attested_by, "FeedMe Audit Manager");
    }

    #[test]
    fn test_compliance_check_max_error_rate_pass() {
        let check = ComplianceCheck {
            name: "Error Rate".to_string(),
            description: "Max 5% error rate".to_string(),
            check_type: CheckType::MaxErrorRate,
            threshold: 0.05,
        };
        let metrics = AuditMetricsSnapshot {
            stage_count: 1,
            total_events_processed: 1000,
            avg_latency_ms: 1.0,
            error_rate: 0.02,
            drop_rate: 0.0,
        };
        assert!(check.evaluate(&metrics));
    }

    #[test]
    fn test_compliance_check_max_error_rate_fail() {
        let check = ComplianceCheck {
            name: "Error Rate".to_string(),
            description: "Max 5% error rate".to_string(),
            check_type: CheckType::MaxErrorRate,
            threshold: 0.05,
        };
        let metrics = AuditMetricsSnapshot {
            stage_count: 1,
            total_events_processed: 1000,
            avg_latency_ms: 1.0,
            error_rate: 0.10,
            drop_rate: 0.0,
        };
        assert!(!check.evaluate(&metrics));
    }

    #[test]
    fn test_compliance_report_empty_manager() {
        let manager = AuditManager::new();
        let report = manager.generate_compliance_report();
        assert_eq!(report.total_policies, 0);
        assert!((report.compliance_score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_and_run_compliance_policy() {
        let mut manager = AuditManager::new();
        manager.add_compliance_policy(
            "latency".to_string(),
            CompliancePolicy {
                name: "Latency Policy".to_string(),
                description: "Max 100ms".to_string(),
                checks: vec![ComplianceCheck {
                    name: "latency".to_string(),
                    description: "latency <= 100ms".to_string(),
                    check_type: CheckType::MaxLatency,
                    threshold: 100.0,
                }],
            },
        );
        let metrics = AuditMetricsSnapshot {
            stage_count: 1,
            total_events_processed: 50,
            avg_latency_ms: 5.0,
            error_rate: 0.0,
            drop_rate: 0.0,
        };
        let results = manager.run_compliance_checks(&metrics);
        assert_eq!(results.len(), 1);
        assert!(results[0].overall_pass);
    }

    #[test]
    fn test_export_json() {
        let manager = AuditManager::new();
        let json = manager.export_audit_trail(ExportFormat::JSON).unwrap();
        assert_eq!(json, "[]");
    }

    #[test]
    fn test_regulatory_evidence() {
        let manager = AuditManager::new();
        let ev = manager.generate_regulatory_evidence("SOC2");
        assert_eq!(ev.regulation, "SOC2");
        assert!(ev.evidence_items.is_empty());
    }
}
