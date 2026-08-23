//! Tenant-scoped policy and tamper-evident administrative audit primitives.
//!
//! These types are deliberately independent of the wire protocol. Broker entry
//! points can consult one live `PolicyStore` before scheduling middleware,
//! connector, storage, or cluster work, while the audit chain remains append-only
//! and verifiable on export.

use crate::error::{BrokerError, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

const MAX_IDENTIFIER_BYTES: usize = 128;

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier(&value, "tenant")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct NamespaceId(String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier(&value, "namespace")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_identifier(value: &str, kind: &str) -> Result<()> {
    crate::broker_ensure!(
        !value.is_empty() && value.len() <= MAX_IDENTIFIER_BYTES,
        "{kind} identifier must be non-empty and bounded"
    );
    crate::broker_ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "{kind} identifier contains an invalid character"
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ResourceScope {
    pub tenant: TenantId,
    pub namespace: NamespaceId,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Publish,
    Subscribe,
    ManageStreams,
    ManageConsumers,
    ManageConnectors,
    ManageMiddleware,
    OperateCluster,
    Observe,
    ManageAuth,
    ManageKeys,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: BTreeSet<Permission>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RoleBinding {
    pub subject: String,
    pub scope: ResourceScope,
    pub role: String,
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PolicySnapshot {
    pub generation: u64,
    pub roles: BTreeMap<String, Role>,
    pub bindings: Vec<RoleBinding>,
}

#[derive(Debug, Default)]
struct PolicyState {
    generation: u64,
    roles: BTreeMap<String, Role>,
    bindings: Vec<RoleBinding>,
}

#[derive(Clone, Debug, Default)]
pub struct PolicyStore {
    state: Arc<RwLock<PolicyState>>,
}

impl PolicyStore {
    pub fn upsert_role(&self, role: Role) -> Result<u64> {
        validate_role(&role)?;
        let mut state = self.state.write().expect("policy lock poisoned");
        state.roles.insert(role.name.clone(), role);
        state.generation = state.generation.saturating_add(1);
        Ok(state.generation)
    }

    pub fn bind(&self, binding: RoleBinding) -> Result<u64> {
        validate_binding(&binding)?;
        let mut state = self.state.write().expect("policy lock poisoned");
        crate::broker_ensure!(
            state.roles.contains_key(&binding.role),
            "cannot bind an unknown role"
        );
        state.bindings.retain(|existing| {
            !(existing.subject == binding.subject
                && existing.scope == binding.scope
                && existing.role == binding.role)
        });
        state.bindings.push(binding);
        state.generation = state.generation.saturating_add(1);
        Ok(state.generation)
    }

    pub fn revoke(&self, subject: &str, scope: &ResourceScope, role: Option<&str>) -> u64 {
        let mut state = self.state.write().expect("policy lock poisoned");
        state.bindings.retain(|binding| {
            !(binding.subject == subject
                && &binding.scope == scope
                && role.is_none_or(|role| binding.role == role))
        });
        state.generation = state.generation.saturating_add(1);
        state.generation
    }

    pub fn authorize(
        &self,
        subject: &str,
        scope: &ResourceScope,
        permission: Permission,
        now_ms: u64,
    ) -> Result<()> {
        let state = self.state.read().expect("policy lock poisoned");
        let allowed = state.bindings.iter().any(|binding| {
            binding.subject == subject
                && binding.scope == *scope
                && binding.expires_at_ms.is_none_or(|expires| now_ms < expires)
                && state
                    .roles
                    .get(&binding.role)
                    .is_some_and(|role| role.permissions.contains(&permission))
        });
        crate::broker_ensure!(allowed, "tenant permission denied");
        Ok(())
    }

    pub fn snapshot(&self) -> PolicySnapshot {
        let state = self.state.read().expect("policy lock poisoned");
        PolicySnapshot {
            generation: state.generation,
            roles: state.roles.clone(),
            bindings: state.bindings.clone(),
        }
    }

    pub fn replace(&self, snapshot: PolicySnapshot) -> Result<()> {
        for role in snapshot.roles.values() {
            validate_role(role)?;
        }
        for binding in &snapshot.bindings {
            validate_binding(binding)?;
            crate::broker_ensure!(
                snapshot.roles.contains_key(&binding.role),
                "policy snapshot binds an unknown role"
            );
        }
        let mut state = self.state.write().expect("policy lock poisoned");
        crate::broker_ensure!(
            snapshot.generation >= state.generation,
            "policy generation would move backwards"
        );
        state.generation = snapshot.generation;
        state.roles = snapshot.roles;
        state.bindings = snapshot.bindings;
        Ok(())
    }
}

fn validate_role(role: &Role) -> Result<()> {
    validate_identifier(&role.name, "role")
}

fn validate_binding(binding: &RoleBinding) -> Result<()> {
    validate_identifier(&binding.subject, "policy subject")
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditEvent {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub actor: String,
    pub tenant: Option<TenantId>,
    pub action: String,
    pub resource: String,
    pub outcome: String,
    pub details: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditRecord {
    pub event: AuditEvent,
    pub previous_hash: String,
    pub hash: String,
}

#[derive(Debug)]
pub struct AuditLog {
    records: Vec<AuditRecord>,
    max_records: usize,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            max_records: 10_000,
        }
    }
}

impl AuditLog {
    pub fn with_capacity(max_records: usize) -> Result<Self> {
        crate::broker_ensure!(max_records > 0, "audit log capacity must be positive");
        Ok(Self {
            records: Vec::new(),
            max_records,
        })
    }

    pub fn append(&mut self, mut event: AuditEvent) -> Result<&AuditRecord> {
        crate::broker_ensure!(
            self.records.len() < self.max_records,
            "audit log capacity exceeded"
        );
        event.sequence = self.records.len() as u64;
        let previous_hash = self
            .records
            .last()
            .map_or_else(|| "0".repeat(64), |record| record.hash.clone());
        let hash = audit_hash(&event, &previous_hash)?;
        self.records.push(AuditRecord {
            event,
            previous_hash,
            hash,
        });
        Ok(self.records.last().expect("record was appended"))
    }

    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }

    pub fn verify(&self) -> Result<()> {
        verify_audit_records(&self.records)
    }

    pub fn verify_export(records: &[AuditRecord]) -> Result<()> {
        verify_audit_records(records)
    }
}

fn audit_hash(event: &AuditEvent, previous_hash: &str) -> Result<String> {
    let encoded = serde_json::to_vec(&(event, previous_hash))
        .map_err(|error| BrokerError::msg(error.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(encoded);
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn verify_audit_records(records: &[AuditRecord]) -> Result<()> {
    let mut previous_hash = "0".repeat(64);
    for (sequence, record) in records.iter().enumerate() {
        crate::broker_ensure!(
            record.event.sequence == sequence as u64,
            "audit sequence is invalid"
        );
        crate::broker_ensure!(
            record.previous_hash == previous_hash,
            "audit chain link is invalid"
        );
        crate::broker_ensure!(
            audit_hash(&record.event, &record.previous_hash)? == record.hash,
            "audit record hash is invalid"
        );
        previous_hash = record.hash.clone();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> ResourceScope {
        ResourceScope {
            tenant: TenantId::new("tenant-a").unwrap(),
            namespace: NamespaceId::new("orders").unwrap(),
        }
    }

    #[test]
    fn policy_updates_revoke_immediately_and_expire_bindings() {
        let store = PolicyStore::default();
        store
            .upsert_role(Role {
                name: "publisher".to_string(),
                permissions: [Permission::Publish].into_iter().collect(),
            })
            .unwrap();
        store
            .bind(RoleBinding {
                subject: "client-a".to_string(),
                scope: scope(),
                role: "publisher".to_string(),
                expires_at_ms: Some(100),
            })
            .unwrap();
        assert!(
            store
                .authorize("client-a", &scope(), Permission::Publish, 99)
                .is_ok()
        );
        assert!(
            store
                .authorize("client-a", &scope(), Permission::Publish, 100)
                .is_err()
        );
        store
            .bind(RoleBinding {
                subject: "client-a".to_string(),
                scope: scope(),
                role: "publisher".to_string(),
                expires_at_ms: None,
            })
            .unwrap();
        store.revoke("client-a", &scope(), Some("publisher"));
        assert!(
            store
                .authorize("client-a", &scope(), Permission::Publish, 0)
                .is_err()
        );
    }

    #[test]
    fn audit_verification_detects_modification_deletion_and_reordering() {
        let mut log = AuditLog::with_capacity(8).unwrap();
        for action in ["auth.change", "stream.delete", "key.rotate"] {
            log.append(AuditEvent {
                sequence: 0,
                timestamp_ms: 1,
                actor: "admin".to_string(),
                tenant: Some(TenantId::new("tenant-a").unwrap()),
                action: action.to_string(),
                resource: "orders".to_string(),
                outcome: "success".to_string(),
                details: BTreeMap::new(),
            })
            .unwrap();
        }
        log.verify().unwrap();
        let mut modified = log.records().to_vec();
        modified[1].event.action = "different".to_string();
        assert!(AuditLog::verify_export(&modified).is_err());
        let mut deleted = log.records().to_vec();
        deleted.remove(1);
        assert!(AuditLog::verify_export(&deleted).is_err());
        let mut reordered = log.records().to_vec();
        reordered.swap(0, 1);
        assert!(AuditLog::verify_export(&reordered).is_err());
    }
}
