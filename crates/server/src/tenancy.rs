//! Tenant-scoped policy and tamper-evident administrative audit primitives.
//!
//! These types are deliberately independent of the wire protocol. Broker entry
//! points can consult one live `PolicyStore` before scheduling middleware,
//! connector, storage, or cluster work, while the audit chain remains append-only
//! and verifiable on export.

use crate::error::{BrokerError, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
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

impl ResourceScope {
    pub fn subject_prefix(&self) -> String {
        format!("{}.{}.", self.tenant.as_str(), self.namespace.as_str())
    }

    pub fn contains_subject(&self, subject: &str) -> bool {
        subject.starts_with(&self.subject_prefix())
    }
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
    bindings_by_subject_scope: HashMap<(String, ResourceScope), Vec<RoleBinding>>,
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
        rebuild_binding_index(&mut state);
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
        rebuild_binding_index(&mut state);
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
        let allowed = state
            .bindings_by_subject_scope
            .get(&(subject.to_string(), scope.clone()))
            .into_iter()
            .flatten()
            .any(|binding| {
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

    pub fn snapshot_for_scope(&self, scope: &ResourceScope) -> PolicySnapshot {
        let state = self.state.read().expect("policy lock poisoned");
        let bindings = state
            .bindings
            .iter()
            .filter(|binding| &binding.scope == scope)
            .cloned()
            .collect::<Vec<_>>();
        let roles = bindings
            .iter()
            .filter_map(|binding| {
                state
                    .roles
                    .get(&binding.role)
                    .map(|role| (role.name.clone(), role.clone()))
            })
            .collect();
        PolicySnapshot {
            generation: state.generation,
            roles,
            bindings,
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
        rebuild_binding_index(&mut state);
        Ok(())
    }
}

fn rebuild_binding_index(state: &mut PolicyState) {
    state.bindings_by_subject_scope.clear();
    for binding in &state.bindings {
        state
            .bindings_by_subject_scope
            .entry((binding.subject.clone(), binding.scope.clone()))
            .or_default()
            .push(binding.clone());
    }
}

fn validate_role(role: &Role) -> Result<()> {
    validate_identifier(&role.name, "role")
}

fn validate_binding(binding: &RoleBinding) -> Result<()> {
    crate::broker_ensure!(
        !binding.subject.is_empty()
            && binding.subject.len() <= MAX_IDENTIFIER_BYTES
            && !binding.subject.chars().any(char::is_whitespace),
        "policy subject must be non-empty, bounded, and contain no whitespace"
    );
    Ok(())
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditStatus {
    pub records_written: u64,
    pub bytes_written: u64,
    pub rotations: u64,
    pub export_position: u64,
    pub write_failures: u64,
    pub verification_failures: u64,
    pub oldest_retained_sequence: Option<u64>,
    pub newest_retained_sequence: Option<u64>,
}

#[derive(Debug)]
pub struct AuditLog {
    records: Vec<AuditRecord>,
    max_records: usize,
    next_sequence: u64,
    last_hash: String,
    segment_bytes_limit: u64,
    segment_index: u64,
    current_segment_bytes: u64,
    bytes_written: u64,
    rotations: u64,
    write_failures: u64,
    verification_failures: u64,
    path: Option<PathBuf>,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            max_records: 10_000,
            next_sequence: 0,
            last_hash: "0".repeat(64),
            segment_bytes_limit: 16 * 1_048_576,
            segment_index: 0,
            current_segment_bytes: 0,
            bytes_written: 0,
            rotations: 0,
            write_failures: 0,
            verification_failures: 0,
            path: None,
        }
    }
}

impl AuditLog {
    pub fn with_capacity(max_records: usize) -> Result<Self> {
        crate::broker_ensure!(max_records > 0, "audit log capacity must be positive");
        Ok(Self {
            records: Vec::new(),
            max_records,
            next_sequence: 0,
            last_hash: "0".repeat(64),
            segment_bytes_limit: 16 * 1_048_576,
            segment_index: 0,
            current_segment_bytes: 0,
            bytes_written: 0,
            rotations: 0,
            write_failures: 0,
            verification_failures: 0,
            path: None,
        })
    }

    pub fn open(path: impl AsRef<Path>, max_records: usize) -> Result<Self> {
        Self::open_with_segment_bytes(path, max_records, 16 * 1_048_576)
    }

    pub fn open_with_segment_bytes(
        path: impl AsRef<Path>,
        max_records: usize,
        segment_bytes_limit: u64,
    ) -> Result<Self> {
        crate::broker_ensure!(max_records > 0, "audit log capacity must be positive");
        crate::broker_ensure!(
            segment_bytes_limit > 0,
            "audit log segment size must be positive"
        );
        let path = path.as_ref().to_path_buf();
        let mut records = Vec::new();
        let mut next_sequence = 0;
        let mut last_hash = "0".repeat(64);
        let segment_paths = audit_segment_paths(&path)?;
        let mut bytes_written: u64 = 0;
        for (_, segment_path) in &segment_paths {
            let file = File::open(segment_path)
                .map_err(|error| BrokerError::with_source("opening audit log", error))?;
            for line in BufReader::new(file).lines() {
                let line =
                    line.map_err(|error| BrokerError::with_source("reading audit log", error))?;
                if line.trim().is_empty() {
                    continue;
                }
                let record: AuditRecord = serde_json::from_str(&line)
                    .map_err(|error| BrokerError::with_source("decoding audit record", error))?;
                verify_audit_record(&record, next_sequence, &last_hash)?;
                next_sequence = next_sequence.saturating_add(1);
                last_hash = record.hash.clone();
                records.push(record);
                if records.len() > max_records {
                    records.remove(0);
                }
            }
            bytes_written = bytes_written.saturating_add(
                fs::metadata(segment_path)
                    .map_err(|error| BrokerError::with_source("reading audit metadata", error))?
                    .len(),
            );
        }
        let (segment_index, current_segment_bytes) = segment_paths
            .last()
            .map(|(index, segment_path)| {
                (
                    *index,
                    fs::metadata(segment_path)
                        .map(|metadata| metadata.len())
                        .unwrap_or(0),
                )
            })
            .unwrap_or((0, 0));
        Ok(Self {
            records,
            max_records,
            next_sequence,
            last_hash,
            segment_bytes_limit,
            segment_index,
            current_segment_bytes,
            bytes_written,
            rotations: segment_index,
            write_failures: 0,
            verification_failures: 0,
            path: Some(path),
        })
    }

    pub fn append(&mut self, mut event: AuditEvent) -> Result<&AuditRecord> {
        event.sequence = self.next_sequence;
        let previous_hash = self.last_hash.clone();
        let hash = audit_hash(&event, &previous_hash)?;
        let record = AuditRecord {
            event,
            previous_hash,
            hash,
        };
        let encoded = serde_json::to_vec(&record)
            .map_err(|error| BrokerError::with_source("encoding audit record", error))?;
        let bytes = encoded.len() as u64 + 1;
        if let Some(path) = &self.path {
            let rotate = self.current_segment_bytes > 0
                && self.current_segment_bytes.saturating_add(bytes) > self.segment_bytes_limit;
            let segment_path = if rotate {
                self.segment_index = self.segment_index.saturating_add(1);
                self.current_segment_bytes = 0;
                audit_segment_path(path, self.segment_index)
            } else {
                audit_segment_path(path, self.segment_index)
            };
            let result = (|| -> Result<()> {
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&segment_path)
                    .map_err(|error| {
                        BrokerError::with_source("opening audit log for append", error)
                    })?;
                file.write_all(&encoded)
                    .map_err(|error| BrokerError::with_source("writing audit record", error))?;
                file.write_all(b"\n")
                    .map_err(|error| BrokerError::with_source("writing audit record", error))?;
                file.sync_data()
                    .map_err(|error| BrokerError::with_source("syncing audit record", error))?;
                Ok(())
            })();
            if let Err(error) = result {
                self.write_failures = self.write_failures.saturating_add(1);
                return Err(error);
            }
            if rotate {
                self.rotations = self.rotations.saturating_add(1);
            }
            self.current_segment_bytes = self.current_segment_bytes.saturating_add(bytes);
            self.bytes_written = self.bytes_written.saturating_add(bytes);
        }
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.last_hash = record.hash.clone();
        self.records.push(record);
        if self.records.len() > self.max_records {
            self.records.remove(0);
        }
        Ok(self.records.last().expect("record was appended"))
    }

    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }

    pub fn records_for_tenant(&self, tenant: &TenantId) -> Vec<AuditRecord> {
        self.records
            .iter()
            .filter(|record| record.event.tenant.as_ref() == Some(tenant))
            .cloned()
            .collect()
    }

    pub fn verify(&mut self) -> Result<()> {
        let result = if let Some(path) = &self.path {
            verify_audit_files(&audit_segment_paths(path)?)
        } else {
            verify_audit_records(&self.records)
        };
        if result.is_err() {
            self.verification_failures = self.verification_failures.saturating_add(1);
        }
        result
    }

    pub fn verify_export(records: &[AuditRecord]) -> Result<()> {
        verify_audit_records(records)
    }

    pub fn export_json(&mut self) -> Result<Vec<u8>> {
        self.verify()?;
        let mut output = Vec::new();
        if let Some(path) = &self.path {
            for (_, segment_path) in audit_segment_paths(path)? {
                let file = File::open(segment_path)
                    .map_err(|error| BrokerError::with_source("opening audit export", error))?;
                for line in BufReader::new(file).lines() {
                    let line = line
                        .map_err(|error| BrokerError::with_source("reading audit export", error))?;
                    if !line.trim().is_empty() {
                        output.extend_from_slice(line.as_bytes());
                        output.push(b'\n');
                    }
                }
            }
        } else {
            for record in &self.records {
                serde_json::to_writer(&mut output, record)
                    .map_err(|error| BrokerError::with_source("encoding audit export", error))?;
                output.push(b'\n');
            }
        }
        Ok(output)
    }

    pub fn status(&self) -> AuditStatus {
        AuditStatus {
            records_written: self.next_sequence,
            bytes_written: self.bytes_written,
            rotations: self.rotations,
            export_position: self.next_sequence,
            write_failures: self.write_failures,
            verification_failures: self.verification_failures,
            oldest_retained_sequence: self.records.first().map(|record| record.event.sequence),
            newest_retained_sequence: self.records.last().map(|record| record.event.sequence),
        }
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
        verify_audit_record(record, sequence as u64, &previous_hash)?;
        previous_hash = record.hash.clone();
    }
    Ok(())
}

fn verify_audit_record(record: &AuditRecord, sequence: u64, previous_hash: &str) -> Result<()> {
    crate::broker_ensure!(
        record.event.sequence == sequence,
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
    Ok(())
}

fn audit_segment_path(path: &Path, index: u64) -> PathBuf {
    if index == 0 {
        path.to_path_buf()
    } else {
        PathBuf::from(format!("{}.segment-{index:08}", path.to_string_lossy()))
    }
}

fn audit_segment_paths(path: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut paths = Vec::new();
    if path.exists() {
        paths.push((0, path.to_path_buf()));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let prefix = format!(
        "{}.segment-",
        path.file_name().unwrap_or_default().to_string_lossy()
    );
    if let Ok(entries) = fs::read_dir(parent) {
        for entry in entries {
            let entry = entry.map_err(|error| {
                BrokerError::with_source("reading audit segment directory", error)
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(index) = name
                .strip_prefix(&prefix)
                .and_then(|value| value.parse().ok())
            else {
                continue;
            };
            paths.push((index, entry.path()));
        }
    }
    paths.sort_by_key(|(index, _)| *index);
    Ok(paths)
}

fn verify_audit_files(paths: &[(u64, PathBuf)]) -> Result<()> {
    let mut sequence = 0;
    let mut previous_hash = "0".repeat(64);
    for (_, path) in paths {
        let file = File::open(path).map_err(|error| {
            BrokerError::with_source("opening audit log for verification", error)
        })?;
        for line in BufReader::new(file).lines() {
            let line =
                line.map_err(|error| BrokerError::with_source("reading audit log", error))?;
            if line.trim().is_empty() {
                continue;
            }
            let record: AuditRecord = serde_json::from_str(&line)
                .map_err(|error| BrokerError::with_source("decoding audit record", error))?;
            verify_audit_record(&record, sequence, &previous_hash)?;
            sequence = sequence.saturating_add(1);
            previous_hash = record.hash;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
    fn scoped_policy_and_audit_exports_do_not_cross_tenants() {
        let store = PolicyStore::default();
        store
            .upsert_role(Role {
                name: "publisher".to_string(),
                permissions: [Permission::Publish].into_iter().collect(),
            })
            .unwrap();
        let tenant_a = scope();
        let tenant_b = ResourceScope {
            tenant: TenantId::new("tenant-b").unwrap(),
            namespace: NamespaceId::new("orders").unwrap(),
        };
        for (subject, scoped) in [("a", tenant_a.clone()), ("b", tenant_b.clone())] {
            store
                .bind(RoleBinding {
                    subject: subject.to_string(),
                    scope: scoped,
                    role: "publisher".to_string(),
                    expires_at_ms: None,
                })
                .unwrap();
        }
        assert_eq!(store.snapshot_for_scope(&tenant_a).bindings.len(), 1);
        assert_eq!(store.snapshot_for_scope(&tenant_a).bindings[0].subject, "a");
        let mut audit = AuditLog::with_capacity(8).unwrap();
        for tenant in [&tenant_a.tenant, &tenant_b.tenant] {
            audit
                .append(AuditEvent {
                    sequence: 0,
                    timestamp_ms: 1,
                    actor: "operator".into(),
                    tenant: Some(tenant.clone()),
                    action: "policy.update".into(),
                    resource: "orders".into(),
                    outcome: "success".into(),
                    details: BTreeMap::new(),
                })
                .unwrap();
        }
        assert_eq!(audit.records_for_tenant(&tenant_a.tenant).len(), 1);
        assert_eq!(audit.records_for_tenant(&tenant_b.tenant).len(), 1);
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

    #[test]
    fn persisted_audit_chain_survives_reopen_and_rejects_tampering() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let mut log = AuditLog::open(&path, 8).unwrap();
        log.append(AuditEvent {
            sequence: 0,
            timestamp_ms: 1,
            actor: "operator".to_string(),
            tenant: Some(TenantId::new("tenant-a").unwrap()),
            action: "policy.update".to_string(),
            resource: "tenant-a/orders".to_string(),
            outcome: "success".to_string(),
            details: BTreeMap::new(),
        })
        .unwrap();
        drop(log);
        let mut reopened = AuditLog::open(&path, 8).unwrap();
        assert_eq!(reopened.records().len(), 1);
        assert_eq!(reopened.export_json().unwrap().lines().count(), 1);
        let mut bytes = std::fs::read(&path).unwrap();
        let index = bytes.len() - 3;
        bytes[index] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(AuditLog::open(&path, 8).is_err());
    }

    #[test]
    fn persisted_audit_log_keeps_appending_beyond_the_memory_window() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let mut log = AuditLog::open_with_segment_bytes(&path, 8, 512).unwrap();
        for sequence in 0..10_001 {
            log.append(AuditEvent {
                sequence,
                timestamp_ms: sequence,
                actor: "operator".to_string(),
                tenant: None,
                action: "health.check".to_string(),
                resource: "cluster".to_string(),
                outcome: "success".to_string(),
                details: BTreeMap::new(),
            })
            .unwrap();
        }
        assert_eq!(log.records().len(), 8);
        assert_eq!(log.records().first().unwrap().event.sequence, 9_993);
        assert!(log.status().rotations > 1);
        log.verify().unwrap();
        drop(log);
        let mut reopened = AuditLog::open_with_segment_bytes(&path, 8, 512).unwrap();
        assert_eq!(reopened.export_json().unwrap().lines().count(), 10_001);
        assert_eq!(reopened.records().last().unwrap().event.sequence, 10_000);
    }

    #[test]
    fn audit_write_failure_is_counted_without_advancing_the_chain() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing").join("audit.log");
        let mut log = AuditLog::open(&path, 8).unwrap();
        let error = log
            .append(AuditEvent {
                sequence: 0,
                timestamp_ms: 1,
                actor: "operator".to_string(),
                tenant: None,
                action: "health.check".to_string(),
                resource: "cluster".to_string(),
                outcome: "success".to_string(),
                details: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("opening audit log"));
        assert_eq!(log.status().write_failures, 1);
        assert_eq!(log.status().records_written, 0);
    }
}
