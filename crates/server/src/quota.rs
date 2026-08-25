use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{collections::HashMap, sync::Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ResourceQuotaConfig;

pub(crate) const DEFAULT_TENANT: &str = "default";

pub(crate) fn persistent_record_bytes(envelope: &crate::partition_log::MessageEnvelope) -> u64 {
    let headers = envelope.headers.iter().fold(0usize, |total, header| {
        total
            .saturating_add(header.name.len())
            .saturating_add(header.value.len())
    });
    128u64
        .saturating_add(envelope.namespace.len() as u64)
        .saturating_add(envelope.stream.as_str().len() as u64)
        .saturating_add(envelope.subject.len() as u64)
        .saturating_add(envelope.key.as_ref().map_or(0, Vec::len) as u64)
        .saturating_add(headers as u64)
        .saturating_add(envelope.reply_to.as_ref().map_or(0, String::len) as u64)
        .saturating_add(envelope.payload.len() as u64)
}

pub(crate) fn persistent_publish_record_bytes(record: &crate::wal::PublishRecord) -> u64 {
    let headers = record.headers.iter().fold(0usize, |total, header| {
        total
            .saturating_add(header.name.len())
            .saturating_add(header.value.len())
    });
    128u64
        .saturating_add(record.namespace.len() as u64)
        .saturating_add(record.stream.as_ref().map_or(0, String::len) as u64)
        .saturating_add(record.subject.len() as u64)
        .saturating_add(record.key.as_ref().map_or(0, Vec::len) as u64)
        .saturating_add(headers as u64)
        .saturating_add(record.reply_to.as_ref().map_or(0, String::len) as u64)
        .saturating_add(record.payload.len() as u64)
}

#[derive(Clone)]
pub(crate) struct QuotaRuntime {
    limits: ResourceQuotaConfig,
    clients: Arc<Semaphore>,
    http: Arc<Semaphore>,
    raft: Arc<Semaphore>,
    routes: Arc<Semaphore>,
    client_rejections: Arc<AtomicU64>,
    http_rejections: Arc<AtomicU64>,
    raft_rejections: Arc<AtomicU64>,
    route_rejections: Arc<AtomicU64>,
    state_rejections: Arc<AtomicU64>,
    outbound_rejections: Arc<AtomicU64>,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct QuotaSnapshot {
    pub(crate) connections: QuotaUsage,
    pub(crate) http_connections: QuotaUsage,
    pub(crate) raft_connections: QuotaUsage,
    pub(crate) route_connections: QuotaUsage,
    pub(crate) state_rejections: u64,
    pub(crate) outbound_rejections: u64,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct QuotaUsage {
    pub(crate) used: usize,
    pub(crate) limit: usize,
    pub(crate) rejections: u64,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(crate) struct TenantQuotaLimits {
    pub(crate) max_connections: usize,
    pub(crate) max_memory_bytes: u64,
    pub(crate) max_disk_bytes: u64,
    pub(crate) max_tasks: usize,
    pub(crate) max_background_tasks: usize,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct TenantQuotaUsage {
    pub(crate) connections: usize,
    pub(crate) memory_bytes: u64,
    pub(crate) disk_bytes: u64,
    pub(crate) tasks: usize,
    pub(crate) background_tasks: usize,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct TenantQuotaStatus {
    pub(crate) usage: TenantQuotaUsage,
    pub(crate) limits: TenantQuotaLimits,
    pub(crate) rejections: TenantQuotaRejections,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct TenantQuotaRejections {
    pub(crate) connections: u64,
    pub(crate) memory_bytes: u64,
    pub(crate) disk_bytes: u64,
    pub(crate) tasks: u64,
    pub(crate) background_tasks: u64,
}

#[derive(Clone)]
pub(crate) struct TenantQuotaRuntime {
    default_limits: TenantQuotaLimits,
    limits: Arc<Mutex<HashMap<String, TenantQuotaLimits>>>,
    usage: Arc<Mutex<HashMap<String, TenantQuotaUsage>>>,
    rejections: Arc<Mutex<HashMap<String, TenantQuotaRejections>>>,
}

impl TenantQuotaRuntime {
    pub(crate) fn new(limits: TenantQuotaLimits) -> Self {
        Self {
            default_limits: limits,
            limits: Arc::new(Mutex::new(HashMap::new())),
            usage: Arc::new(Mutex::new(HashMap::new())),
            rejections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn set_tenant_limits(&self, tenant: &str, limits: TenantQuotaLimits) {
        self.limits
            .lock()
            .expect("tenant quota limits lock poisoned")
            .insert(tenant.to_string(), limits);
    }

    fn limits_for(&self, tenant: &str) -> TenantQuotaLimits {
        self.limits
            .lock()
            .expect("tenant quota limits lock poisoned")
            .get(tenant)
            .copied()
            .unwrap_or(self.default_limits)
    }

    fn reject(&self, tenant: &str, dimension: fn(&mut TenantQuotaRejections)) {
        dimension(
            self.rejections
                .lock()
                .expect("tenant quota rejection lock poisoned")
                .entry(tenant.to_string())
                .or_default(),
        );
    }

    pub(crate) fn try_connection(&self, tenant: &str) -> bool {
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        let entry = usage.entry(tenant.to_string()).or_default();
        if entry.connections >= self.limits_for(tenant).max_connections {
            drop(usage);
            self.reject(tenant, |rejections| rejections.connections += 1);
            return false;
        }
        entry.connections += 1;
        true
    }

    pub(crate) fn release_connection(&self, tenant: &str) {
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        if let Some(entry) = usage.get_mut(tenant) {
            entry.connections = entry.connections.saturating_sub(1);
        }
    }

    pub(crate) fn transfer_connection(&self, from: &str, to: &str) -> bool {
        self.transfer(
            from,
            to,
            TenantQuotaUsage {
                connections: 1,
                ..Default::default()
            },
        )
    }

    pub(crate) fn transfer(&self, from: &str, to: &str, request: TenantQuotaUsage) -> bool {
        if from == to {
            return true;
        }
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        let source = usage.get(from).copied().unwrap_or_default();
        if source.connections < request.connections
            || source.memory_bytes < request.memory_bytes
            || source.disk_bytes < request.disk_bytes
            || source.tasks < request.tasks
            || source.background_tasks < request.background_tasks
        {
            return false;
        }
        let target = usage.get(to).copied().unwrap_or_default();
        let limits = self.limits_for(to);
        if target.connections.saturating_add(request.connections) > limits.max_connections
            || target.memory_bytes.saturating_add(request.memory_bytes) > limits.max_memory_bytes
            || target.disk_bytes.saturating_add(request.disk_bytes) > limits.max_disk_bytes
            || target.tasks.saturating_add(request.tasks) > limits.max_tasks
            || target
                .background_tasks
                .saturating_add(request.background_tasks)
                > limits.max_background_tasks
        {
            drop(usage);
            self.reject(to, |rejections| rejections.connections += 1);
            return false;
        }
        let source = usage.entry(from.to_string()).or_default();
        source.connections -= request.connections;
        source.memory_bytes -= request.memory_bytes;
        source.disk_bytes -= request.disk_bytes;
        source.tasks -= request.tasks;
        source.background_tasks -= request.background_tasks;
        let target = usage.entry(to.to_string()).or_default();
        target.connections += request.connections;
        target.memory_bytes += request.memory_bytes;
        target.disk_bytes += request.disk_bytes;
        target.tasks += request.tasks;
        target.background_tasks += request.background_tasks;
        true
    }

    pub(crate) fn try_reserve(&self, tenant: &str, request: TenantQuotaUsage) -> bool {
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        let entry = usage.entry(tenant.to_string()).or_default();
        let limits = self.limits_for(tenant);
        let dimension: Option<fn(&mut TenantQuotaRejections)> =
            if entry.connections.saturating_add(request.connections) > limits.max_connections {
                Some(|rejections: &mut TenantQuotaRejections| rejections.connections += 1)
            } else if entry.memory_bytes.saturating_add(request.memory_bytes)
                > limits.max_memory_bytes
            {
                Some(|rejections: &mut TenantQuotaRejections| rejections.memory_bytes += 1)
            } else if entry.disk_bytes.saturating_add(request.disk_bytes) > limits.max_disk_bytes {
                Some(|rejections: &mut TenantQuotaRejections| rejections.disk_bytes += 1)
            } else if entry.tasks.saturating_add(request.tasks) > limits.max_tasks {
                Some(|rejections: &mut TenantQuotaRejections| rejections.tasks += 1)
            } else if entry
                .background_tasks
                .saturating_add(request.background_tasks)
                > limits.max_background_tasks
            {
                Some(|rejections: &mut TenantQuotaRejections| rejections.background_tasks += 1)
            } else {
                None
            };
        let fits = dimension.is_none();
        if fits {
            entry.connections += request.connections;
            entry.memory_bytes += request.memory_bytes;
            entry.disk_bytes += request.disk_bytes;
            entry.tasks += request.tasks;
            entry.background_tasks += request.background_tasks;
        }
        drop(usage);
        if let Some(dimension) = dimension {
            self.reject(tenant, dimension);
        }
        fits
    }

    pub(crate) fn release(&self, tenant: &str, request: TenantQuotaUsage) {
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        if let Some(entry) = usage.get_mut(tenant) {
            entry.connections = entry.connections.saturating_sub(request.connections);
            entry.memory_bytes = entry.memory_bytes.saturating_sub(request.memory_bytes);
            entry.disk_bytes = entry.disk_bytes.saturating_sub(request.disk_bytes);
            entry.tasks = entry.tasks.saturating_sub(request.tasks);
            entry.background_tasks = entry
                .background_tasks
                .saturating_sub(request.background_tasks);
        }
    }

    pub(crate) fn snapshot(&self) -> HashMap<String, TenantQuotaUsage> {
        self.usage
            .lock()
            .expect("tenant quota lock poisoned")
            .clone()
    }

    pub(crate) fn replace_disk_usage(&self, disk_usage: HashMap<String, u64>) {
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        for (tenant, entry) in usage.iter_mut() {
            entry.disk_bytes = disk_usage.get(tenant).copied().unwrap_or_default();
        }
        for (tenant, disk_bytes) in disk_usage {
            usage.entry(tenant).or_default().disk_bytes = disk_bytes;
        }
    }

    pub(crate) fn status_snapshot(&self) -> HashMap<String, TenantQuotaStatus> {
        let usage = self.snapshot();
        let limits = self
            .limits
            .lock()
            .expect("tenant quota limits lock poisoned")
            .clone();
        let rejections = self
            .rejections
            .lock()
            .expect("tenant quota rejection lock poisoned")
            .clone();
        let mut tenants = usage
            .into_iter()
            .map(|(tenant, usage)| {
                let limit = limits.get(&tenant).copied().unwrap_or(self.default_limits);
                let rejection = rejections.get(&tenant).copied().unwrap_or_default();
                (
                    tenant,
                    TenantQuotaStatus {
                        usage,
                        limits: limit,
                        rejections: rejection,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        for (tenant, limit) in limits {
            let rejection = rejections.get(&tenant).copied().unwrap_or_default();
            tenants.entry(tenant).or_insert_with(|| TenantQuotaStatus {
                usage: TenantQuotaUsage::default(),
                limits: limit,
                rejections: rejection,
            });
        }
        tenants
    }
}

#[cfg(test)]
mod tenant_tests {
    use super::*;

    #[test]
    fn tenant_limits_bound_all_resource_dimensions() {
        let quotas = TenantQuotaRuntime::new(TenantQuotaLimits {
            max_connections: 1,
            max_memory_bytes: 10,
            max_disk_bytes: 20,
            max_tasks: 2,
            max_background_tasks: 1,
        });
        assert!(quotas.try_connection("tenant-a"));
        assert!(!quotas.try_connection("tenant-a"));
        assert!(quotas.try_reserve(
            "tenant-a",
            TenantQuotaUsage {
                memory_bytes: 10,
                disk_bytes: 20,
                tasks: 2,
                background_tasks: 1,
                ..Default::default()
            }
        ));
        assert!(!quotas.try_reserve(
            "tenant-a",
            TenantQuotaUsage {
                memory_bytes: 1,
                ..Default::default()
            }
        ));
        assert!(quotas.try_reserve(
            "tenant-b",
            TenantQuotaUsage {
                connections: 1,
                ..Default::default()
            }
        ));
        quotas.release_connection("tenant-a");
        assert!(quotas.try_connection("tenant-a"));
    }
}

impl QuotaRuntime {
    pub(crate) fn new(limits: &ResourceQuotaConfig) -> Self {
        Self {
            limits: limits.clone(),
            clients: Arc::new(Semaphore::new(limits.max_connections)),
            http: Arc::new(Semaphore::new(limits.max_http_connections)),
            raft: Arc::new(Semaphore::new(limits.max_raft_connections)),
            routes: Arc::new(Semaphore::new(limits.max_route_connections)),
            client_rejections: Arc::new(AtomicU64::new(0)),
            http_rejections: Arc::new(AtomicU64::new(0)),
            raft_rejections: Arc::new(AtomicU64::new(0)),
            route_rejections: Arc::new(AtomicU64::new(0)),
            state_rejections: Arc::new(AtomicU64::new(0)),
            outbound_rejections: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn try_client(&self) -> Option<OwnedSemaphorePermit> {
        acquire(&self.clients, &self.client_rejections)
    }

    pub(crate) fn try_http(&self) -> Option<OwnedSemaphorePermit> {
        acquire(&self.http, &self.http_rejections)
    }

    pub(crate) fn try_raft(&self) -> Option<OwnedSemaphorePermit> {
        acquire(&self.raft, &self.raft_rejections)
    }

    pub(crate) fn try_route(&self) -> Option<OwnedSemaphorePermit> {
        acquire(&self.routes, &self.route_rejections)
    }

    pub(crate) fn reject_state(&self) {
        self.state_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reject_outbound(&self) {
        self.outbound_rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> QuotaSnapshot {
        QuotaSnapshot {
            connections: usage(
                &self.clients,
                self.limits.max_connections,
                &self.client_rejections,
            ),
            http_connections: usage(
                &self.http,
                self.limits.max_http_connections,
                &self.http_rejections,
            ),
            raft_connections: usage(
                &self.raft,
                self.limits.max_raft_connections,
                &self.raft_rejections,
            ),
            route_connections: usage(
                &self.routes,
                self.limits.max_route_connections,
                &self.route_rejections,
            ),
            state_rejections: self.state_rejections.load(Ordering::Relaxed),
            outbound_rejections: self.outbound_rejections.load(Ordering::Relaxed),
        }
    }
}

fn acquire(semaphore: &Arc<Semaphore>, rejected: &AtomicU64) -> Option<OwnedSemaphorePermit> {
    match semaphore.clone().try_acquire_owned() {
        Ok(permit) => Some(permit),
        Err(_) => {
            rejected.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

fn usage(semaphore: &Semaphore, limit: usize, rejected: &AtomicU64) -> QuotaUsage {
    QuotaUsage {
        used: limit.saturating_sub(semaphore.available_permits()),
        limit,
        rejections: rejected.load(Ordering::Relaxed),
    }
}
