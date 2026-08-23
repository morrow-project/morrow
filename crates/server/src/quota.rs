use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{collections::HashMap, sync::Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ResourceQuotaConfig;

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

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone)]
pub(crate) struct TenantQuotaRuntime {
    limits: TenantQuotaLimits,
    usage: Arc<Mutex<HashMap<String, TenantQuotaUsage>>>,
}

impl TenantQuotaRuntime {
    pub(crate) fn new(limits: TenantQuotaLimits) -> Self {
        Self {
            limits,
            usage: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn try_connection(&self, tenant: &str) -> bool {
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        let entry = usage.entry(tenant.to_string()).or_default();
        if entry.connections >= self.limits.max_connections {
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

    pub(crate) fn try_reserve(&self, tenant: &str, request: TenantQuotaUsage) -> bool {
        let mut usage = self.usage.lock().expect("tenant quota lock poisoned");
        let entry = usage.entry(tenant.to_string()).or_default();
        let fits = entry.connections.saturating_add(request.connections)
            <= self.limits.max_connections
            && entry.memory_bytes.saturating_add(request.memory_bytes)
                <= self.limits.max_memory_bytes
            && entry.disk_bytes.saturating_add(request.disk_bytes) <= self.limits.max_disk_bytes
            && entry.tasks.saturating_add(request.tasks) <= self.limits.max_tasks
            && entry
                .background_tasks
                .saturating_add(request.background_tasks)
                <= self.limits.max_background_tasks;
        if fits {
            entry.connections += request.connections;
            entry.memory_bytes += request.memory_bytes;
            entry.disk_bytes += request.disk_bytes;
            entry.tasks += request.tasks;
            entry.background_tasks += request.background_tasks;
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
