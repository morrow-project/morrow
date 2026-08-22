use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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
