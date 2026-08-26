use super::*;
use protocol::broker_control::{
    BROKER_CONTROL_PROTOCOL_VERSION, BrokerHeartbeat, BrokerRegistration, CapacitySummary,
    MetadataUpdate, RegistrationAccepted,
};
use std::collections::VecDeque;

const DEFAULT_UPDATE_WINDOW: usize = 256;

#[derive(Clone)]
pub(super) struct BrokerControlRegistry {
    inner: Arc<Mutex<BrokerControlRegistryState>>,
}

struct BrokerControlRegistryState {
    next_session_id: u64,
    revision: u64,
    updates: VecDeque<MetadataUpdate>,
    brokers: HashMap<u64, BrokerSession>,
    update_window: usize,
}

#[derive(Debug, Clone)]
struct BrokerSession {
    incarnation: u64,
    session_id: u64,
    capacity: CapacitySummary,
    client_addr: String,
    replication_addr: Option<String>,
    feature_gates: Vec<String>,
    security_references: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationResult {
    pub accepted: RegistrationAccepted,
    pub fenced_session: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationError {
    UnsupportedVersion(u16),
    InvalidIdentity,
    StaleIncarnation,
    UnknownSession,
    FencedSession,
}

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported control protocol version {version}")
            }
            Self::InvalidIdentity => {
                formatter.write_str("broker identity and incarnation must be non-zero")
            }
            Self::StaleIncarnation => formatter.write_str("broker incarnation is stale"),
            Self::UnknownSession => formatter.write_str("broker session is unknown"),
            Self::FencedSession => formatter.write_str("broker session has been fenced"),
        }
    }
}

impl std::error::Error for RegistrationError {}

impl BrokerControlRegistry {
    pub(super) fn new() -> Self {
        Self::with_update_window(DEFAULT_UPDATE_WINDOW)
    }

    pub(super) fn with_update_window(update_window: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BrokerControlRegistryState {
                next_session_id: 1,
                revision: 0,
                updates: VecDeque::new(),
                brokers: HashMap::new(),
                update_window: update_window.max(1),
            })),
        }
    }

    pub(super) async fn register(
        &self,
        registration: BrokerRegistration,
    ) -> std::result::Result<RegistrationResult, RegistrationError> {
        if registration.protocol_version != BROKER_CONTROL_PROTOCOL_VERSION {
            return Err(RegistrationError::UnsupportedVersion(
                registration.protocol_version,
            ));
        }
        if registration.broker_id == 0 || registration.incarnation == 0 {
            return Err(RegistrationError::InvalidIdentity);
        }
        let mut state = self.inner.lock().await;
        let fenced_session = match state.brokers.get(&registration.broker_id) {
            Some(existing) if registration.incarnation < existing.incarnation => {
                return Err(RegistrationError::StaleIncarnation);
            }
            Some(existing) => Some(existing.session_id),
            None => None,
        };
        let session_id = state.next_session_id;
        state.next_session_id = state.next_session_id.saturating_add(1);
        state.brokers.insert(
            registration.broker_id,
            BrokerSession {
                incarnation: registration.incarnation,
                session_id,
                capacity: registration.capacity,
                client_addr: registration.client_addr,
                replication_addr: registration.replication_addr,
                feature_gates: registration.feature_gates,
                security_references: registration.security_references,
            },
        );
        let snapshot_required = registration.last_revision.saturating_add(1)
            < state
                .updates
                .front()
                .map_or(state.revision.saturating_add(1), |update| update.revision);
        let updates = if snapshot_required {
            Vec::new()
        } else {
            state
                .updates
                .iter()
                .filter(|update| update.revision > registration.last_revision)
                .cloned()
                .collect()
        };
        Ok(RegistrationResult {
            accepted: RegistrationAccepted {
                protocol_version: BROKER_CONTROL_PROTOCOL_VERSION,
                broker_id: registration.broker_id,
                incarnation: registration.incarnation,
                session_id,
                controller_revision: state.revision,
                updates,
                snapshot_required,
            },
            fenced_session,
        })
    }

    pub(super) async fn heartbeat(
        &self,
        heartbeat: BrokerHeartbeat,
    ) -> std::result::Result<(), RegistrationError> {
        if heartbeat.protocol_version != BROKER_CONTROL_PROTOCOL_VERSION {
            return Err(RegistrationError::UnsupportedVersion(
                heartbeat.protocol_version,
            ));
        }
        let mut state = self.inner.lock().await;
        let Some(session) = state.brokers.get_mut(&heartbeat.broker_id) else {
            return Err(RegistrationError::UnknownSession);
        };
        if session.session_id != heartbeat.session_id {
            return Err(RegistrationError::FencedSession);
        }
        if session.incarnation != heartbeat.incarnation {
            return Err(RegistrationError::StaleIncarnation);
        }
        session.capacity = heartbeat.capacity;
        Ok(())
    }

    pub(super) async fn publish_update(&self, payload: Vec<u8>) -> MetadataUpdate {
        let mut state = self.inner.lock().await;
        state.revision = state.revision.saturating_add(1);
        let update = MetadataUpdate::new(state.revision, payload);
        state.updates.push_back(update.clone());
        while state.updates.len() > state.update_window {
            state.updates.pop_front();
        }
        update
    }

    pub(super) async fn updates_after(&self, revision: u64) -> Option<Vec<MetadataUpdate>> {
        let state = self.inner.lock().await;
        if revision >= state.revision {
            return Some(Vec::new());
        }
        let first = state.updates.front()?.revision;
        if revision.saturating_add(1) < first {
            return None;
        }
        Some(
            state
                .updates
                .iter()
                .filter(|update| update.revision > revision)
                .cloned()
                .collect(),
        )
    }

    pub(super) async fn broker_count(&self) -> usize {
        self.inner.lock().await.brokers.len()
    }
}

impl Morrow {
    /// Register a broker on the controller control plane. The registry is
    /// intentionally independent of the fixed OpenRaft voter set, so combined
    /// and broker-only nodes can reconnect without changing quorum membership.
    pub async fn register_broker(
        &self,
        registration: BrokerRegistration,
    ) -> std::result::Result<RegistrationResult, RegistrationError> {
        self.broker_control.register(registration).await
    }

    pub async fn heartbeat_broker(
        &self,
        heartbeat: BrokerHeartbeat,
    ) -> std::result::Result<(), RegistrationError> {
        self.broker_control.heartbeat(heartbeat).await
    }

    pub async fn publish_broker_metadata_update(&self, payload: Vec<u8>) -> MetadataUpdate {
        self.broker_control.publish_update(payload).await
    }

    pub async fn broker_metadata_updates_after(
        &self,
        revision: u64,
    ) -> Option<Vec<MetadataUpdate>> {
        self.broker_control.updates_after(revision).await
    }

    pub async fn registered_broker_count(&self) -> usize {
        self.broker_control.broker_count().await
    }
}
