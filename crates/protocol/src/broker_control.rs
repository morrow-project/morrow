//! Versioned control-plane messages used to register brokers with controllers.
//!
//! The control stream is deliberately separate from the client protocol.  It
//! carries bounded registration/heartbeat traffic and resumable metadata
//! updates; message payloads are checksummed so a reconnect cannot silently
//! apply a truncated or reordered update.

use serde::{Deserialize, Serialize};

pub const BROKER_CONTROL_PROTOCOL_VERSION: u16 = 1;
pub const MAX_CONTROL_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRegistration {
    pub protocol_version: u16,
    pub broker_id: u64,
    pub incarnation: u64,
    pub client_addr: String,
    pub replication_addr: Option<String>,
    pub capacity: CapacitySummary,
    pub feature_gates: Vec<String>,
    pub security_references: Vec<String>,
    pub last_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CapacitySummary {
    pub disk_bytes: u64,
    pub disk_used_bytes: u64,
    pub partition_count: u32,
    pub leader_count: u32,
    pub throughput_bytes_per_second: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerHeartbeat {
    pub protocol_version: u16,
    pub broker_id: u64,
    pub incarnation: u64,
    pub session_id: u64,
    pub capacity: CapacitySummary,
    pub last_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatAccepted {
    pub controller_revision: u64,
    pub updates: Vec<MetadataUpdate>,
    pub snapshot_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataUpdate {
    pub revision: u64,
    pub checksum: u32,
    pub payload: Vec<u8>,
}

impl MetadataUpdate {
    pub fn new(revision: u64, payload: Vec<u8>) -> Self {
        Self {
            revision,
            checksum: checksum(&payload),
            payload,
        }
    }

    pub fn verify(&self) -> bool {
        checksum(&self.payload) == self.checksum
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationAccepted {
    pub protocol_version: u16,
    pub broker_id: u64,
    pub incarnation: u64,
    pub session_id: u64,
    pub controller_revision: u64,
    pub updates: Vec<MetadataUpdate>,
    pub snapshot_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerControlFrame {
    Register(BrokerRegistration),
    RegisterAccepted(RegistrationAccepted),
    Heartbeat(BrokerHeartbeat),
    HeartbeatAccepted(HeartbeatAccepted),
    MetadataUpdate(MetadataUpdate),
    MetadataSnapshot(MetadataUpdate),
    Error(ControlError),
}

impl BrokerControlFrame {
    pub fn encode(&self) -> Result<Vec<u8>, ControlCodecError> {
        let mut body = Vec::new();
        ciborium::ser::into_writer(self, &mut body).map_err(ControlCodecError::Encode)?;
        if body.len() > MAX_CONTROL_FRAME_BYTES {
            return Err(ControlCodecError::FrameTooLarge);
        }
        let len = u32::try_from(body.len()).map_err(|_| ControlCodecError::FrameTooLarge)?;
        let mut encoded = Vec::with_capacity(4 + body.len());
        encoded.extend_from_slice(&len.to_be_bytes());
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, ControlCodecError> {
        if encoded.len() < 4 {
            return Err(ControlCodecError::InvalidLength);
        }
        let len = u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize;
        if len > MAX_CONTROL_FRAME_BYTES || encoded.len() != len + 4 {
            return Err(ControlCodecError::InvalidLength);
        }
        let frame: Self =
            ciborium::de::from_reader(&encoded[4..]).map_err(ControlCodecError::Decode)?;
        let updates = match &frame {
            Self::MetadataUpdate(update) | Self::MetadataSnapshot(update) => {
                std::slice::from_ref(update)
            }
            Self::RegisterAccepted(accepted) => accepted.updates.as_slice(),
            Self::HeartbeatAccepted(accepted) => accepted.updates.as_slice(),
            _ => &[],
        };
        if updates.iter().any(|update| !update.verify())
            || matches!(&frame, Self::RegisterAccepted(accepted)
                if accepted.updates.windows(2).any(|w| w[0].revision >= w[1].revision))
            || matches!(&frame, Self::HeartbeatAccepted(accepted)
                if accepted.updates.windows(2).any(|w| w[0].revision >= w[1].revision))
        {
            return Err(ControlCodecError::ChecksumMismatch);
        }
        Ok(frame)
    }
}

#[derive(Debug)]
pub enum ControlCodecError {
    Encode(ciborium::ser::Error<std::io::Error>),
    Decode(ciborium::de::Error<std::io::Error>),
    FrameTooLarge,
    InvalidLength,
    ChecksumMismatch,
}

fn checksum(payload: &[u8]) -> u32 {
    crc32fast::hash(payload)
}
