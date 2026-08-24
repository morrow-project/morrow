//! Protocol-independent values shared by all Morrow wire codecs.
//!
//! The text and CBOR frontends are responsible only for translating bytes to
//! and from these values. Broker code should eventually exchange these types
//! at the transport boundary instead of depending on a particular encoding.

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WireEncoding {
    Text,
    Cbor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub protocol_versions: Vec<u16>,
    pub encodings: Vec<WireEncoding>,
    pub features: Vec<String>,
    pub max_frame_size: usize,
    pub max_metadata_size: usize,
    pub max_payload_size: usize,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            // Version 2 is retained here for the current text command surface;
            // the new binary/text semantic model is protocol v1.
            protocol_versions: vec![PROTOCOL_VERSION, 2],
            encodings: vec![WireEncoding::Text, WireEncoding::Cbor],
            features: vec![
                "request-ids".into(),
                "checksums".into(),
                "credit-flow-control".into(),
            ],
            max_frame_size: crate::cbor::DEFAULT_MAX_FRAME_SIZE,
            max_metadata_size: crate::cbor::DEFAULT_MAX_FRAME_SIZE,
            max_payload_size: crate::cbor::DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

impl Capabilities {
    pub fn supports(&self, version: u16, encoding: WireEncoding) -> bool {
        self.protocol_versions.contains(&version) && self.encodings.contains(&encoding)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct RequestId(u64);

impl RequestId {
    pub fn new(value: u64) -> Option<Self> {
        (value != 0).then_some(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for RequestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| serde::de::Error::custom("request ID must be nonzero"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub request_id: RequestId,
    pub body: RequestBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "arguments", rename_all = "snake_case")]
pub enum RequestBody {
    Connect(ConnectRequest),
    Ping,
    Publish(PublishRequest),
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    ConsumerCreate(ConsumerCreateRequest),
    ConsumerDelete(ConsumerDeleteRequest),
    Fetch(FetchRequest),
    Ack(AckRequest),
    Nack(NackRequest),
    Extend(ExtendRequest),
    Credit(CreditRequest),
    Group(GroupRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectRequest {
    pub durable_id: Option<String>,
    pub protocol_version: u16,
    pub encoding: WireEncoding,
    pub features: Vec<String>,
    pub verbose: bool,
    pub ack_timeout_ms: Option<u64>,
    pub max_in_flight: Option<usize>,
    pub auth: Option<AuthProof>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthProof {
    pub mechanism: String,
    pub client_id: String,
    pub proof: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishRequest {
    pub message: Message,
    pub durability: PublishDurability,
    pub producer: Option<ProducerIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishDurability {
    Accepted,
    LocalAppend,
    Replicated,
    QuorumCommitted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerIdentity {
    pub producer_id: String,
    pub epoch: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeRequest {
    pub subject: String,
    pub queue: Option<String>,
    pub subscription_id: String,
    pub start: StartPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
    pub subscription_id: String,
    pub max_messages: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerCreateRequest {
    pub name: String,
    pub filter_subject: String,
    pub start: StartPosition,
    pub retry_policy: Option<RetryPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerDeleteRequest {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRequest {
    pub consumer: String,
    pub max_messages: usize,
    pub max_bytes: usize,
    pub max_wait_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckRequest {
    pub consumer: String,
    pub delivery_token: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NackRequest {
    pub consumer: String,
    pub delivery_token: Vec<u8>,
    pub delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtendRequest {
    pub consumer: String,
    pub delivery_token: Vec<u8>,
    pub extension_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreditRequest {
    pub subscription_id: String,
    pub messages: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "arguments", rename_all = "snake_case")]
pub enum GroupRequest {
    Join {
        group: String,
        member: String,
        partitions: u32,
        strategy: String,
        instance_id: Option<String>,
    },
    Heartbeat {
        group: String,
        member: String,
        generation: u64,
    },
    Leave {
        group: String,
        member: String,
        generation: u64,
    },
    Commit {
        group: String,
        member: String,
        generation: u64,
        partition: u32,
        offset: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub request_id: RequestId,
    pub body: ResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "response", content = "value", rename_all = "snake_case")]
pub enum ResponseBody {
    Accepted,
    Published(PublishResult),
    Subscribed { subscription_id: String },
    Unsubscribed { subscription_id: String },
    ConsumerCreated { name: String },
    ConsumerDeleted { name: String },
    Fetched { deliveries: Vec<Delivery> },
    Acknowledged,
    Group(GroupResult),
    Pong,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishResult {
    pub message_id: Option<String>,
    pub durability: PublishDurability,
    pub stream: Option<String>,
    pub partition: Option<u32>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupResult {
    Joined {
        generation: u64,
        partitions: Vec<u32>,
    },
    Heartbeat,
    Left,
    Committed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delivery {
    pub consumer: Option<String>,
    pub subscription_id: Option<String>,
    pub delivery_token: Vec<u8>,
    pub attempt: u32,
    pub lease_deadline_ms: Option<u64>,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowUpdate {
    pub subscription_id: String,
    pub messages: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub request_id: Option<RequestId>,
    pub code: String,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub subject: String,
    pub reply_to: Option<String>,
    pub headers: Vec<Header>,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub message_id: Option<String>,
    pub timestamp_ms: Option<u64>,
    pub position: Option<StreamPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelError(pub String);

impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ModelError {}

impl Message {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.subject.is_empty()
            || self.subject.starts_with('/')
            || self.subject.ends_with('/')
            || self.subject.split('/').any(|segment| {
                segment.is_empty()
                    || segment.chars().any(char::is_whitespace)
                    || segment.contains('*')
            })
        {
            return Err(ModelError(
                "message subject is not a concrete subject".into(),
            ));
        }
        if let Some(reply_to) = &self.reply_to {
            if reply_to.is_empty() || reply_to.chars().any(char::is_whitespace) {
                return Err(ModelError("message reply subject is invalid".into()));
            }
        }
        for header in &self.headers {
            header.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub name: String,
    pub values: Vec<Vec<u8>>,
}

impl Header {
    pub fn single(name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            values: vec![value.into()],
        }
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        if self.name.is_empty()
            || self.name.contains(':')
            || self.name.contains('\r')
            || self.name.contains('\n')
            || self.name.chars().any(char::is_whitespace)
            || self.values.is_empty()
        {
            return Err(ModelError(format!("header {:?} is invalid", self.name)));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPosition {
    pub stream: String,
    pub partition: u32,
    pub offset: u64,
    pub partitioning_epoch: Option<u64>,
    pub leader_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    Earliest,
    Latest,
    Committed,
    Offset(u64),
    Timestamp(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: String,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter_percent: u8,
    pub terminal_action: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Frame {
    Request(Request),
    Response(Response),
    Delivery(Delivery),
    WindowUpdate(WindowUpdate),
    Error(Error),
}

impl Frame {
    pub fn validate(&self) -> Result<(), ModelError> {
        match self {
            Self::Request(Request {
                body: RequestBody::Publish(publish),
                ..
            }) => publish.message.validate(),
            Self::Delivery(delivery) => delivery.message.validate(),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_model_round_trips_through_json() {
        let frame = Frame::Request(Request {
            request_id: RequestId::new(42).unwrap(),
            body: RequestBody::Publish(PublishRequest {
                message: Message {
                    subject: "orders/created".into(),
                    reply_to: Some("_MORROW/INBOX/client/1".into()),
                    headers: vec![Header {
                        name: "content-type".into(),
                        values: vec![b"application/json".to_vec()],
                    }],
                    key: Some(b"customer-7".to_vec()),
                    payload: b"{}".to_vec(),
                    message_id: Some("message-1".into()),
                    timestamp_ms: Some(1_234),
                    position: None,
                },
                durability: PublishDurability::QuorumCommitted,
                producer: Some(ProducerIdentity {
                    producer_id: "producer-1".into(),
                    epoch: 2,
                    sequence: 7,
                }),
            }),
        });
        let encoded = serde_json::to_vec(&frame).unwrap();
        let decoded: Frame = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn protocol_version_is_explicit() {
        assert_eq!(PROTOCOL_VERSION, 1);
    }

    #[test]
    fn default_capabilities_advertise_both_wire_codecs() {
        let capabilities = Capabilities::default();
        assert!(capabilities.supports(PROTOCOL_VERSION, WireEncoding::Text));
        assert!(capabilities.supports(PROTOCOL_VERSION, WireEncoding::Cbor));
        assert!(capabilities.protocol_versions.contains(&2));
    }

    #[test]
    fn request_ids_reserve_zero_for_unsolicited_frames() {
        assert!(RequestId::new(0).is_none());
        assert_eq!(RequestId::new(42).unwrap().get(), 42);
        let error = serde_json::from_str::<RequestId>("0").unwrap_err();
        assert!(error.to_string().contains("nonzero"));
    }

    #[test]
    fn message_validation_supports_typed_repeated_headers() {
        let message = Message {
            subject: "orders/created".into(),
            reply_to: None,
            headers: vec![Header {
                name: "trace-id".into(),
                values: vec![b"one".to_vec(), b"two".to_vec()],
            }],
            key: None,
            payload: Vec::new(),
            message_id: None,
            timestamp_ms: None,
            position: None,
        };
        assert!(message.validate().is_ok());
        assert!(
            Header::single("bad header", b"value".to_vec())
                .validate()
                .is_err()
        );
    }
}
