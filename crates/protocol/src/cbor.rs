//! CBOR wire codec for protocol v1.
//!
//! The fixed envelope keeps transport framing independent from the CBOR
//! representation. Message payloads are carried after the CBOR metadata when
//! the frame has one, so arbitrary payload bytes are not interpreted as CBOR.

use crate::model::{Delivery, Frame, PROTOCOL_VERSION, Request, RequestBody};
use std::fmt;

pub const MAGIC: [u8; 4] = *b"MOR1";
pub const HEADER_LEN: usize = 28;
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

const KIND_REQUEST: u8 = 1;
const KIND_RESPONSE: u8 = 2;
const KIND_DELIVERY: u8 = 3;
const KIND_WINDOW_UPDATE: u8 = 4;
const KIND_ERROR: u8 = 5;
const KIND_PING: u8 = 6;
const KIND_PONG: u8 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_frame_size: usize,
    pub max_metadata_size: usize,
    pub max_payload_size: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_metadata_size: DEFAULT_MAX_FRAME_SIZE,
            max_payload_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CborError {
    TooShort,
    InvalidMagic,
    UnsupportedVersion(u16),
    UnknownFrameKind(u8),
    InvalidFlags(u8),
    InvalidLength(&'static str),
    FrameTooLarge,
    MetadataTooLarge,
    PayloadTooLarge,
    Truncated,
    ChecksumMismatch,
    Cbor(String),
}

impl fmt::Display for CborError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort => f.write_str("CBOR frame is shorter than its header"),
            Self::InvalidMagic => f.write_str("invalid Morrow CBOR frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported Morrow protocol version {version}")
            }
            Self::UnknownFrameKind(kind) => write!(f, "unknown Morrow frame kind {kind}"),
            Self::InvalidFlags(flags) => write!(f, "unsupported Morrow frame flags {flags:#x}"),
            Self::InvalidLength(field) => write!(f, "CBOR {field} length does not fit in a frame"),
            Self::FrameTooLarge => f.write_str("CBOR frame exceeds the configured maximum"),
            Self::MetadataTooLarge => f.write_str("CBOR metadata exceeds the configured maximum"),
            Self::PayloadTooLarge => f.write_str("CBOR payload exceeds the configured maximum"),
            Self::Truncated => f.write_str("truncated CBOR frame"),
            Self::ChecksumMismatch => f.write_str("CBOR frame checksum mismatch"),
            Self::Cbor(message) => write!(f, "CBOR metadata error: {message}"),
        }
    }
}

impl std::error::Error for CborError {}

pub fn encode(frame: &Frame) -> Result<Vec<u8>, CborError> {
    encode_with_flags(frame, 0)
}

pub fn encode_with_flags(frame: &Frame, flags: u8) -> Result<Vec<u8>, CborError> {
    if flags != 0 {
        return Err(CborError::InvalidFlags(flags));
    }
    frame
        .validate()
        .map_err(|error| CborError::Cbor(error.to_string()))?;
    let (metadata_frame, payload) = split_payload(frame);
    let mut metadata = Vec::new();
    ciborium::ser::into_writer(&metadata_frame, &mut metadata)
        .map_err(|error| CborError::Cbor(error.to_string()))?;
    let kind = frame_kind(frame)?;
    let request_id = frame_request_id(frame);
    let metadata_len =
        u32::try_from(metadata.len()).map_err(|_| CborError::InvalidLength("metadata"))?;
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| CborError::InvalidLength("payload"))?;
    let total_len = HEADER_LEN
        .checked_add(metadata.len())
        .and_then(|length| length.checked_add(payload.len()))
        .ok_or(CborError::InvalidLength("frame"))?;
    if total_len > DEFAULT_MAX_FRAME_SIZE {
        return Err(CborError::FrameTooLarge);
    }
    let checksum = checksum(&metadata, &payload);
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(&MAGIC);
    encoded.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    encoded.push(kind);
    encoded.push(flags);
    encoded.extend_from_slice(&request_id.to_be_bytes());
    encoded.extend_from_slice(&metadata_len.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&checksum.to_be_bytes());
    encoded.extend_from_slice(&metadata);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub fn decode(encoded: &[u8], limits: DecodeLimits) -> Result<Frame, CborError> {
    if encoded.len() < HEADER_LEN {
        return Err(CborError::TooShort);
    }
    if encoded[..4] != MAGIC {
        return Err(CborError::InvalidMagic);
    }
    let version = u16::from_be_bytes([encoded[4], encoded[5]]);
    if version != PROTOCOL_VERSION {
        return Err(CborError::UnsupportedVersion(version));
    }
    let kind = encoded[6];
    frame_kind_from_byte(kind)?;
    let flags = encoded[7];
    if flags != 0 {
        return Err(CborError::InvalidFlags(flags));
    }
    let request_id = u64::from_be_bytes(encoded[8..16].try_into().expect("fixed header"));
    let metadata_len =
        u32::from_be_bytes(encoded[16..20].try_into().expect("fixed header")) as usize;
    let payload_len =
        u32::from_be_bytes(encoded[20..24].try_into().expect("fixed header")) as usize;
    let expected_checksum = u32::from_be_bytes(encoded[24..28].try_into().expect("fixed header"));
    if metadata_len > limits.max_metadata_size {
        return Err(CborError::MetadataTooLarge);
    }
    if payload_len > limits.max_payload_size {
        return Err(CborError::PayloadTooLarge);
    }
    let total_len = HEADER_LEN
        .checked_add(metadata_len)
        .and_then(|length| length.checked_add(payload_len))
        .ok_or(CborError::FrameTooLarge)?;
    if total_len > limits.max_frame_size {
        return Err(CborError::FrameTooLarge);
    }
    if encoded.len() != total_len {
        return Err(CborError::Truncated);
    }
    let metadata_end = HEADER_LEN + metadata_len;
    let metadata = &encoded[HEADER_LEN..metadata_end];
    let payload = &encoded[metadata_end..];
    if checksum(metadata, payload) != expected_checksum {
        return Err(CborError::ChecksumMismatch);
    }
    let mut frame: Frame =
        ciborium::de::from_reader(metadata).map_err(|error| CborError::Cbor(error.to_string()))?;
    if frame_kind(&frame)? != kind || frame_request_id(&frame) != request_id {
        return Err(CborError::Cbor(
            "CBOR envelope metadata does not match its header".into(),
        ));
    }
    restore_payload(&mut frame, payload)?;
    Ok(frame)
}

fn frame_kind(frame: &Frame) -> Result<u8, CborError> {
    match frame {
        Frame::Request(_) => Ok(KIND_REQUEST),
        Frame::Response(_) => Ok(KIND_RESPONSE),
        Frame::Delivery(_) => Ok(KIND_DELIVERY),
        Frame::WindowUpdate(_) => Ok(KIND_WINDOW_UPDATE),
        Frame::Error(_) => Ok(KIND_ERROR),
        Frame::Ping(_) => Ok(KIND_PING),
        Frame::Pong(_) => Ok(KIND_PONG),
    }
}

fn frame_kind_from_byte(kind: u8) -> Result<(), CborError> {
    match kind {
        KIND_REQUEST | KIND_RESPONSE | KIND_DELIVERY | KIND_WINDOW_UPDATE | KIND_ERROR
        | KIND_PING | KIND_PONG => Ok(()),
        other => Err(CborError::UnknownFrameKind(other)),
    }
}

fn frame_request_id(frame: &Frame) -> u64 {
    match frame {
        Frame::Request(Request { request_id, .. }) => request_id.get(),
        Frame::Response(response) => response.request_id.get(),
        Frame::Error(error) => error.request_id.map_or(0, |request_id| request_id.get()),
        Frame::Delivery(_) | Frame::WindowUpdate(_) | Frame::Ping(_) | Frame::Pong(_) => 0,
    }
}

fn split_payload(frame: &Frame) -> (Frame, Vec<u8>) {
    let mut metadata_frame = frame.clone();
    let payload = match &mut metadata_frame {
        Frame::Request(Request {
            body: RequestBody::Publish(publish),
            ..
        }) => std::mem::take(&mut publish.message.payload),
        Frame::Delivery(Delivery { message, .. }) => std::mem::take(&mut message.payload),
        _ => Vec::new(),
    };
    (metadata_frame, payload)
}

fn restore_payload(frame: &mut Frame, payload: &[u8]) -> Result<(), CborError> {
    match frame {
        Frame::Request(Request {
            body: RequestBody::Publish(publish),
            ..
        }) => {
            publish.message.payload = payload.to_vec();
        }
        Frame::Delivery(delivery) => delivery.message.payload = payload.to_vec(),
        _ if !payload.is_empty() => {
            return Err(CborError::Cbor("frame kind cannot carry a payload".into()));
        }
        _ => {}
    }
    Ok(())
}

fn checksum(metadata: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(metadata);
    hasher.update(payload);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Header, Message, PublishDurability, PublishRequest};

    fn publish_frame(payload: &[u8]) -> Frame {
        Frame::Request(Request {
            request_id: crate::model::RequestId::new(42).unwrap(),
            body: RequestBody::Publish(PublishRequest {
                message: Message {
                    subject: "orders/created".into(),
                    reply_to: None,
                    headers: vec![Header {
                        name: "content-type".into(),
                        values: vec![b"application/octet-stream".to_vec()],
                    }],
                    key: None,
                    payload: payload.to_vec(),
                    message_id: Some("message-1".into()),
                    timestamp_ms: Some(1234),
                    position: None,
                },
                durability: PublishDurability::QuorumCommitted,
                producer: None,
            }),
        })
    }

    #[test]
    fn round_trips_structured_metadata_and_opaque_payload() {
        let frame = publish_frame(&[0, 1, 2, 255]);
        let encoded = encode(&frame).unwrap();
        assert_eq!(&encoded[..4], b"MOR1");
        assert_eq!(
            encoded.len(),
            HEADER_LEN + u32::from_be_bytes(encoded[16..20].try_into().unwrap()) as usize + 4
        );
        assert_eq!(decode(&encoded, DecodeLimits::default()).unwrap(), frame);
    }

    #[test]
    fn rejects_truncated_and_corrupt_frames() {
        let encoded = encode(&publish_frame(b"hello")).unwrap();
        assert_eq!(
            decode(&encoded[..encoded.len() - 1], DecodeLimits::default()),
            Err(CborError::Truncated)
        );
        let mut corrupt = encoded;
        *corrupt.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode(&corrupt, DecodeLimits::default()),
            Err(CborError::ChecksumMismatch)
        );
    }

    #[test]
    fn enforces_decode_limits_before_allocating_metadata() {
        let encoded = encode(&publish_frame(b"hello")).unwrap();
        let limit = DecodeLimits {
            max_frame_size: encoded.len() - 1,
            ..DecodeLimits::default()
        };
        assert_eq!(decode(&encoded, limit), Err(CborError::FrameTooLarge));
    }
}
