//! Human-readable protocol-v1 frontend for the semantic model.
//!
//! The legacy command grammar remains available through `protocol.rs`. This
//! frontend is a lossless debug representation of the same model used by the
//! CBOR codec and is intentionally line-oriented for use with simple TCP
//! tools.

use crate::model::Frame;
use std::fmt;

pub const PREFIX: &str = "FRAME ";
pub const DEFAULT_MAX_LINE_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextError {
    MissingLineEnding,
    LineTooLong,
    InvalidUtf8,
    InvalidPrefix,
    Json(String),
    Model(String),
}

impl fmt::Display for TextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingLineEnding => f.write_str("text frame must end with CRLF or LF"),
            Self::LineTooLong => f.write_str("text frame exceeds the configured line limit"),
            Self::InvalidUtf8 => f.write_str("text frame is not UTF-8"),
            Self::InvalidPrefix => f.write_str("text frame must start with FRAME"),
            Self::Json(message) => write!(f, "text frame JSON error: {message}"),
            Self::Model(message) => write!(f, "text frame model error: {message}"),
        }
    }
}

impl std::error::Error for TextError {}

pub fn encode(frame: &Frame) -> Result<Vec<u8>, TextError> {
    frame
        .validate()
        .map_err(|error| TextError::Model(error.to_string()))?;
    let json = serde_json::to_string(frame).map_err(|error| TextError::Json(error.to_string()))?;
    let mut output = String::with_capacity(PREFIX.len() + json.len() + 2);
    output.push_str(PREFIX);
    output.push_str(&json);
    output.push_str("\r\n");
    Ok(output.into_bytes())
}

pub fn decode(line: &[u8], max_line_size: usize) -> Result<Frame, TextError> {
    if line.len() > max_line_size {
        return Err(TextError::LineTooLong);
    }
    let line = if let Some(line) = line.strip_suffix(b"\r\n") {
        line
    } else if let Some(line) = line.strip_suffix(b"\n") {
        line
    } else {
        return Err(TextError::MissingLineEnding);
    };
    let line = std::str::from_utf8(line).map_err(|_| TextError::InvalidUtf8)?;
    let json = line.strip_prefix(PREFIX).ok_or(TextError::InvalidPrefix)?;
    let frame: Frame =
        serde_json::from_str(json).map_err(|error| TextError::Json(error.to_string()))?;
    frame
        .validate()
        .map_err(|error| TextError::Model(error.to_string()))?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Message, PublishDurability, PublishRequest, Request, RequestBody, RequestId,
    };

    fn frame() -> Frame {
        Frame::Request(Request {
            request_id: RequestId::new(9).unwrap(),
            body: RequestBody::Publish(PublishRequest {
                message: Message {
                    subject: "orders/created".into(),
                    reply_to: None,
                    headers: Vec::new(),
                    key: None,
                    payload: b"hello".to_vec(),
                    message_id: Some("m-1".into()),
                    timestamp_ms: None,
                    position: None,
                },
                durability: PublishDurability::Accepted,
                producer: None,
            }),
        })
    }

    #[test]
    fn round_trips_the_semantic_model_as_a_debug_frame() {
        let encoded = encode(&frame()).unwrap();
        assert!(encoded.starts_with(b"FRAME {"));
        assert_eq!(decode(&encoded, DEFAULT_MAX_LINE_SIZE).unwrap(), frame());
    }

    #[test]
    fn rejects_bad_endings_prefixes_and_limits() {
        let encoded = encode(&frame()).unwrap();
        assert_eq!(
            decode(&encoded[..encoded.len() - 2], 1024),
            Err(TextError::MissingLineEnding)
        );
        assert_eq!(decode(b"NOPE {}\r\n", 1024), Err(TextError::InvalidPrefix));
        assert_eq!(
            decode(&encoded, encoded.len() - 1),
            Err(TextError::LineTooLong)
        );
    }
}
