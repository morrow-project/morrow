//! Shared protocol-v1 codec conformance corpus.

use crate::model::{
    Frame, Header, Message, PublishDurability, PublishRequest, Request, RequestBody, RequestId,
};

pub fn corpus() -> Vec<Frame> {
    vec![
        Frame::Request(Request {
            request_id: RequestId::new(1).unwrap(),
            body: RequestBody::Ping,
        }),
        Frame::Request(Request {
            request_id: RequestId::new(2).unwrap(),
            body: RequestBody::Publish(PublishRequest {
                message: Message {
                    subject: "orders/created".into(),
                    reply_to: Some("_MORROW/INBOX/client/1".into()),
                    headers: vec![Header::single("content-type", b"application/json".to_vec())],
                    key: Some(b"customer-7".to_vec()),
                    payload: br#"{"id":7}"#.to_vec(),
                    message_id: Some("message-1".into()),
                    timestamp_ms: Some(1234),
                    position: None,
                },
                durability: PublishDurability::QuorumCommitted,
                producer: None,
            }),
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_and_cbor_codecs_round_trip_the_same_corpus() {
        for frame in corpus() {
            let text = crate::text::encode(&frame).unwrap();
            let cbor = crate::cbor::encode(&frame).unwrap();
            assert_eq!(
                crate::text::decode(&text, crate::text::DEFAULT_MAX_LINE_SIZE).unwrap(),
                frame
            );
            assert_eq!(
                crate::cbor::decode(&cbor, Default::default()).unwrap(),
                frame
            );
        }
    }

    #[test]
    #[ignore = "manual codec throughput benchmark"]
    fn benchmark_codec_round_trips() {
        let corpus = corpus();
        let start = std::time::Instant::now();
        let mut bytes = 0usize;
        for _ in 0..10_000 {
            for frame in &corpus {
                let encoded = crate::cbor::encode(frame).unwrap();
                bytes += encoded.len();
                let _ = crate::cbor::decode(&encoded, Default::default()).unwrap();
            }
        }
        eprintln!(
            "CBOR conformance benchmark: {} bytes in {:?}",
            bytes,
            start.elapsed()
        );
    }
}
