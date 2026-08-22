use tokio::io::BufReader;

use super::*;
use crate::{
    batch, consumer_ok, control_ok, durable_message, durable_message_encoded_len, hmsg, msg,
    parse_ack_subject, producer_ack, producer_ack_with_position,
};

#[tokio::test]
async fn parses_pub_with_payload() {
    let mut reader = BufReader::new(&b"PUB orders/created 5\r\nhello\r\n"[..]);
    let command = read_command(&mut reader, 1024, 8192)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command,
        Command::Pub {
            subject: "orders/created".into(),
            reply_to: None,
            headers: vec![],
            key: None,
            payload: b"hello".to_vec(),
            ack: None,
        }
    );
}

#[tokio::test]
async fn parses_hpub_with_qos_headers() {
    let line = hpub("orders/created", None, "1", Some("msg-1"), b"hello");
    let mut reader = BufReader::new(line.as_bytes());
    let command = read_command(&mut reader, 1024, 8192)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command,
        Command::Pub {
            subject: "orders/created".into(),
            reply_to: None,
            headers: vec![],
            key: None,
            payload: b"hello".to_vec(),
            ack: Some(ProducerAckRequest {
                level: AckLevel::Durable,
                msg_id: "msg-1".into(),
            }),
        }
    );
}

#[tokio::test]
async fn parses_hpub_with_reply_to_and_qos_headers() {
    let line = hpub(
        "service/echo",
        Some("_MORROW/INBOX/client/1"),
        "3",
        Some("msg-3"),
        b"hello",
    );
    let mut reader = BufReader::new(line.as_bytes());
    let command = read_command(&mut reader, 1024, 8192)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command,
        Command::Pub {
            subject: "service/echo".into(),
            reply_to: Some("_MORROW/INBOX/client/1".into()),
            headers: vec![],
            key: None,
            payload: b"hello".to_vec(),
            ack: Some(ProducerAckRequest {
                level: AckLevel::ClusterDurable,
                msg_id: "msg-3".into(),
            }),
        }
    );
}

#[tokio::test]
async fn parses_application_headers_and_partition_key() {
    let headers = "MORROW/1.0\r\nMorrow-Key: customer-7\r\nTrace-Id: trace-1\r\n\r\n";
    let frame = format!(
        "HPUB orders/created {} {}\r\n{headers}hello\r\n",
        headers.len(),
        headers.len() + 5
    );
    let mut reader = BufReader::new(frame.as_bytes());
    let command = read_command(&mut reader, 1024, 8192)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command,
        Command::Pub {
            subject: "orders/created".into(),
            reply_to: None,
            headers: vec![("Trace-Id".into(), "trace-1".into())],
            key: Some(b"customer-7".to_vec()),
            payload: b"hello".to_vec(),
            ack: None,
        }
    );
}

#[tokio::test]
async fn parses_all_qos_levels() {
    for (raw, level) in [
        ("0", AckLevel::Accepted),
        ("1", AckLevel::Durable),
        ("2", AckLevel::HighDurability),
        ("3", AckLevel::ClusterDurable),
    ] {
        let line = hpub("orders/created", None, raw, Some("msg"), b"hello");
        let mut reader = BufReader::new(line.as_bytes());
        let command = read_command(&mut reader, 1024, 8192)
            .await
            .unwrap()
            .unwrap();
        let Command::Pub { ack: Some(ack), .. } = command else {
            panic!("expected QoS ack request");
        };
        assert_eq!(ack.level, level);
    }
}

#[tokio::test]
async fn rejects_hpub_qos_without_msg_id() {
    let line = hpub("orders/created", None, "1", None, b"hello");
    let mut reader = BufReader::new(line.as_bytes());
    let err = read_command(&mut reader, 1024, 8192).await.unwrap_err();
    assert!(err.0.contains("Morrow-Msg-Id"));
}

#[tokio::test]
async fn rejects_invalid_hpub_qos_and_lengths() {
    let line = hpub("orders/created", None, "4", Some("msg-1"), b"hello");
    let mut bad_qos = BufReader::new(line.as_bytes());
    let err = read_command(&mut bad_qos, 1024, 8192).await.unwrap_err();
    assert!(err.0.contains("Morrow-QoS"));

    let mut bad_len = BufReader::new(&b"HPUB orders/created 6 5\r\nMORROW/1.0\r\n\r\n"[..]);
    let err = read_command(&mut bad_len, 1024, 8192).await.unwrap_err();
    assert!(err.0.contains("headers length exceeds"));
}

#[tokio::test]
async fn rejects_non_morrow_header_preambles() {
    let mut reader = BufReader::new(&b"HPUB orders/created 13 13\r\nOTHER/1.0\r\n\r\n\r\n"[..]);
    let err = read_command(&mut reader, 1024, 8192).await.unwrap_err();
    assert!(err.0.contains("MORROW/1.0"));
}

fn hpub(
    subject: &str,
    reply_to: Option<&str>,
    qos: &str,
    msg_id: Option<&str>,
    payload: &[u8],
) -> String {
    let mut headers = format!("MORROW/1.0\r\nMorrow-QoS: {qos}\r\n");
    if let Some(msg_id) = msg_id {
        headers.push_str(&format!("Morrow-Msg-Id: {msg_id}\r\n"));
    }
    headers.push_str("\r\n");
    let total_len = headers.len() + payload.len();
    let payload = std::str::from_utf8(payload).unwrap();
    match reply_to {
        Some(reply_to) => {
            format!(
                "HPUB {subject} {reply_to} {} {total_len}\r\n{headers}{payload}\r\n",
                headers.len()
            )
        }
        None => format!(
            "HPUB {subject} {} {total_len}\r\n{headers}{payload}\r\n",
            headers.len()
        ),
    }
}

#[tokio::test]
async fn parses_connect_durable_metadata() {
    let mut reader = BufReader::new(
        &b"CONN {\"verbose\":true,\"durable_id\":\"client1\",\"ack_timeout_ms\":25,\"max_in_flight\":7,\"protocol_version\":2}\r\n"[..],
    );
    let command = read_command(&mut reader, 1024, 8192)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command,
        Command::Connect {
            verbose: true,
            durable_id: Some("client1".into()),
            ack_timeout_ms: Some(25),
            max_in_flight: Some(7),
            protocol_version: Some(2),
            auth: None,
        }
    );
}

#[tokio::test]
async fn parses_connect_client_auth() {
    let mut reader =
        BufReader::new(&b"CONN {\"client_id\":\"client1\",\"signature\":\"1234\"}\r\n"[..]);
    let command = read_command(&mut reader, 1024, 8192)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command,
        Command::Connect {
            verbose: false,
            durable_id: None,
            ack_timeout_ms: None,
            max_in_flight: None,
            protocol_version: None,
            auth: Some(ConnectAuth {
                client_id: "client1".into(),
                signature: "1234".into(),
            }),
        }
    );
}

#[tokio::test]
async fn rejects_malformed_connect_field_types() {
    for (payload, expected) in [
        (r#"{"verbose":"true"}"#, "verbose"),
        (r#"{"durable_id":7}"#, "durable_id"),
        (r#"{"ack_timeout_ms":"25"}"#, "ack_timeout_ms"),
        (r#"{"max_in_flight":"7"}"#, "max_in_flight"),
        (r#"{"protocol_version":"2"}"#, "protocol_version"),
        (r#"{"client_id":7,"signature":"1234"}"#, "client_id"),
        (r#"{"client_id":"client1","signature":1234}"#, "signature"),
    ] {
        let line = format!("CONN {payload}\r\n");
        let mut reader = BufReader::new(line.as_bytes());
        let err = read_command(&mut reader, 1024, 8192).await.unwrap_err();
        assert!(
            err.0.contains(expected),
            "expected {expected:?} in error {err:?}"
        );
    }
}

#[tokio::test]
async fn parses_pull_consumer_commands() {
    let cases = [
        (
            "CONSUMER CREATE worker orders/* @earliest\r\n",
            Command::ConsumerCreate {
                name: "worker".into(),
                filter_subject: "orders/*".into(),
                start: StartPosition::Earliest,
            },
        ),
        (
            "CONSUMER DELETE worker\r\n",
            Command::ConsumerDelete {
                name: "worker".into(),
            },
        ),
        (
            "FETCH worker 10 4096 25\r\n",
            Command::Fetch {
                name: "worker".into(),
                max_messages: 10,
                max_bytes: 4096,
                max_wait_ms: 25,
            },
        ),
        (
            "ACK worker 7 9\r\n",
            Command::Ack {
                name: "worker".into(),
                seq: 7,
                delivery_id: 9,
            },
        ),
        (
            "NACK worker 7 9 50\r\n",
            Command::Nack {
                name: "worker".into(),
                seq: 7,
                delivery_id: 9,
                delay_ms: 50,
            },
        ),
        (
            "EXTEND worker 7 9 100\r\n",
            Command::Extend {
                name: "worker".into(),
                seq: 7,
                delivery_id: 9,
                extension_ms: 100,
            },
        ),
        (
            "CREDIT sid1 5 8192\r\n",
            Command::Credit {
                sid: "sid1".into(),
                messages: 5,
                bytes: 8192,
            },
        ),
    ];
    for (wire, expected) in cases {
        let mut reader = BufReader::new(wire.as_bytes());
        assert_eq!(
            read_command(&mut reader, 1024, 8192)
                .await
                .unwrap()
                .unwrap(),
            expected
        );
    }
}

#[test]
fn encodes_pull_delivery_frames() {
    assert_eq!(consumer_ok("CREATE", "worker"), b"C-OK CREATE worker\r\n");
    assert_eq!(
        control_ok("ACK", "worker", 7, 9),
        b"D-OK ACK worker 7 9\r\n"
    );
    assert_eq!(batch("worker", 1, 5), b"BATCH worker 1 5\r\n");
    assert_eq!(
        durable_message(
            "worker",
            "orders/created",
            None,
            &[],
            "orders",
            2,
            41,
            Some(b"customer-7"),
            1234,
            3,
            900,
            7,
            9,
            b"hello"
        ),
        b"DDELIVER worker orders/created - orders 2 41 637573746f6d65722d37 1234 3 900 7 9 14 19\r\nMORROW/1.0\r\n\r\nhello\r\n"
    );
}

#[test]
fn calculates_pull_delivery_frame_length_without_encoding() {
    let headers = [("x-region", "ap-south-1"), ("x-attempt", "17")];
    let payload = b"hello";
    let encoded = durable_message(
        "worker",
        "orders/created",
        Some("reply.subject"),
        &headers,
        "orders",
        u32::MAX,
        u64::MAX,
        Some(&[0, 1, 0xfe, 0xff]),
        u64::MAX,
        u32::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        payload,
    );
    let length = durable_message_encoded_len(
        "worker",
        "orders/created",
        Some("reply.subject"),
        headers.iter().copied(),
        "orders",
        u32::MAX,
        u64::MAX,
        Some(&[0, 1, 0xfe, 0xff]),
        u64::MAX,
        u32::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        payload,
    );

    assert_eq!(length, encoded.len());
}

#[tokio::test]
async fn parses_sub_variants() {
    let mut reader = BufReader::new(&b"SUB orders/* workers 7\r\n"[..]);
    let command = read_command(&mut reader, 1024, 8192)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command,
        Command::Sub {
            subject: "orders/*".into(),
            queue: Some("workers".into()),
            sid: "7".into(),
            start: StartPosition::Latest,
        }
    );

    let mut earliest = BufReader::new(&b"SUB orders/* 8 @earliest\r\n"[..]);
    assert_eq!(
        read_command(&mut earliest, 1024, 8192)
            .await
            .unwrap()
            .unwrap(),
        Command::Sub {
            subject: "orders/*".into(),
            queue: None,
            sid: "8".into(),
            start: StartPosition::Earliest,
        }
    );

    let mut exact = BufReader::new(&b"SUB orders/* workers 9 @offset:42\r\n"[..]);
    let Command::Sub { start, .. } = read_command(&mut exact, 1024, 8192).await.unwrap().unwrap()
    else {
        panic!("expected SUB");
    };
    assert_eq!(start, StartPosition::Offset(42));
}

#[tokio::test]
async fn rejects_oversized_payload() {
    let mut reader = BufReader::new(&b"PUB orders/created 5\r\nhello\r\n"[..]);
    let err = read_command(&mut reader, 4, 8192).await.unwrap_err();
    assert!(err.0.contains("exceeds max payload"));
}

#[tokio::test]
async fn rejects_oversized_control_line_before_payload_read() {
    let mut reader = BufReader::new(&b"CONN {\"durable_id\":\"client1\"}\r\n"[..]);
    let err = read_command(&mut reader, 1024, 16).await.unwrap_err();
    assert!(err.0.contains("max_control_line"));
}

#[tokio::test]
async fn rejects_fetch_values_larger_than_platform_usize() {
    let too_large = (usize::MAX as u128) + 1;
    for wire in [
        format!("FETCH worker {too_large} 1 0\r\n"),
        format!("FETCH worker 1 {too_large} 0\r\n"),
    ] {
        let mut reader = BufReader::new(wire.as_bytes());
        let err = read_command(&mut reader, 1024, 8192).await.unwrap_err();
        assert!(err.0.contains("too large") || err.0.contains("integer"));
    }
}

#[test]
fn encodes_msg_frames() {
    assert_eq!(
        msg("orders/created", "1", None, b"ok"),
        b"DELIVER orders/created 1 2\r\nok\r\n"
    );
}

#[test]
fn encodes_hmsg_frames() {
    assert_eq!(
        hmsg(
            "orders/created",
            "1",
            Some("_MORROW/INBOX/client/1"),
            &[("Morrow-Ack", "_MORROW/ACK/consumer/1/2")],
            b"ok"
        ),
        b"HDELIVER orders/created 1 _MORROW/INBOX/client/1 52 54\r\nMORROW/1.0\r\nMorrow-Ack: _MORROW/ACK/consumer/1/2\r\n\r\nok\r\n"
    );
}

#[test]
fn encodes_producer_ack_frames() {
    assert_eq!(
        producer_ack("msg-1", AckLevel::Durable, true, Some(42)),
        b"P-ACK msg-1 1 OK true 42\r\n"
    );
    assert_eq!(
        producer_ack("msg-2", AckLevel::Accepted, false, None),
        b"P-ACK msg-2 0 OK false -\r\n"
    );
    assert_eq!(
        producer_ack_with_position(
            "msg-3",
            AckLevel::Durable,
            true,
            Some(9),
            Some(("orders", 2, 41, 7, 3)),
        ),
        b"P-ACK msg-3 1 OK true 9 orders 2 41 7 3\r\n"
    );
}

#[test]
fn parses_ack_subjects() {
    assert_eq!(
        parse_ack_subject("_MORROW/ACK/consumer1/42/9"),
        Some(AckSubject {
            consumer_id: "consumer1".into(),
            seq: 42,
            delivery_id: 9,
        })
    );
    assert!(parse_ack_subject("_MORROW/ACK/consumer1/nope/9").is_none());
}
