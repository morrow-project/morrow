use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use super::*;

#[tokio::test]
async fn keyed_qos_publish_encodes_partition_key_and_waits_for_commit_ack() {
    let (client_io, server_io) = tokio::io::duplex(1024);
    let mut client = Client {
        stream: BufReader::new(Box::new(client_io)),
        max_payload: 1024,
        inbox_prefix: "_INBOX.test".to_string(),
        inbox_counter: 0,
        durable: true,
        push_credit_messages: 1,
    };
    let server = tokio::spawn(async move {
        let mut server = tokio::io::BufReader::new(server_io);
        let mut line = String::new();
        server.read_line(&mut line).await.unwrap();
        let parts = line.split_whitespace().collect::<Vec<_>>();
        assert_eq!(parts[0], "HPUB");
        assert_eq!(parts[1], "orders.created");
        let total = parts[3].parse::<usize>().unwrap();
        let mut body = vec![0; total + 2];
        server.read_exact(&mut body).await.unwrap();
        let encoded = String::from_utf8(body).unwrap();
        assert!(encoded.contains("Broker-Key: customer-7\r\n"));
        assert!(encoded.ends_with("\r\n\r\nhello\r\n"));
        server
            .get_mut()
            .write_all(b"P-ACK msg-1 2 OK true 9 orders 0 4 1 0\r\n")
            .await
            .unwrap();
    });

    let ack = client
        .publish_with_qos_and_key(
            "orders.created",
            None,
            b"hello",
            protocol::AckLevel::HighDurability,
            "msg-1",
            Some("customer-7"),
        )
        .await
        .unwrap();
    assert_eq!(ack.offset, Some(4));
    server.await.unwrap();
}

#[tokio::test]
async fn parses_hmsg_with_reply_and_broker_ack_header() {
    let (mut writer, reader) = tokio::io::duplex(128);
    writer
        .write_all(b"NATS/1.0\r\nBroker-Ack: _BROKER.ACK.consumer.1.2\r\n\r\nhello\r\n")
        .await
        .unwrap();
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);

    let frame = parse_frame(
        &mut reader,
        "HMSG service.echo sid1 _INBOX.client.1 50 55",
        1024,
    )
    .await
    .unwrap()
    .unwrap();

    let ServerFrame::Message(message) = frame else {
        panic!("expected HMSG to parse as Message");
    };
    assert_eq!(message.subject, "service.echo");
    assert_eq!(message.sid, "sid1");
    assert_eq!(message.reply_to.as_deref(), Some("_INBOX.client.1"));
    assert_eq!(
        message.ack_subject.as_deref(),
        Some("_BROKER.ACK.consumer.1.2")
    );
    assert_eq!(message.payload, b"hello");
}

#[tokio::test]
async fn parses_msg_ack_reply_as_ack_subject() {
    let (mut writer, reader) = tokio::io::duplex(64);
    writer.write_all(b"hello\r\n").await.unwrap();
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);

    let frame = parse_frame(
        &mut reader,
        "MSG orders.created sid1 _BROKER.ACK.consumer.1.2 5",
        1024,
    )
    .await
    .unwrap()
    .unwrap();

    let ServerFrame::Message(message) = frame else {
        panic!("expected MSG to parse as Message");
    };
    assert!(message.reply_to.is_none());
    assert_eq!(
        message.ack_subject.as_deref(),
        Some("_BROKER.ACK.consumer.1.2")
    );
}

#[tokio::test]
async fn parses_producer_ack_frame() {
    let (_writer, reader) = tokio::io::duplex(64);
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);

    let frame = parse_frame(&mut reader, "P-ACK msg-1 2 OK true 42", 1024)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        frame,
        ServerFrame::ProducerAck(ProducerAck {
            msg_id: "msg-1".into(),
            level: protocol::AckLevel::HighDurability,
            retained: true,
            seq: Some(42),
            stream: None,
            partition: None,
            offset: None,
            partitioning_epoch: None,
            leader_epoch: None,
        })
    );
}

#[tokio::test]
async fn parses_partition_position_from_producer_ack() {
    let (_writer, reader) = tokio::io::duplex(64);
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);
    let frame = parse_frame(&mut reader, "P-ACK msg-1 1 OK true 9 orders 2 41 7 3", 1024)
        .await
        .unwrap()
        .unwrap();
    let ServerFrame::ProducerAck(ack) = frame else {
        panic!("expected producer ack");
    };
    assert_eq!(ack.stream.as_deref(), Some("orders"));
    assert_eq!(ack.partition, Some(2));
    assert_eq!(ack.offset, Some(41));
    assert_eq!(ack.partitioning_epoch, Some(7));
    assert_eq!(ack.leader_epoch, Some(3));
}

#[tokio::test]
async fn rejects_malformed_hmsg_lengths() {
    let (_writer, reader) = tokio::io::duplex(64);
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);

    let err = parse_frame(&mut reader, "HMSG service.echo sid1 nope 5", 1024)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("headers length"));

    let err = parse_frame(&mut reader, "HMSG service.echo sid1 6 5", 1024)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exceeds total"));

    let err = parse_frame(&mut reader, "HMSG service.echo sid1 1 2048", 1024)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exceeds max payload"));
}

#[tokio::test]
async fn parses_pull_batch_and_durable_message_frames() {
    let (_writer, reader) = tokio::io::duplex(64);
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);
    assert_eq!(
        parse_frame(&mut reader, "C-OK CREATE worker", 1024)
            .await
            .unwrap()
            .unwrap(),
        ServerFrame::ConsumerOk {
            operation: "CREATE".into(),
            name: "worker".into(),
        }
    );
    assert_eq!(
        parse_frame(&mut reader, "D-OK ACK worker 7 9", 1024)
            .await
            .unwrap()
            .unwrap(),
        ServerFrame::DeliveryControlOk {
            operation: "ACK".into(),
            name: "worker".into(),
            seq: 7,
            delivery_id: 9,
        }
    );
    assert_eq!(
        parse_frame(&mut reader, "BATCH worker 1 5", 1024)
            .await
            .unwrap()
            .unwrap(),
        ServerFrame::Batch {
            name: "worker".into(),
            messages: 1,
            bytes: 5,
        }
    );

    let (mut writer, reader) = tokio::io::duplex(64);
    writer
        .write_all(b"NATS/1.0\r\nTrace-Id: abc\r\n\r\nhello\r\n")
        .await
        .unwrap();
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);
    let frame = parse_frame(
        &mut reader,
        "DMSG worker orders.created _INBOX.reply orders 2 41 637573746f6d65722d37 1234 3 900 7 9 27 32",
        1024,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        frame,
        ServerFrame::DurableMessage(DurableMessage {
            consumer: "worker".into(),
            subject: "orders.created".into(),
            reply_to: Some("_INBOX.reply".into()),
            headers: vec![("Trace-Id".into(), "abc".into())],
            stream: "orders".into(),
            partition: 2,
            offset: 41,
            key: Some(b"customer-7".to_vec()),
            timestamp_ms: 1234,
            attempt: 3,
            lease_deadline_ms: 900,
            seq: 7,
            delivery_id: 9,
            payload: b"hello".to_vec(),
        })
    );
}

#[tokio::test]
async fn rejects_oversized_pull_delivery_before_allocating_body() {
    let (_writer, reader) = tokio::io::duplex(64);
    let mut reader = BufReader::new(Box::new(reader) as Box<dyn ClientStream>);
    let error = parse_frame(
        &mut reader,
        "DMSG worker orders.created - orders 0 0 - 0 1 900 7 9 12 2048",
        1024,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("exceeds max payload"));
}
