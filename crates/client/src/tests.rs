use tokio::io::AsyncWriteExt;

use super::*;

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
        })
    );
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
