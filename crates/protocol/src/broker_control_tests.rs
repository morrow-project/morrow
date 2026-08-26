use super::broker_control::*;

#[test]
fn control_frames_round_trip_with_checksum() {
    let frame = BrokerControlFrame::MetadataUpdate(MetadataUpdate::new(7, b"delta".to_vec()));
    let encoded = frame.encode().unwrap();
    assert_eq!(BrokerControlFrame::decode(&encoded).unwrap(), frame);
}

#[test]
fn heartbeat_acceptance_carries_resumable_updates() {
    let frame = BrokerControlFrame::HeartbeatAccepted(HeartbeatAccepted {
        controller_revision: 8,
        updates: vec![MetadataUpdate::new(8, b"assignment".to_vec())],
        snapshot_required: false,
    });
    let encoded = frame.encode().unwrap();
    assert_eq!(BrokerControlFrame::decode(&encoded).unwrap(), frame);
}

#[test]
fn tampered_metadata_is_rejected() {
    let frame = BrokerControlFrame::MetadataUpdate(MetadataUpdate::new(7, b"delta".to_vec()));
    let mut encoded = frame.encode().unwrap();
    *encoded.last_mut().unwrap() ^= 1;
    assert!(matches!(
        BrokerControlFrame::decode(&encoded),
        Err(ControlCodecError::ChecksumMismatch)
    ));
}

#[test]
fn accepted_updates_must_be_ordered() {
    let first = MetadataUpdate::new(2, b"b".to_vec());
    let second = MetadataUpdate::new(1, b"a".to_vec());
    let frame = BrokerControlFrame::RegisterAccepted(RegistrationAccepted {
        protocol_version: BROKER_CONTROL_PROTOCOL_VERSION,
        broker_id: 1,
        incarnation: 1,
        session_id: 9,
        controller_revision: 2,
        updates: vec![first, second],
        snapshot_required: false,
    });
    let encoded = frame.encode().unwrap();
    assert!(matches!(
        BrokerControlFrame::decode(&encoded),
        Err(ControlCodecError::ChecksumMismatch)
    ));
}

#[test]
fn accepted_updates_verify_every_checksum() {
    let first = MetadataUpdate::new(1, b"a".to_vec());
    let mut second = MetadataUpdate::new(2, b"b".to_vec());
    second.checksum ^= 1;
    let frame = BrokerControlFrame::RegisterAccepted(RegistrationAccepted {
        protocol_version: BROKER_CONTROL_PROTOCOL_VERSION,
        broker_id: 1,
        incarnation: 1,
        session_id: 9,
        controller_revision: 2,
        updates: vec![first, second],
        snapshot_required: false,
    });
    let encoded = frame.encode().unwrap();
    assert!(matches!(
        BrokerControlFrame::decode(&encoded),
        Err(ControlCodecError::ChecksumMismatch)
    ));
}
