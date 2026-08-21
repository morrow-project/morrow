use super::*;

impl Broker {
    pub(super) async fn send_producer_ack(
        &self,
        publisher_id: u64,
        ack: &protocol::ProducerAckRequest,
        retained: bool,
        seq: Option<u64>,
    ) -> Result<()> {
        self.send_to(
            publisher_id,
            protocol::producer_ack(&ack.msg_id, ack.level, retained, seq),
        )
        .await
    }

    pub(super) async fn send_positioned_producer_ack(
        &self,
        publisher_id: u64,
        ack: &protocol::ProducerAckRequest,
        record: &PublishRecord,
    ) -> Result<()> {
        let position = match (record.stream.as_deref(), record.partition, record.offset) {
            (Some(stream), Some(partition), Some(offset)) => Some((
                stream,
                partition,
                offset,
                record.partitioning_epoch,
                record.leader_epoch,
            )),
            _ => None,
        };
        self.send_to(
            publisher_id,
            protocol::producer_ack_with_position(
                &ack.msg_id,
                ack.level,
                true,
                Some(record.seq),
                position,
            ),
        )
        .await
    }
}
