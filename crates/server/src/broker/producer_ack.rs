use super::*;

impl Morrow {
    pub(super) async fn send_producer_ack(
        &self,
        publisher_id: u64,
        ack: &protocol::ProducerAckRequest,
        retained: bool,
        seq: Option<u64>,
    ) -> Result<()> {
        let contract = self
            .connections
            .lock()
            .await
            .clients
            .get(&publisher_id)
            .and_then(|client| client.ack_contract_version);
        self.send_to(
            publisher_id,
            protocol::producer_ack_with_contract(&ack.msg_id, ack.level, retained, seq, contract),
        )
        .await
    }

    pub(super) async fn send_positioned_producer_ack(
        &self,
        publisher_id: u64,
        ack: &protocol::ProducerAckRequest,
        record: &PublishRecord,
    ) -> Result<()> {
        let contract = self
            .connections
            .lock()
            .await
            .clients
            .get(&publisher_id)
            .and_then(|client| client.ack_contract_version);
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
            protocol::producer_ack_with_position_and_contract(
                &ack.msg_id,
                ack.level,
                true,
                Some(record.seq),
                position,
                contract,
            ),
        )
        .await
    }
}
