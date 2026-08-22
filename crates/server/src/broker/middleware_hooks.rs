use super::*;

impl Morrow {
    pub(super) async fn run_after_commit_middleware(
        &self,
        publisher_id: u64,
        record: &PublishRecord,
    ) -> Result<()> {
        let outcome = self
            .middleware
            .process(
                MiddlewareStage::AfterCommit,
                MiddlewareMessage {
                    subject: record.subject.clone(),
                    key: record.key.clone(),
                    headers: record
                        .headers
                        .iter()
                        .map(|header| (header.name.clone(), header.value.clone()))
                        .collect(),
                    payload: record.payload.clone(),
                    reply_to: record.reply_to.clone(),
                },
                0,
            )
            .map_err(|err| BrokerError::with_source("after-commit middleware failed", err))?;
        if outcome.decision == MiddlewareDecision::Reject {
            crate::broker_bail!("after-commit middleware rejected committed record");
        }
        for emitted in outcome.emitted {
            Box::pin(self.publish_with_depth(
                publisher_id,
                emitted.subject,
                None,
                Vec::new(),
                None,
                emitted.payload,
                None,
                1,
            ))
            .await?;
        }
        Ok(())
    }
}
