use super::*;

impl Morrow {
    pub(super) async fn redelivery_loop(self) {
        loop {
            let deadline = self.inner.lock().await.next_lease_deadline();
            let wait = deadline.map_or(
                Duration::from_millis(RETENTION_TICK_INTERVAL_MS),
                |deadline| {
                    Duration::from_millis(
                        deadline
                            .saturating_sub(self.hooks.clock.now_ms())
                            .min(RETENTION_TICK_INTERVAL_MS),
                    )
                },
            );
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.redelivery_notify.notified() => continue,
            }
            if let Err(err) = self.expire_and_redeliver().await {
                error!(error = ?err, "redelivery error");
            }
        }
    }

    pub(super) async fn expire_and_redeliver(&self) -> Result<()> {
        let now = self.hooks.clock.now_ms();
        if let Some(cluster) = self.cluster_runtime().await {
            cluster.enforce_retention(now)?;
            self.sync_cluster_deltas(&cluster).await?;
        }
        let expired = {
            let _storage_operation = self.storage_gate.read().await;
            let mut inner = self.inner.lock().await;
            inner.enforce_stream_retention(&self.partition_logs, &self.config.streams, now)?;
            inner.expire_due_leases(now, MAX_EXPIRED_LEASES_PER_TICK)
        };
        if expired > 0 {
            self.metrics
                .redeliveries_total
                .fetch_add(expired as u64, Ordering::Relaxed);
            self.pull_waiters.notify_all();
        }
        self.deliver_pending().await
    }
}
