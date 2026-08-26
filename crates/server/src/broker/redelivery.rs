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
                self.storage_failure.store(true, Ordering::Relaxed);
                error!(error = ?err, "redelivery error");
            }
        }
    }

    pub(super) async fn expire_and_redeliver(&self) -> Result<()> {
        let now = self.hooks.clock.now_ms();
        self.groups.lock().await.values_mut().for_each(|group| {
            group.expire(now);
        });
        if let Some(cluster) = self.cluster_runtime().await {
            crate::broker_ensure!(
                self.reserve_retention_work().await,
                "retention work budget exhausted"
            );
            let result = cluster.enforce_retention(now);
            self.release_retention_work().await;
            result?;
            self.sync_cluster_deltas(&cluster).await?;
        }
        let (expired, dead_letter_writes, released) = {
            let _storage_operation = self.storage_gate.read().await;
            let mut inner = self.inner.lock().await;
            inner.activate_due_scheduled(now, MAX_EXPIRED_LEASES_PER_TICK);
            let tenants = self.config.tenant_quotas.keys().cloned().collect();
            crate::broker_ensure!(
                self.reserve_retention_work().await,
                "retention work budget exhausted"
            );
            let released = inner.enforce_stream_retention(
                &self.partition_logs,
                &self.config.streams,
                now,
                &tenants,
            );
            self.release_retention_work().await;
            let released = released?;
            let before = inner.dead_letters.len();
            let expired = inner.expire_due_leases(now, MAX_EXPIRED_LEASES_PER_TICK)?;
            (
                expired,
                inner.dead_letters.len().saturating_sub(before),
                released,
            )
        };
        for (tenant, bytes) in released {
            self.tenant_quotas.release(
                &tenant,
                crate::quota::TenantQuotaUsage {
                    disk_bytes: bytes,
                    ..Default::default()
                },
            );
        }
        self.metrics
            .dead_letter_writes_total
            .fetch_add(dead_letter_writes as u64, Ordering::Relaxed);
        if expired > 0 {
            self.metrics
                .redeliveries_total
                .fetch_add(expired as u64, Ordering::Relaxed);
            self.pull_waiters.notify_all();
        }
        self.deliver_pending().await
    }
}
