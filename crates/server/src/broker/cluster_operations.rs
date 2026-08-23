use super::*;

impl Morrow {
    pub(super) async fn start_cluster(&self) -> Result<()> {
        let Some(cluster_config) = &self.config.cluster else {
            return Ok(());
        };
        let runtime = RaftRuntime::open(
            cluster_config,
            self.tls_acceptor.is_some(),
            &self.config.streams,
            self.config.wal_segment_bytes,
            self.quotas.clone(),
        )
        .await?;
        runtime.spawn_listener(cluster_config.raft_listen);
        let runtime = ClusterRuntime::real(runtime);
        self.sync_from_cluster(&runtime).await?;
        *self.cluster.lock().await = Some(runtime);
        Ok(())
    }

    pub(super) async fn start_route_mesh(&self) -> Result<()> {
        let Some(route_mesh) = &self.route_mesh else {
            return Ok(());
        };
        route_mesh.start(self.clone()).await?;
        self.sync_route_interests().await;
        Ok(())
    }

    pub(super) fn spawn_cluster_log_monitor(&self) {
        if self.config.cluster.is_none() {
            return;
        }
        let broker = self.clone();
        tokio::spawn(async move {
            broker.cluster_log_monitor().await;
        });
    }

    pub(super) async fn cluster_log_monitor(self) {
        let mut previous_leader = self.current_leader_for_log().await;
        let mut full_mesh_formed = false;
        let mut interval =
            tokio::time::interval(Duration::from_millis(CLUSTER_LOG_SCAN_INTERVAL_MS));
        loop {
            interval.tick().await;
            if let Some(cluster) = self.cluster_runtime().await {
                if let Err(err) = self.sync_cluster_deltas(&cluster).await {
                    error!(error = ?err, "cluster delta application failed");
                }
                if cluster.is_leader().await
                    && let Err(err) = cluster.ensure_metadata_ready().await
                {
                    error!(error = ?err, "cluster metadata reconciliation failed");
                }
            }
            let leader = self.current_leader_for_log().await;
            if leader != previous_leader {
                self.log_cluster_event("cluster leader changed").await;
                previous_leader = leader;
            }

            let Some(route_mesh) = &self.route_mesh else {
                continue;
            };
            let cluster_size = self.cluster_size_for_log().await;
            let formed = cluster_size > 1
                && route_mesh.connected_peer_count().await >= cluster_size.saturating_sub(1);
            if formed && !full_mesh_formed {
                self.log_cluster_event("full member cluster formed").await;
            }
            full_mesh_formed = formed;
        }
    }

    pub(super) async fn log_cluster_event(&self, event: &str) {
        let cluster_size = self.cluster_size_for_log().await;
        let leader_id = format_leader_id(self.current_leader_for_log().await);
        info!(event, cluster_size, leader_id, "cluster lifecycle");
    }

    pub(super) async fn cluster_size_for_log(&self) -> usize {
        self.cluster_runtime()
            .await
            .map(|cluster| cluster.cluster_size())
            .or_else(|| {
                self.config
                    .cluster
                    .as_ref()
                    .map(|cluster| cluster.nodes.len())
            })
            .unwrap_or(1)
    }

    pub(super) async fn current_leader_for_log(&self) -> Option<u64> {
        self.cluster_runtime().await?.current_leader().await
    }

    pub(super) async fn cluster_runtime(&self) -> Option<ClusterRuntime> {
        self.cluster.lock().await.clone()
    }

    pub(super) async fn sync_from_cluster(&self, cluster: &ClusterRuntime) -> Result<()> {
        let _storage_operation = self.storage_gate.read().await;
        let state = cluster.durable_state();
        let group_records = state.groups.clone();
        let last_applied = state.last_applied.map(|log_id| log_id.index);
        let mut inner = self.inner.lock().await;
        inner.sync_durable_state(&self.partition_logs, state, &self.config.streams)?;
        drop(inner);
        let coordinators = group_records
            .into_iter()
            .map(|(group, record)| {
                crate::consumer_group::GroupCoordinator::from_record(record)
                    .map(|coordinator| (group, coordinator))
                    .with_context(|| "replaying replicated consumer-group state".to_string())
            })
            .collect::<Result<HashMap<_, _>>>()?;
        *self.groups.lock().await = coordinators;
        self.set_cluster_applied_log_index(last_applied);
        self.cluster_application_metrics
            .full_reconciliations
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(super) async fn sync_route_interests(&self) {
        let Some(route_mesh) = &self.route_mesh else {
            return;
        };
        let interests = self.transient.lock().await.route_interests();
        route_mesh.set_local_interests(interests).await;
    }

    pub(super) async fn update_route_interests(&self, changes: RouteInterestChanges) {
        let Some(route_mesh) = &self.route_mesh else {
            return;
        };
        route_mesh.update_local_interests(changes).await;
    }

    pub(super) async fn cluster_write(
        &self,
        cluster: &ClusterRuntime,
        command: BrokerCommand,
    ) -> Result<BrokerResponse> {
        let apply_command = command.clone();
        let response = if self.route_mesh.is_some() {
            cluster.client_write_forwarded(command).await
        } else {
            cluster.client_write(command).await
        }?;
        if cluster.has_delta_stream() {
            self.sync_cluster_deltas(cluster).await?;
        } else {
            self.apply_cluster_command(apply_command, &response).await?;
            self.cluster_application_metrics
                .delta_applications
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(response)
    }
}
