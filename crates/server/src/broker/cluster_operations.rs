use super::*;

impl Morrow {
    pub(super) async fn start_cluster(&self, listener: Option<TcpListener>) -> Result<()> {
        let Some(cluster_config) = &self.config.cluster else {
            return Ok(());
        };
        let runtime = RaftRuntime::open(
            cluster_config,
            self.tls_acceptor.is_some(),
            &self.config.streams,
            self.config.wal_segment_bytes,
            self.quotas.clone(),
            self.work_scheduler.clone(),
        )
        .await?;
        runtime.spawn_listener(
            listener.expect("configured Raft listener was not pre-bound"),
            self.broker_control.clone(),
            cluster_config.role.participates_in_metadata_quorum(),
        );
        if matches!(cluster_config.role, crate::config::ClusterRole::Broker) {
            self.spawn_broker_registration(runtime.clone(), cluster_config.clone());
        }
        let runtime = ClusterRuntime::real(runtime);
        self.sync_from_cluster(&runtime).await?;
        let bootstrap_runtime = runtime.clone();
        *self.cluster.lock().await = Some(runtime);
        self.spawn_metadata_bootstrap(bootstrap_runtime);
        Ok(())
    }

    fn spawn_metadata_bootstrap(&self, runtime: ClusterRuntime) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(25));
            loop {
                interval.tick().await;
                if runtime.is_leader().await && runtime.ensure_metadata_ready().await.is_ok() {
                    break;
                }
            }
        });
    }

    fn spawn_broker_registration(
        &self,
        runtime: RaftRuntime,
        cluster_config: crate::config::ClusterConfig,
    ) {
        let broker_id = cluster_config.node_id;
        let controller_id = cluster_config
            .controller_voters
            .iter()
            .copied()
            .find(|id| *id != broker_id)
            .or_else(|| cluster_config.controller_voters.first().copied());
        let Some(controller_id) = controller_id else {
            return;
        };
        let client_addr = self.config.listen.to_string();
        let replication_addr = cluster_config.route_advertise.clone().or_else(|| {
            cluster_config
                .route_listen
                .map(|address| address.to_string())
        });
        let heartbeat_interval_ms = cluster_config.heartbeat_interval_ms;
        tracing::info!(
            broker_id,
            controller_id,
            "starting broker control registration"
        );
        tokio::spawn(async move {
            let incarnation = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(1)
                .max(1);
            let mut last_revision = 0;
            let mut session_id = None;
            let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_interval_ms));
            loop {
                tracing::trace!(
                    broker_id,
                    controller_id,
                    registered = session_id.is_some(),
                    "broker control registration tick"
                );
                if let Some(session) = session_id {
                    let heartbeat = protocol::broker_control::BrokerHeartbeat {
                        protocol_version: protocol::broker_control::BROKER_CONTROL_PROTOCOL_VERSION,
                        broker_id,
                        incarnation,
                        session_id: session,
                        capacity: protocol::broker_control::CapacitySummary::default(),
                        last_revision,
                    };
                    match runtime
                        .heartbeat_with_controller(controller_id, heartbeat)
                        .await
                    {
                        Ok(accepted) => {
                            last_revision = accepted.controller_revision;
                        }
                        Err(error) => {
                            tracing::debug!(
                                broker_id,
                                controller_id,
                                ?error,
                                "broker heartbeat failed"
                            );
                            session_id = None;
                        }
                    }
                } else {
                    let registration = protocol::broker_control::BrokerRegistration {
                        protocol_version: protocol::broker_control::BROKER_CONTROL_PROTOCOL_VERSION,
                        broker_id,
                        incarnation,
                        client_addr: client_addr.clone(),
                        replication_addr: replication_addr.clone(),
                        capacity: protocol::broker_control::CapacitySummary::default(),
                        feature_gates: Vec::new(),
                        security_references: Vec::new(),
                        last_revision,
                    };
                    match runtime
                        .register_with_controller(controller_id, registration)
                        .await
                    {
                        Ok(result) => {
                            session_id = Some(result.session_id);
                            last_revision = result.controller_revision;
                        }
                        Err(error) => {
                            tracing::debug!(
                                broker_id,
                                controller_id,
                                ?error,
                                "broker registration failed"
                            );
                        }
                    }
                }
                interval.tick().await;
            }
        });
    }

    pub(super) async fn start_route_mesh(&self, listener: Option<TcpListener>) -> Result<()> {
        if self
            .config
            .cluster
            .as_ref()
            .is_some_and(|cluster| !cluster.role.serves_client_traffic())
        {
            return Ok(());
        }
        let Some(route_mesh) = &self.route_mesh else {
            return Ok(());
        };
        route_mesh
            .start(
                self.clone(),
                listener.expect("configured route listener was not pre-bound"),
            )
            .await?;
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
                if let Err(err) = self.sync_local_partition_commits(&cluster).await {
                    error!(error = ?err, "local partition commit application failed");
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
