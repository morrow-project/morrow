use super::*;

impl RouteMesh {
    pub(super) fn from_config(
        config: &Config,
        quotas: Arc<crate::quota::QuotaRuntime>,
    ) -> Result<Option<Self>> {
        let Some(cluster) = config.cluster.as_ref() else {
            return Ok(None);
        };
        let Some(route_listen) = cluster.route_listen else {
            return Ok(None);
        };
        let route_advertise = cluster
            .advertised_route_addr()
            .expect("validated route advertisement")
            .to_string();
        let tls = cluster
            .route_tls
            .as_ref()
            .map(|tls| -> Result<RouteTlsRuntime> {
                Ok(RouteTlsRuntime {
                    acceptor: crate::tls::load_internal_acceptor(tls)?,
                    connector: crate::tls::load_internal_connector(tls)?,
                    peer_identities: Arc::new(crate::tls::load_peer_certificates(&cluster.nodes)?),
                    server_names: Arc::new(
                        cluster
                            .nodes
                            .iter()
                            .map(|node| {
                                (
                                    node.node_id,
                                    node.tls_server_name.clone().expect("validated TLS name"),
                                )
                            })
                            .collect(),
                    ),
                    handshake_timeout_ms: tls.handshake_timeout_ms,
                })
            })
            .transpose()?;
        let configured_route_nodes = Arc::new(
            cluster
                .nodes
                .iter()
                .filter_map(|node| Some((node.route_addr.clone()?, node.node_id)))
                .collect(),
        );
        Ok(Some(Self {
            inner: Arc::new(Mutex::new(RouteMeshState {
                node_id: cluster.node_id,
                route_listen,
                route_advertise,
                client_addr: config.listen,
                seeds: cluster.routes.clone(),
                reconnect_ms: cluster.route_reconnect_ms,
                peers: HashMap::new(),
                known_peers: HashMap::new(),
                local_interests: BTreeSet::new(),
                local_interest_version: 0,
            })),
            auth_token: cluster.auth_token.clone(),
            tls,
            configured_route_nodes,
            quotas,
        }))
    }

    pub(super) async fn start(&self, broker: Morrow) -> Result<()> {
        let (listen, reconnect_ms) = {
            let state = self.inner.lock().await;
            (state.route_listen, state.reconnect_ms)
        };
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding route listener {listen}"))?;
        let accept_mesh = self.clone();
        let accept_broker = broker.clone();
        let accept_auth_token = self.auth_token.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let Some(permit) = accept_mesh.quotas.try_route() else {
                            continue;
                        };
                        let mesh = accept_mesh.clone();
                        let broker = accept_broker.clone();
                        let auth_token = accept_auth_token.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            let result =
                                accept_route_stream(mesh, broker, stream, auth_token).await;
                            if let Err(err) = result {
                                error!(error = ?err, "route connection error");
                            }
                        });
                    }
                    Err(err) => {
                        error!(error = ?err, "accepting route connection failed");
                    }
                }
            }
        });

        let dial_mesh = self.clone();
        let dial_broker = broker;
        let dial_auth_token = self.auth_token.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(reconnect_ms));
            loop {
                interval.tick().await;
                let addrs = dial_mesh.dial_candidates().await;
                for (addr, expected_node_id) in addrs {
                    let Some(permit) = dial_mesh.quotas.try_route() else {
                        break;
                    };
                    match TcpStream::connect(&addr).await {
                        Ok(stream) => {
                            let mesh = dial_mesh.clone();
                            let broker = dial_broker.clone();
                            let auth_token = dial_auth_token.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let result = connect_route_stream(
                                    mesh,
                                    broker,
                                    stream,
                                    auth_token,
                                    expected_node_id,
                                )
                                .await;
                                if let Err(err) = result {
                                    error!(error = ?err, "route connection error");
                                }
                            });
                        }
                        Err(err) => {
                            dial_mesh.note_dial_error(addr, err.to_string()).await;
                        }
                    }
                }
            }
        });
        Ok(())
    }

    pub(super) async fn dial_candidates(&self) -> Vec<(String, Option<u64>)> {
        let state = self.inner.lock().await;
        let mut addrs = state.seeds.clone();
        addrs.extend(
            state
                .known_peers
                .values()
                .map(|peer| peer.route_addr.clone()),
        );
        addrs.sort();
        addrs.dedup();
        addrs
            .into_iter()
            .filter(|addr| *addr != state.route_advertise)
            .filter(|addr| {
                !state
                    .peers
                    .values()
                    .any(|peer| peer.info.route_addr == *addr)
            })
            .map(|addr| {
                let node_id = self.configured_route_nodes.get(&addr).copied();
                (addr, node_id)
            })
            .collect()
    }

    pub(super) async fn note_dial_error(&self, addr: String, error: String) {
        let mut state = self.inner.lock().await;
        for peer in state.known_peers.values_mut() {
            if peer.route_addr == addr {
                continue;
            }
        }
        for peer in state.peers.values_mut() {
            if peer.info.route_addr == addr {
                peer.last_error = Some(error.clone());
                peer.reconnect_attempts = peer.reconnect_attempts.saturating_add(1);
            }
        }
    }

    pub(super) async fn hello(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::Hello {
            node_id: state.node_id,
            route_addr: state.route_advertise.clone(),
            client_addr: state.client_addr,
        }
    }

    pub(super) async fn peer_list(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::PeerList {
            peers: state
                .known_peers
                .values()
                .cloned()
                .chain(std::iter::once(RoutePeerInfo {
                    node_id: state.node_id,
                    route_addr: state.route_advertise.clone(),
                    client_addr: state.client_addr,
                }))
                .collect(),
        }
    }

    pub(super) async fn interests(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::Interests {
            version: state.local_interest_version,
            subjects: state.local_interests.iter().cloned().collect(),
        }
    }

    pub(super) async fn register_peer(
        &self,
        info: RoutePeerInfo,
        direction: RouteDirection,
        sender: mpsc::Sender<RouteFrame>,
    ) -> Option<bool> {
        let mut state = self.inner.lock().await;
        if info.node_id == state.node_id
            || info.route_addr == state.route_advertise
            || !preferred_route_direction(state.node_id, info.node_id, direction)
            || state
                .known_peers
                .values()
                .any(|peer| peer.node_id != info.node_id && peer.route_addr == info.route_addr)
        {
            return None;
        }
        let added = !state.known_peers.contains_key(&info.node_id);
        state.known_peers.insert(info.node_id, info.clone());
        state.peers.insert(
            info.node_id,
            RoutePeer {
                info,
                sender,
                direction,
                state: "connected",
                reconnect_attempts: 0,
                last_error: None,
                remote_interests: BTreeSet::new(),
                remote_interest_version: 0,
                remote_interest_index: subject::SubjectTrie::default(),
            },
        );
        Some(added)
    }

    pub(super) async fn remove_peer(&self, node_id: u64, sender: &mpsc::Sender<RouteFrame>) {
        let mut state = self.inner.lock().await;
        if state
            .peers
            .get(&node_id)
            .is_some_and(|peer| peer.sender.same_channel(sender))
        {
            state.peers.remove(&node_id);
        }
    }

    pub(super) async fn merge_peers(&self, peers: Vec<RoutePeerInfo>) -> Vec<u64> {
        let mut state = self.inner.lock().await;
        let node_id = state.node_id;
        let route_advertise = state.route_advertise.clone();
        let mut added = Vec::new();
        for peer in peers {
            if peer.node_id != node_id
                && peer.route_addr != route_advertise
                && !state.known_peers.values().any(|known| {
                    known.node_id != peer.node_id && known.route_addr == peer.route_addr
                })
            {
                if !state.known_peers.contains_key(&peer.node_id) {
                    added.push(peer.node_id);
                }
                state.known_peers.insert(peer.node_id, peer);
            }
        }
        added
    }

    pub(super) async fn connected_peer_count(&self) -> usize {
        self.inner.lock().await.peers.len()
    }

    pub(super) async fn set_remote_interests(
        &self,
        node_id: u64,
        version: u64,
        subjects: Vec<String>,
    ) {
        let mut state = self.inner.lock().await;
        if let Some(peer) = state.peers.get_mut(&node_id)
            && version >= peer.remote_interest_version
        {
            peer.remote_interest_index = subject::SubjectTrie::default();
            for subject in &subjects {
                peer.remote_interest_index.insert(subject, ());
            }
            peer.remote_interests = subjects.into_iter().collect();
            peer.remote_interest_version = version;
        }
    }

    pub(super) async fn apply_remote_interest_delta(
        &self,
        node_id: u64,
        version: u64,
        added: Vec<String>,
        removed: Vec<String>,
    ) -> bool {
        let mut state = self.inner.lock().await;
        let Some(peer) = state.peers.get_mut(&node_id) else {
            return true;
        };
        if version != peer.remote_interest_version.saturating_add(1) {
            return false;
        }
        for subject in removed {
            peer.remote_interests.remove(&subject);
            peer.remote_interest_index.remove(&subject, &());
        }
        for subject in added {
            peer.remote_interests.insert(subject.clone());
            peer.remote_interest_index.insert(&subject, ());
        }
        peer.remote_interest_version = version;
        true
    }

    pub(super) async fn set_local_interests(&self, subjects: Vec<String>) {
        let frame_and_senders = {
            let mut state = self.inner.lock().await;
            let subjects = subjects.into_iter().collect::<BTreeSet<_>>();
            if state.local_interests == subjects {
                return;
            }
            state.local_interests = subjects;
            state.local_interest_version = state.local_interest_version.saturating_add(1);
            let frame = RouteFrame::Interests {
                version: state.local_interest_version,
                subjects: state.local_interests.iter().cloned().collect(),
            };
            let senders = state
                .peers
                .values()
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>();
            (frame, senders)
        };
        let (frame, senders) = frame_and_senders;
        for sender in senders {
            let _ = sender.send(frame.clone()).await;
        }
    }

    pub(super) async fn update_local_interests(&self, changes: RouteInterestChanges) {
        if changes.is_empty() {
            return;
        }
        let frame_and_senders = {
            let mut state = self.inner.lock().await;
            for subject in &changes.removed {
                state.local_interests.remove(subject);
            }
            for subject in &changes.added {
                state.local_interests.insert(subject.clone());
            }
            state.local_interest_version = state.local_interest_version.saturating_add(1);
            let frame = RouteFrame::InterestDelta {
                version: state.local_interest_version,
                added: changes.added,
                removed: changes.removed,
            };
            let senders = state
                .peers
                .values()
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>();
            (frame, senders)
        };
        let (frame, senders) = frame_and_senders;
        for sender in senders {
            let _ = sender.send(frame.clone()).await;
        }
    }

    pub(super) async fn broadcast_peer_list(&self) {
        let frame = self.peer_list().await;
        let senders = {
            let state = self.inner.lock().await;
            state
                .peers
                .values()
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>()
        };
        for sender in senders {
            let _ = sender.send(frame.clone()).await;
        }
    }

    pub(super) async fn forward_publish(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) {
        let _span = tracing::info_span!("morrow.route.forward", payload_bytes = payload.len());
        let targets = {
            let state = self.inner.lock().await;
            state
                .peers
                .values()
                .filter(|peer| peer.remote_interest_index.matches_any(subject))
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>()
        };
        for sender in targets {
            let _ = sender
                .send(RouteFrame::Publish {
                    subject: subject.to_string(),
                    reply_to: reply_to.map(str::to_string),
                    payload: payload.to_vec(),
                })
                .await;
        }
    }

    pub(super) async fn topology_response(&self) -> RouteTopologyResponse {
        let state = self.inner.lock().await;
        let mut discovered = state
            .known_peers
            .values()
            .map(|peer| RouteDiscoveredPeerResponse {
                node_id: peer.node_id,
                route_addr: peer.route_addr.clone(),
                client_addr: peer.client_addr.to_string(),
                connected: state.peers.contains_key(&peer.node_id),
            })
            .collect::<Vec<_>>();
        discovered.sort_by_key(|peer| peer.node_id);
        let mut connected = state
            .peers
            .iter()
            .map(|(node_id, peer)| RoutePeerResponse {
                node_id: *node_id,
                route_addr: peer.info.route_addr.clone(),
                client_addr: peer.info.client_addr.to_string(),
                direction: match peer.direction {
                    RouteDirection::Inbound => "inbound",
                    RouteDirection::Outbound => "outbound",
                },
                state: peer.state,
                reconnect_attempts: peer.reconnect_attempts,
                last_error: peer.last_error.clone(),
                subscriptions: peer.remote_interests.len(),
                subjects: peer.remote_interests.iter().cloned().collect(),
            })
            .collect::<Vec<_>>();
        connected.sort_by_key(|peer| peer.node_id);
        RouteTopologyResponse {
            listen: state.route_listen.to_string(),
            seeds: state.seeds.clone(),
            discovered,
            connected,
        }
    }
}

fn preferred_route_direction(
    local_node_id: u64,
    remote_node_id: u64,
    direction: RouteDirection,
) -> bool {
    matches!(
        (local_node_id.cmp(&remote_node_id), direction),
        (std::cmp::Ordering::Less, RouteDirection::Inbound)
            | (std::cmp::Ordering::Greater, RouteDirection::Outbound)
    )
}
