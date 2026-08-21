use super::*;

impl RouteMesh {
    pub(super) fn from_config(config: &Config) -> Option<Self> {
        let cluster = config.cluster.as_ref()?;
        let route_addr = cluster.route_listen?;
        Some(Self {
            inner: Arc::new(Mutex::new(RouteMeshState {
                node_id: cluster.node_id,
                route_addr,
                client_addr: config.listen,
                seeds: cluster.routes.clone(),
                reconnect_ms: cluster.route_reconnect_ms,
                peers: HashMap::new(),
                known_peers: HashMap::new(),
                local_interests: Vec::new(),
            })),
            auth_token: cluster.auth_token.clone(),
        })
    }

    pub(super) async fn start(&self, broker: Broker) -> Result<()> {
        let (listen, reconnect_ms) = {
            let state = self.inner.lock().await;
            (state.route_addr, state.reconnect_ms)
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
                        let mesh = accept_mesh.clone();
                        let broker = accept_broker.clone();
                        let auth_token = accept_auth_token.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_route_stream(
                                mesh,
                                broker,
                                stream,
                                RouteDirection::Inbound,
                                auth_token,
                            )
                            .await
                            {
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
                for addr in addrs {
                    match TcpStream::connect(&addr).await {
                        Ok(stream) => {
                            let mesh = dial_mesh.clone();
                            let broker = dial_broker.clone();
                            let auth_token = dial_auth_token.clone();
                            tokio::spawn(async move {
                                if let Err(err) = handle_route_stream(
                                    mesh,
                                    broker,
                                    stream,
                                    RouteDirection::Outbound,
                                    auth_token,
                                )
                                .await
                                {
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

    pub(super) async fn dial_candidates(&self) -> Vec<String> {
        let state = self.inner.lock().await;
        let mut addrs = state.seeds.clone();
        addrs.extend(
            state
                .known_peers
                .values()
                .map(|peer| peer.route_addr.to_string()),
        );
        addrs.sort();
        addrs.dedup();
        addrs
            .into_iter()
            .filter(|addr| *addr != state.route_addr.to_string())
            .filter(|addr| {
                !state
                    .peers
                    .values()
                    .any(|peer| peer.info.route_addr.to_string() == *addr)
            })
            .collect()
    }

    pub(super) async fn note_dial_error(&self, addr: String, error: String) {
        let mut state = self.inner.lock().await;
        for peer in state.known_peers.values_mut() {
            if peer.route_addr.to_string() == addr {
                continue;
            }
        }
        for peer in state.peers.values_mut() {
            if peer.info.route_addr.to_string() == addr {
                peer.last_error = Some(error.clone());
                peer.reconnect_attempts = peer.reconnect_attempts.saturating_add(1);
            }
        }
    }

    pub(super) async fn hello(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::Hello {
            node_id: state.node_id,
            route_addr: state.route_addr,
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
                    route_addr: state.route_addr,
                    client_addr: state.client_addr,
                }))
                .collect(),
        }
    }

    pub(super) async fn interests(&self) -> RouteFrame {
        let state = self.inner.lock().await;
        RouteFrame::Interests {
            subjects: state.local_interests.clone(),
        }
    }

    pub(super) async fn register_peer(
        &self,
        info: RoutePeerInfo,
        direction: RouteDirection,
        sender: mpsc::Sender<RouteFrame>,
    ) -> Option<bool> {
        let mut state = self.inner.lock().await;
        if info.node_id == state.node_id || info.route_addr == state.route_addr {
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
                remote_interests: Vec::new(),
                remote_interest_index: subject::SubjectTrie::default(),
            },
        );
        Some(added)
    }

    pub(super) async fn remove_peer(&self, node_id: u64) {
        let mut state = self.inner.lock().await;
        state.peers.remove(&node_id);
    }

    pub(super) async fn merge_peers(&self, peers: Vec<RoutePeerInfo>) -> Vec<u64> {
        let mut state = self.inner.lock().await;
        let node_id = state.node_id;
        let route_addr = state.route_addr;
        let mut added = Vec::new();
        for peer in peers {
            if peer.node_id != node_id && peer.route_addr != route_addr {
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

    pub(super) async fn set_remote_interests(&self, node_id: u64, subjects: Vec<String>) {
        let mut state = self.inner.lock().await;
        if let Some(peer) = state.peers.get_mut(&node_id) {
            peer.remote_interest_index = subject::SubjectTrie::default();
            for subject in &subjects {
                peer.remote_interest_index.insert(subject, ());
            }
            peer.remote_interests = subjects;
        }
    }

    pub(super) async fn set_local_interests(&self, subjects: Vec<String>) {
        let senders = {
            let mut state = self.inner.lock().await;
            state.local_interests = subjects.clone();
            state
                .peers
                .values()
                .map(|peer| peer.sender.clone())
                .collect::<Vec<_>>()
        };
        for sender in senders {
            let _ = sender
                .send(RouteFrame::Interests {
                    subjects: subjects.clone(),
                })
                .await;
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
                route_addr: peer.route_addr.to_string(),
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
                route_addr: peer.info.route_addr.to_string(),
                client_addr: peer.info.client_addr.to_string(),
                direction: match peer.direction {
                    RouteDirection::Inbound => "inbound",
                    RouteDirection::Outbound => "outbound",
                },
                state: peer.state,
                reconnect_attempts: peer.reconnect_attempts,
                last_error: peer.last_error.clone(),
                subscriptions: peer.remote_interests.len(),
                subjects: peer.remote_interests.clone(),
            })
            .collect::<Vec<_>>();
        connected.sort_by_key(|peer| peer.node_id);
        RouteTopologyResponse {
            listen: state.route_addr.to_string(),
            seeds: state.seeds.clone(),
            discovered,
            connected,
        }
    }
}

pub(super) async fn handle_route_stream(
    mesh: RouteMesh,
    broker: Broker,
    stream: TcpStream,
    direction: RouteDirection,
    auth_token: String,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (sender, mut receiver) = mpsc::channel::<RouteFrame>(256);
    let writer_auth_token = auth_token.clone();
    sender
        .send(mesh.hello().await)
        .await
        .map_err(|_| BrokerError::msg("route writer closed"))?;
    sender
        .send(mesh.peer_list().await)
        .await
        .map_err(|_| BrokerError::msg("route writer closed"))?;
    sender
        .send(mesh.interests().await)
        .await
        .map_err(|_| BrokerError::msg("route writer closed"))?;
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = receiver.recv().await {
            write_route_frame(&mut writer, &writer_auth_token, &frame).await?;
        }
        Ok::<(), BrokerError>(())
    });

    let mut peer_id = None;
    loop {
        let Some(frame) = read_route_frame(&mut reader, &auth_token).await? else {
            break;
        };
        match frame {
            RouteFrame::Hello {
                node_id,
                route_addr,
                client_addr,
            } => {
                let info = RoutePeerInfo {
                    node_id,
                    route_addr,
                    client_addr,
                };
                let Some(added_peer) = mesh.register_peer(info, direction, sender.clone()).await
                else {
                    break;
                };
                peer_id = Some(node_id);
                if added_peer {
                    broker.log_cluster_event("cluster peer added").await;
                }
                mesh.broadcast_peer_list().await;
            }
            RouteFrame::PeerList { peers } => {
                for _ in mesh.merge_peers(peers).await {
                    broker.log_cluster_event("cluster peer added").await;
                }
            }
            RouteFrame::Interests { subjects } => {
                if let Some(node_id) = peer_id {
                    mesh.set_remote_interests(node_id, subjects).await;
                }
            }
            RouteFrame::Publish {
                subject,
                reply_to,
                payload,
            } => {
                broker
                    .deliver_route_publish(&subject, reply_to.as_deref(), &payload)
                    .await?;
            }
            RouteFrame::Ping => {
                let _ = sender.send(RouteFrame::Pong).await;
            }
            RouteFrame::Pong => {}
        }
    }
    if let Some(node_id) = peer_id {
        mesh.remove_peer(node_id).await;
    }
    writer_task.abort();
    Ok(())
}

pub(super) async fn read_route_frame<R>(
    reader: &mut R,
    auth_token: &str,
) -> Result<Option<RouteFrame>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0_u8; 4];
    match tokio::time::timeout(
        Duration::from_millis(ROUTE_FRAME_READ_TIMEOUT_MS),
        reader.read_exact(&mut len),
    )
    .await
    .map_err(|_| BrokerError::msg("route frame read timed out"))?
    {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let len = u32::from_be_bytes(len) as usize;
    crate::broker_ensure!(len <= MAX_ROUTE_FRAME, "route frame too large");
    let mut payload = vec![0; len];
    tokio::time::timeout(
        Duration::from_millis(ROUTE_FRAME_READ_TIMEOUT_MS),
        reader.read_exact(&mut payload),
    )
    .await
    .map_err(|_| BrokerError::msg("route frame read timed out"))??;
    let envelope: AuthenticatedRouteFrame =
        serde_json::from_slice(&payload).context("decoding route frame")?;
    crate::broker_ensure!(
        crate::security::constant_time_eq(&envelope.auth_token, auth_token),
        "invalid route auth token"
    );
    Ok(Some(envelope.frame))
}

pub(super) async fn write_route_frame<W>(
    writer: &mut W,
    auth_token: &str,
    frame: &RouteFrame,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(&AuthenticatedRouteFrame {
        auth_token: auth_token.to_string(),
        frame: frame.clone(),
    })
    .context("encoding route frame")?;
    crate::broker_ensure!(payload.len() <= u32::MAX as usize, "route frame too large");
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&payload).await?;
    Ok(())
}
