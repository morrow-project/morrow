use super::*;

pub(super) async fn handle_route_stream<S>(
    mesh: RouteMesh,
    broker: Morrow,
    stream: S,
    direction: RouteDirection,
    auth_token: String,
    tls_peer_id: Option<u64>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (sender, mut receiver) = mpsc::channel::<RouteFrame>(256);
    let writer_auth_token = auth_token.clone();
    for frame in [
        mesh.hello().await,
        mesh.peer_list().await,
        mesh.interests().await,
    ] {
        sender
            .send(frame)
            .await
            .map_err(|_| BrokerError::msg("route writer closed"))?;
    }
    let writer_task = tokio::spawn(async move {
        write_route_handshake(&mut writer, &writer_auth_token).await?;
        let heartbeat_every = Duration::from_millis(ROUTE_FRAME_READ_TIMEOUT_MS / 2);
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + heartbeat_every,
            heartbeat_every,
        );
        loop {
            let frame = tokio::select! {
                frame = receiver.recv() => {
                    let Some(frame) = frame else {
                        break;
                    };
                    frame
                }
                _ = heartbeat.tick() => RouteFrame::Ping,
            };
            write_route_session_frame(&mut writer, &frame).await?;
        }
        Ok::<(), BrokerError>(())
    });

    read_route_handshake(&mut reader, &auth_token).await?;
    let mut peer_id = None;
    while let Some(frame) = read_route_session_frame(&mut reader).await? {
        match frame {
            RouteFrame::Hello {
                node_id,
                route_addr,
                client_addr,
            } => {
                if let Some(peer_id) = tls_peer_id {
                    crate::broker_ensure!(
                        node_id == peer_id,
                        "route hello node ID does not match peer certificate"
                    );
                }
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
            RouteFrame::Interests { version, subjects } => {
                if let Some(node_id) = peer_id {
                    mesh.set_remote_interests(node_id, version, subjects).await;
                }
            }
            RouteFrame::InterestDelta {
                version,
                added,
                removed,
            } => {
                if let Some(node_id) = peer_id
                    && !mesh
                        .apply_remote_interest_delta(node_id, version, added, removed)
                        .await
                {
                    let _ = sender.send(RouteFrame::InterestResync).await;
                }
            }
            RouteFrame::InterestResync => {
                let _ = sender.send(mesh.interests().await).await;
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
        mesh.remove_peer(node_id, &sender).await;
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

const ROUTE_WIRE_VERSION: u8 = 1;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RouteWireFrame {
    version: u8,
    auth_token: Option<String>,
    frame: serde_json::Value,
}

async fn read_route_wire_frame<R>(reader: &mut R) -> Result<Option<RouteWireFrame>>
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
    let wire: RouteWireFrame = match ciborium::de::from_reader(payload.as_slice()) {
        Ok(wire) => wire,
        Err(err) => return Err(BrokerError::with_source("decoding binary route frame", err)),
    };
    crate::broker_ensure!(
        wire.version == ROUTE_WIRE_VERSION,
        "unsupported route frame version"
    );
    Ok(Some(wire))
}

pub(super) async fn read_route_handshake<R>(reader: &mut R, auth_token: &str) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let Some(wire) = read_route_wire_frame(reader).await? else {
        return Err(BrokerError::msg("route connection closed before handshake"));
    };
    crate::broker_ensure!(
        crate::security::constant_time_eq(
            wire.auth_token.as_deref().unwrap_or_default(),
            auth_token
        ),
        "invalid route auth token"
    );
    crate::broker_ensure!(
        matches!(route_frame_from_value(wire.frame)?, RouteFrame::Ping),
        "invalid route handshake"
    );
    Ok(())
}

pub(super) async fn read_route_session_frame<R>(reader: &mut R) -> Result<Option<RouteFrame>>
where
    R: AsyncRead + Unpin,
{
    let Some(wire) = read_route_wire_frame(reader).await? else {
        return Ok(None);
    };
    crate::broker_ensure!(
        wire.auth_token.is_none(),
        "route auth repeated after handshake"
    );
    Ok(Some(route_frame_from_value(wire.frame)?))
}

fn route_frame_to_value(frame: &RouteFrame) -> Result<serde_json::Value> {
    serde_json::to_value(frame).context("encoding route frame payload")
}

fn route_frame_from_value(value: serde_json::Value) -> Result<RouteFrame> {
    serde_json::from_value(value).context("decoding route frame payload")
}

async fn write_route_wire_frame<W>(writer: &mut W, wire: &RouteWireFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut payload = Vec::new();
    ciborium::ser::into_writer(wire, &mut payload).context("encoding binary route frame")?;
    crate::broker_ensure!(payload.len() <= u32::MAX as usize, "route frame too large");
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&payload).await?;
    Ok(())
}

pub(super) async fn write_route_handshake<W>(writer: &mut W, auth_token: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_route_wire_frame(
        writer,
        &RouteWireFrame {
            version: ROUTE_WIRE_VERSION,
            auth_token: Some(auth_token.to_string()),
            frame: route_frame_to_value(&RouteFrame::Ping)?,
        },
    )
    .await
}

pub(super) async fn write_route_session_frame<W>(writer: &mut W, frame: &RouteFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_route_wire_frame(
        writer,
        &RouteWireFrame {
            version: ROUTE_WIRE_VERSION,
            auth_token: None,
            frame: route_frame_to_value(frame)?,
        },
    )
    .await
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
    let mut encoded = Vec::with_capacity(4 + payload.len());
    encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&payload);
    writer.write_all(&encoded).await?;
    Ok(())
}
