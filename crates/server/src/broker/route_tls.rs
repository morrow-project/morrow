use super::*;

pub(super) async fn accept_route_stream(
    mesh: RouteMesh,
    broker: Morrow,
    stream: TcpStream,
    auth_token: String,
) -> Result<()> {
    if let Some(tls) = mesh.tls.clone() {
        let stream = tokio::time::timeout(
            Duration::from_millis(tls.handshake_timeout_ms),
            tls.acceptor.accept(stream),
        )
        .await
        .map_err(|_| BrokerError::msg("route TLS handshake timed out"))?
        .context("accepting route TLS")?;
        let peer_id = crate::tls::identify_peer(
            stream.get_ref().1.peer_certificates(),
            &tls.peer_identities,
        )?;
        handle_route_stream(
            mesh,
            broker,
            stream,
            RouteDirection::Inbound,
            auth_token,
            Some(peer_id),
        )
        .await
    } else {
        handle_route_stream(
            mesh,
            broker,
            stream,
            RouteDirection::Inbound,
            auth_token,
            None,
        )
        .await
    }
}

pub(super) async fn connect_route_stream(
    mesh: RouteMesh,
    broker: Morrow,
    stream: TcpStream,
    auth_token: String,
    expected_node_id: Option<u64>,
) -> Result<()> {
    if let Some(tls) = mesh.tls.clone() {
        let expected_node_id = expected_node_id
            .ok_or_else(|| BrokerError::msg("route TLS target has no configured node ID"))?;
        let server_name = tls
            .server_names
            .get(&expected_node_id)
            .ok_or_else(|| BrokerError::msg("route TLS target has no server name"))?;
        let server_name = rustls::pki_types::ServerName::try_from(server_name.clone())
            .context("parsing route TLS server name")?;
        let stream = tokio::time::timeout(
            Duration::from_millis(tls.handshake_timeout_ms),
            tls.connector.connect(server_name, stream),
        )
        .await
        .map_err(|_| BrokerError::msg("route TLS handshake timed out"))?
        .context("connecting route TLS")?;
        let peer_id = crate::tls::identify_peer(
            stream.get_ref().1.peer_certificates(),
            &tls.peer_identities,
        )?;
        crate::broker_ensure!(
            peer_id == expected_node_id,
            "route TLS certificate belongs to a different node"
        );
        handle_route_stream(
            mesh,
            broker,
            stream,
            RouteDirection::Outbound,
            auth_token,
            Some(peer_id),
        )
        .await
    } else {
        handle_route_stream(
            mesh,
            broker,
            stream,
            RouteDirection::Outbound,
            auth_token,
            None,
        )
        .await
    }
}
