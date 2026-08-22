use super::*;

impl Morrow {
    pub(super) fn spawn_http_status_listener(&self) {
        let Some(listen) = self.config.http_listen else {
            return;
        };
        let broker = self.clone();
        tokio::spawn(async move {
            if let Err(err) = broker.serve_http_status(listen).await {
                error!(error = ?err, "http status error");
            }
        });
    }

    pub(super) async fn serve_http_status(&self, listen: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("binding HTTP status listener {listen}"))?;
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .context("accepting HTTP status connection")?;
            let Some(permit) = self.quotas.try_http() else {
                continue;
            };
            let broker = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let result = broker.accept_http_status(stream).await;
                if let Err(err) = result {
                    error!(error = ?err, "http status connection error");
                }
            });
        }
    }

    async fn accept_http_status(&self, stream: TcpStream) -> Result<()> {
        if let Some(acceptor) = &self.admin_tls_acceptor {
            let timeout_ms = self
                .config
                .admin_tls
                .as_ref()
                .map(|tls| tls.handshake_timeout_ms)
                .unwrap_or(2_000);
            let mut stream =
                tokio::time::timeout(Duration::from_millis(timeout_ms), acceptor.accept(stream))
                    .await
                    .map_err(|_| BrokerError::msg("admin TLS handshake timed out"))?
                    .context("accepting admin TLS connection")?;
            self.handle_http_status(&mut stream).await?;
            stream
                .shutdown()
                .await
                .context("shutting down admin TLS connection")
        } else {
            let mut stream = stream;
            self.handle_http_status(&mut stream).await?;
            stream
                .shutdown()
                .await
                .context("shutting down admin connection")
        }
    }

    pub(super) async fn handle_http_status<S>(&self, mut stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        let complete = tokio::time::timeout(
            Duration::from_millis(self.config.quotas.http_header_timeout_ms),
            async {
                loop {
                    let read = stream
                        .read(&mut buf)
                        .await
                        .context("reading HTTP status request")?;
                    if read == 0 {
                        return Ok::<bool, BrokerError>(false);
                    }
                    request.extend_from_slice(&buf[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n")
                        || request.len() >= 8 * 1024
                    {
                        return Ok::<bool, BrokerError>(true);
                    }
                }
            },
        )
        .await
        .map_err(|_| BrokerError::msg("admin HTTP header read timed out"))??;
        if !complete {
            return Ok(());
        }
        let request_line = request
            .split(|byte| *byte == b'\n')
            .next()
            .and_then(|line| std::str::from_utf8(line).ok())
            .map(str::trim_end)
            .unwrap_or("");
        let Some(path) = http_request_path(request_line) else {
            return write_http_not_found(&mut stream).await;
        };
        let Some(admin_token) = self.config.admin_token.as_deref() else {
            return write_http_unauthorized(&mut stream).await;
        };
        if !http_authorized(&request, admin_token) {
            return write_http_unauthorized(&mut stream).await;
        }
        match path {
            "/cluster" => self.write_cluster_response(&mut stream).await,
            "/connections" => self.write_connections_response(&mut stream).await,
            "/quotas" => self.write_quotas_response(&mut stream).await,
            "/subscriptions" => self.write_subscriptions_response(&mut stream).await,
            "/streams" => self.write_streams_response(&mut stream).await,
            "/wal" => self.write_wal_response(&mut stream).await,
            _ => write_http_not_found(&mut stream).await,
        }
    }

    async fn write_cluster_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.cluster_response().await)
            .context("serializing HTTP cluster response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_connections_response<W: AsyncWrite + Unpin>(
        &self,
        stream: &mut W,
    ) -> Result<()> {
        let body = serde_json::to_vec(&self.connections_response().await)
            .context("serializing HTTP connections response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_quotas_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.quotas_response().await)
            .context("serializing HTTP quota response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_subscriptions_response<W: AsyncWrite + Unpin>(
        &self,
        stream: &mut W,
    ) -> Result<()> {
        let body = serde_json::to_vec(&self.subscriptions_response().await)
            .context("serializing HTTP subscriptions response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_streams_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.streams_response().await)
            .context("serializing HTTP streams response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_wal_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.wal_status_response().await)
            .context("serializing HTTP WAL response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }
}
