use super::*;

impl Morrow {
    pub(super) fn spawn_http_status_listener(&self, listener: Option<TcpListener>) {
        let Some(listener) = listener else {
            return;
        };
        let broker = self.clone();
        tokio::spawn(async move {
            if let Err(err) = broker.serve_http_status(listener).await {
                error!(error = ?err, "http status error");
            }
        });
    }

    pub(super) async fn serve_http_status(&self, listener: TcpListener) -> Result<()> {
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
        let Some(request_target) = http_request_path(request_line) else {
            return write_http_not_found(&mut stream).await;
        };
        let method = http_request_method(request_line).unwrap_or("GET");
        let (path, query) = request_target
            .split_once('?')
            .unwrap_or((request_target, ""));

        if matches!(path, "/health/live" | "/api/v1/health/live") {
            return write_http_response(
                &mut stream,
                "200 OK",
                "application/json",
                br#"{"status":"alive"}"#,
            )
            .await;
        }

        if matches!(path, "/health/ready" | "/api/v1/health/ready") {
            let health = self.health_response().await;
            let status = if health.status == "ready" {
                "200 OK"
            } else {
                "503 Service Unavailable"
            };
            let body = serde_json::to_vec(&health).context("serializing HTTP health response")?;
            return write_http_response(&mut stream, status, "application/json", &body).await;
        }

        let Some(admin_token) = self.config.admin_token.as_deref() else {
            return write_http_unauthorized(&mut stream).await;
        };
        if !http_authorized(&request, admin_token) {
            return write_http_unauthorized(&mut stream).await;
        }
        if method == "GET" && matches!(path, "/audit/status" | "/api/v1/audit/status") {
            let body = serde_json::to_vec(&self.audit_status())
                .context("serializing HTTP audit status response")?;
            return write_http_response(&mut stream, "200 OK", "application/json", &body).await;
        }
        if method == "POST" && matches!(path, "/audit/verify" | "/api/v1/audit/verify") {
            return match self.verify_audit_log() {
                Ok(()) => {
                    write_http_response(
                        &mut stream,
                        "200 OK",
                        "application/json",
                        br#"{"verified":true}"#,
                    )
                    .await
                }
                Err(error) => {
                    let body = serde_json::json!({"verified": false, "error": error.to_string()});
                    let body = serde_json::to_vec(&body)
                        .context("serializing HTTP audit verification response")?;
                    write_http_response(
                        &mut stream,
                        "500 Internal Server Error",
                        "application/json",
                        &body,
                    )
                    .await
                }
            };
        }
        if method == "GET" && matches!(path, "/audit/export" | "/api/v1/audit/export") {
            let body = self.export_audit_log()?;
            return write_http_response(&mut stream, "200 OK", "application/x-ndjson", &body).await;
        }
        if method == "DELETE" {
            if let Some(id) = path
                .strip_prefix("/api/v1/dead-letters/")
                .or_else(|| path.strip_prefix("/dead-letters/"))
                .and_then(|value| value.parse().ok())
            {
                return if self.purge_dead_letter(id).await? {
                    write_http_response(&mut stream, "204 No Content", "application/json", &[])
                        .await
                } else {
                    write_http_not_found(&mut stream).await
                };
            }
        }
        if method == "GET" {
            if let Some(id) = path
                .strip_prefix("/api/v1/dead-letters/")
                .or_else(|| path.strip_prefix("/dead-letters/"))
                .and_then(|value| value.parse().ok())
            {
                return match self.dead_letter_response(id).await {
                    Some(record) => {
                        let body = serde_json::to_vec(&record)
                            .context("serializing HTTP dead-letter record")?;
                        write_http_response(&mut stream, "200 OK", "application/json", &body).await
                    }
                    None => write_http_not_found(&mut stream).await,
                };
            }
        }
        if method == "POST" {
            if let Some(id) = path
                .strip_prefix("/api/v1/dead-letters/")
                .and_then(|value| value.strip_suffix("/replay"))
                .or_else(|| {
                    path.strip_prefix("/dead-letters/")
                        .and_then(|value| value.strip_suffix("/replay"))
                })
                .and_then(|value| value.parse().ok())
            {
                let replayed = self.replay_dead_letter(id).await?;
                let body = format!("{{\"replayed\":{replayed}}}");
                return write_http_response(
                    &mut stream,
                    if replayed { "200 OK" } else { "404 Not Found" },
                    "application/json",
                    body.as_bytes(),
                )
                .await;
            }
        }
        if method == "GET" {
            if let Some((name, key)) = path
                .strip_prefix("/api/v1/views/")
                .or_else(|| path.strip_prefix("/views/"))
                .and_then(|value| value.split_once('/'))
            {
                return match self.view_query(name, key).await {
                    Some(view) => {
                        let body = serde_json::to_vec(&view)
                            .context("serializing HTTP view query response")?;
                        write_http_response(&mut stream, "200 OK", "application/json", &body).await
                    }
                    None => write_http_not_found(&mut stream).await,
                };
            }
        }
        match path {
            "/cluster" | "/api/v1/cluster" => self.write_cluster_response(&mut stream).await,
            "/connections" => self.write_connections_response(&mut stream, None).await,
            "/api/v1/connections" => {
                self.write_connections_response(&mut stream, Some(parse_page(query)))
                    .await
            }
            "/quotas" | "/api/v1/quotas" => self.write_quotas_response(&mut stream).await,
            "/connectors" | "/api/v1/connectors" => {
                self.write_connectors_response(&mut stream).await
            }
            "/routes" | "/api/v1/routes" => self.write_routes_response(&mut stream).await,
            "/subscriptions" => self.write_subscriptions_response(&mut stream, None).await,
            "/api/v1/subscriptions" => {
                self.write_subscriptions_response(&mut stream, Some(parse_page(query)))
                    .await
            }
            "/streams" | "/api/v1/streams" => self.write_streams_response(&mut stream).await,
            "/views" | "/api/v1/views" => self.write_views_response(&mut stream).await,
            "/dead-letters" | "/api/v1/dead-letters" => {
                self.write_dead_letters_response(&mut stream, parse_page(query))
                    .await
            }
            "/producers" | "/api/v1/producers" => {
                self.write_producers_response(&mut stream, parse_page(query))
                    .await
            }
            "/groups" | "/api/v1/groups" => self.write_groups_response(&mut stream).await,
            "/wal" | "/api/v1/storage" => self.write_wal_response(&mut stream).await,
            "/middleware" | "/api/v1/middleware" => {
                self.write_middleware_response(&mut stream).await
            }
            "/metrics" | "/api/v1/metrics" => self.write_metrics_response(&mut stream).await,
            _ => write_http_not_found(&mut stream).await,
        }
    }

    async fn write_views_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.views_response().await)
            .context("serializing HTTP views response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_cluster_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.cluster_response().await)
            .context("serializing HTTP cluster response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_connections_response<W: AsyncWrite + Unpin>(
        &self,
        stream: &mut W,
        page: Option<(usize, usize)>,
    ) -> Result<()> {
        let body = serde_json::to_vec(&self.connections_response_page(page).await)
            .context("serializing HTTP connections response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_quotas_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.quotas_response().await)
            .context("serializing HTTP quota response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_connectors_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.connectors_response().await)
            .context("serializing HTTP connectors response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_routes_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.routes_response().await)
            .context("serializing HTTP routes response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_subscriptions_response<W: AsyncWrite + Unpin>(
        &self,
        stream: &mut W,
        page: Option<(usize, usize)>,
    ) -> Result<()> {
        let body = match page {
            Some((offset, limit)) => {
                serde_json::to_vec(&self.subscriptions_response_page(offset, limit).await)
            }
            None => serde_json::to_vec(&self.subscriptions_response().await),
        }
        .context("serializing HTTP subscriptions response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_streams_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.streams_response().await)
            .context("serializing HTTP streams response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_groups_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let groups = self.groups.lock().await;
        let mut response = groups
            .iter()
            .map(|(name, coordinator)| (name.clone(), coordinator.snapshot()))
            .collect::<Vec<_>>();
        response.sort_by(|left, right| left.0.cmp(&right.0));
        let body = serde_json::to_vec(&response).context("serializing HTTP group response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_dead_letters_response<W: AsyncWrite + Unpin>(
        &self,
        stream: &mut W,
        (offset, limit): (usize, usize),
    ) -> Result<()> {
        let body = serde_json::to_vec(&self.dead_letters_response_page(offset, limit).await)
            .context("serializing HTTP dead-letter response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_producers_response<W: AsyncWrite + Unpin>(
        &self,
        stream: &mut W,
        (offset, limit): (usize, usize),
    ) -> Result<()> {
        let body = serde_json::to_vec(&self.producers_response_page(offset, limit).await)
            .context("serializing HTTP producer response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_wal_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&self.wal_status_response().await)
            .context("serializing HTTP WAL response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }

    async fn write_metrics_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = self.metrics_response().await;
        write_http_text_response(stream, "200 OK", &body).await
    }

    async fn write_middleware_response<W: AsyncWrite + Unpin>(&self, stream: &mut W) -> Result<()> {
        let body = serde_json::to_vec(&MiddlewareResponse {
            current_generation: self.middleware.current_generation(),
        })
        .context("serializing HTTP middleware response")?;
        write_http_response(stream, "200 OK", "application/json", &body).await
    }
}

fn parse_page(query: &str) -> (usize, usize) {
    let offset = http_query_parameter(query, "offset")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let limit = http_query_parameter(query, "limit")
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);
    (offset, limit.clamp(1, 1_000))
}
