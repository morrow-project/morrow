use super::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        Error as WebSocketError,
        error::CapacityError,
        handshake::server::{Request, Response},
        http::{HeaderValue, StatusCode},
        protocol::{Message, WebSocketConfig},
    },
};

const TEXT_SUBPROTOCOL: &str = "morrow.v1.text";
const BINARY_SUBPROTOCOL: &str = "morrow.v1.binary";
const BRIDGE_CAPACITY: usize = 128 * 1024;

impl Morrow {
    pub(super) async fn spawn_websocket_listener(
        &self,
        listener: Option<TcpListener>,
    ) -> Result<()> {
        let Some(config) = self.config.websocket.clone() else {
            return Ok(());
        };
        let listener = listener.expect("configured WebSocket listener was not pre-bound");
        info!(listen = %config.listen, tls = config.tls.is_some(), "WebSocket listener started");
        let broker = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let broker = broker.clone();
                        let config = config.clone();
                        tokio::spawn(async move {
                            if let Err(error) = broker
                                .handle_websocket_connection(stream, peer, config)
                                .await
                            {
                                tracing::debug!(?error, %peer, "WebSocket connection ended");
                            }
                        });
                    }
                    Err(error) => {
                        warn!(?error, "WebSocket accept failed");
                    }
                }
            }
        });
        Ok(())
    }

    async fn handle_websocket_connection(
        &self,
        stream: TcpStream,
        peer: std::net::SocketAddr,
        config: crate::config::WebSocketConfig,
    ) -> Result<()> {
        let stream: Box<dyn WebSocketIo> = if let Some(acceptor) = &self.websocket_tls_acceptor {
            let timeout_ms = config
                .tls
                .as_ref()
                .map(|tls| tls.handshake_timeout_ms)
                .unwrap_or(2_000);
            let stream =
                tokio::time::timeout(Duration::from_millis(timeout_ms), acceptor.accept(stream))
                    .await
                    .map_err(|_| BrokerError::msg("WebSocket TLS handshake timed out"))??;
            Box::new(stream)
        } else {
            Box::new(stream)
        };

        let max_frame = self
            .config
            .max_encoded_batch_bytes
            .max(self.config.max_payload)
            .saturating_add(self.config.max_control_line);
        let allowed_origins = Arc::new(config.allowed_origins);
        let binary_mode = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_mode = Arc::clone(&binary_mode);
        let callback = move |request: &Request, mut response: Response| {
            validate_origin(request, &allowed_origins)?;
            let requested = request
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            if requested.is_empty() {
                return Ok(response);
            }
            let protocol = if requested
                .split(',')
                .map(str::trim)
                .any(|protocol| protocol == BINARY_SUBPROTOCOL)
            {
                callback_mode.store(true, Ordering::Relaxed);
                BINARY_SUBPROTOCOL
            } else if requested
                .split(',')
                .map(str::trim)
                .any(|protocol| protocol == TEXT_SUBPROTOCOL)
            {
                TEXT_SUBPROTOCOL
            } else if !requested.is_empty() {
                return Err(rejection_response(
                    StatusCode::NOT_IMPLEMENTED,
                    "requested WebSocket subprotocol is not supported by the legacy session frontend",
                ));
            } else {
                unreachable!("empty WebSocket protocol list handled above")
            };
            response
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", HeaderValue::from_static(protocol));
            Ok(response)
        };
        let mut websocket_config = WebSocketConfig::default();
        websocket_config.max_message_size = Some(max_frame);
        websocket_config.max_frame_size = Some(max_frame);
        let websocket = accept_hdr_async_with_config(stream, callback, Some(websocket_config))
            .await
            .map_err(|error| BrokerError::with_source("WebSocket handshake failed", error))?;
        let Some(permit) = self.quotas.try_client() else {
            return Err(BrokerError::msg("connection quota exceeded"));
        };
        self.metrics
            .websocket_connections
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .websocket_connections_total
            .fetch_add(1, Ordering::Relaxed);

        let (websocket_write, websocket_read) = websocket.split();
        let (client_stream, broker_stream) = tokio::io::duplex(BRIDGE_CAPACITY);
        let (broker_read, broker_write) = tokio::io::split(broker_stream);
        let (control_tx, control_rx) = mpsc::channel(8);
        let inbound = tokio::spawn(websocket_to_broker(
            websocket_read,
            broker_write,
            max_frame,
            control_tx,
            self.metrics.clone(),
        ));
        let outbound = tokio::spawn(broker_to_websocket(
            websocket_write,
            broker_read,
            control_rx,
            binary_mode.load(Ordering::Relaxed),
            max_frame,
            self.metrics.clone(),
        ));

        let result = self
            .handle_client_with_remote_addr(client_stream, Some(peer))
            .await;
        inbound.abort();
        outbound.abort();
        self.metrics
            .websocket_connections
            .fetch_sub(1, Ordering::Relaxed);
        if result.is_err() {
            self.metrics
                .websocket_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(permit);
        result
    }
}

trait WebSocketIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T> WebSocketIo for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

fn validate_origin(
    request: &Request,
    allowed_origins: &[String],
) -> std::result::Result<Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse>
{
    let Some(origin) = request.headers().get("Origin") else {
        return Ok(Response::default());
    };
    let origin = origin
        .to_str()
        .map_err(|_| rejection_response(StatusCode::FORBIDDEN, "invalid Origin header"))?;
    if allowed_origins.iter().any(|allowed| allowed == origin) {
        Ok(Response::default())
    } else {
        Err(rejection_response(
            StatusCode::FORBIDDEN,
            "WebSocket origin is not allowed",
        ))
    }
}

fn rejection_response(
    status: StatusCode,
    message: &str,
) -> tokio_tungstenite::tungstenite::handshake::server::ErrorResponse {
    Response::builder()
        .status(status)
        .body(Some(message.to_string()))
        .expect("valid WebSocket rejection response")
}

async fn websocket_to_broker<R, W>(
    mut websocket: R,
    mut broker: W,
    max_frame: usize,
    control: mpsc::Sender<Message>,
    metrics: Arc<BrokerMetrics>,
) -> std::result::Result<(), WebSocketError>
where
    R: futures_util::Stream<Item = std::result::Result<Message, WebSocketError>> + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = websocket.next().await {
        match message? {
            Message::Text(text) => {
                if text.len() > max_frame {
                    return Err(WebSocketError::Capacity(CapacityError::MessageTooLong {
                        size: text.len(),
                        max_size: max_frame,
                    }));
                }
                broker
                    .write_all(text.as_bytes())
                    .await
                    .map_err(WebSocketError::Io)?;
                metrics
                    .websocket_messages_received_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .websocket_bytes_received_total
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
            }
            Message::Binary(bytes) => {
                if bytes.len() > max_frame {
                    return Err(WebSocketError::Capacity(CapacityError::MessageTooLong {
                        size: bytes.len(),
                        max_size: max_frame,
                    }));
                }
                broker.write_all(&bytes).await.map_err(WebSocketError::Io)?;
                metrics
                    .websocket_messages_received_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .websocket_bytes_received_total
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            }
            Message::Ping(payload) => {
                control
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| WebSocketError::ConnectionClosed)?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => break,
            Message::Frame(_) => {}
        }
    }
    broker.shutdown().await.map_err(WebSocketError::Io)
}

async fn broker_to_websocket<W, R>(
    mut websocket: W,
    mut broker: R,
    mut control: mpsc::Receiver<Message>,
    binary_mode: bool,
    max_frame: usize,
    metrics: Arc<BrokerMetrics>,
) -> std::result::Result<(), WebSocketError>
where
    W: futures_util::Sink<Message, Error = WebSocketError> + Unpin,
    R: tokio::io::AsyncRead + Unpin,
{
    let mut read_buffer = vec![0_u8; 16 * 1024];
    let mut frame_buffer = Vec::new();
    loop {
        tokio::select! {
            Some(message) = control.recv() => {
                websocket.send(message).await?;
            }
                read = broker.read(&mut read_buffer) => {
                let read = read.map_err(WebSocketError::Io)?;
                if read == 0 {
                    websocket.close().await?;
                    return Ok(());
                }
                if binary_mode {
                    frame_buffer.extend_from_slice(&read_buffer[..read]);
                    if frame_buffer.len() > 0 {
                        for frame in drain_broker_frames(&mut frame_buffer, max_frame)? {
                            let len = frame.len();
                            websocket.send(Message::binary(frame)).await?;
                            metrics.websocket_messages_sent_total.fetch_add(1, Ordering::Relaxed);
                            metrics.websocket_bytes_sent_total.fetch_add(len as u64, Ordering::Relaxed);
                        }
                    }
                } else {
                    let text = String::from_utf8_lossy(&read_buffer[..read]).into_owned();
                    websocket.send(Message::text(text)).await?;
                    metrics.websocket_messages_sent_total.fetch_add(1, Ordering::Relaxed);
                    metrics.websocket_bytes_sent_total.fetch_add(read as u64, Ordering::Relaxed);
                }
            }
        }
    }
}

fn drain_broker_frames(
    buffer: &mut Vec<u8>,
    max_frame: usize,
) -> std::result::Result<Vec<Vec<u8>>, WebSocketError> {
    if buffer.len() > max_frame {
        return Err(WebSocketError::Capacity(CapacityError::MessageTooLong {
            size: buffer.len(),
            max_size: max_frame,
        }));
    }
    let mut frames = Vec::new();
    while let Some(header_end) = buffer.windows(2).position(|window| window == b"\r\n") {
        let header = &buffer[..header_end];
        let frame_len = if header.starts_with(b"MSG ") {
            header
                .split(|byte| *byte == b' ')
                .next_back()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .and_then(|bytes| bytes.parse::<usize>().ok())
                .map(|payload_len| header_end + 2 + payload_len + 2)
        } else if header.starts_with(b"HMSG ") {
            header
                .split(|byte| *byte == b' ')
                .next_back()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .and_then(|bytes| bytes.parse::<usize>().ok())
                .map(|payload_len| header_end + 2 + payload_len + 2)
        } else {
            Some(header_end + 2)
        };
        let Some(frame_len) = frame_len else {
            return Err(WebSocketError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid broker frame length",
            )));
        };
        if frame_len > max_frame {
            return Err(WebSocketError::Capacity(CapacityError::MessageTooLong {
                size: frame_len,
                max_size: max_frame,
            }));
        }
        if buffer.len() < frame_len {
            break;
        }
        frames.push(buffer.drain(..frame_len).collect());
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_must_match_the_allowlist() {
        let request = Request::builder()
            .uri("ws://localhost/")
            .header("Origin", "https://example.com")
            .body(())
            .unwrap();
        assert!(validate_origin(&request, &["http://localhost:3000".into()]).is_err());
        assert!(validate_origin(&request, &["https://example.com".into()]).is_ok());
    }

    #[test]
    fn missing_origin_is_allowed_for_non_browser_clients() {
        let request = Request::builder().uri("ws://localhost/").body(()).unwrap();
        assert!(validate_origin(&request, &[]).is_ok());
    }

    #[test]
    fn binary_broker_frames_are_reassembled_without_lossy_conversion() {
        let mut buffer = b"MSG subject 1 5\r\nhel".to_vec();
        assert!(drain_broker_frames(&mut buffer, 64).unwrap().is_empty());
        buffer.extend_from_slice(b"lo\r\nMSG other 1 2\r\nok\r\n");
        assert_eq!(
            drain_broker_frames(&mut buffer, 64).unwrap(),
            vec![
                b"MSG subject 1 5\r\nhello\r\n".to_vec(),
                b"MSG other 1 2\r\nok\r\n".to_vec()
            ]
        );
        assert!(buffer.is_empty());
    }
}
