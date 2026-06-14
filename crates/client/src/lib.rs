use ed25519_dalek::{Signer, SigningKey};
pub use protocol;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use std::{
    error::Error, fmt, fs::File, io::BufReader as StdBufReader, net::SocketAddr, path::Path,
    path::PathBuf, sync::Arc, time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;
pub struct Client {
    stream: BufReader<Box<dyn ClientStream>>,
    max_payload: usize,
    inbox_prefix: String,
    inbox_counter: u64,
}
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub addr: SocketAddr,
    pub max_payload: usize,
    pub tls: Option<ClientTlsOptions>,
    pub auth: Option<ClientAuth>,
    pub durable_id: Option<String>,
    pub verbose: bool,
    pub ack_timeout_ms: u64,
    pub max_in_flight: usize,
}
#[derive(Debug, Clone)]
pub struct ClientTlsOptions {
    pub server_name: String,
    pub ca_cert_file: PathBuf,
}
trait ClientStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> ClientStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub raw: String,
    pub auth_required: bool,
    pub nonce: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub subject: String,
    pub sid: String,
    pub reply_to: Option<String>,
    pub ack_subject: Option<String>,
    pub headers: Vec<(String, String)>,
    pub payload: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerAck {
    pub msg_id: String,
    pub level: protocol::AckLevel,
    pub retained: bool,
    pub seq: Option<u64>,
}
#[derive(Debug, Clone)]
pub struct ClientAuth {
    client_id: String,
    signing_key: SigningKey,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerFrame {
    Info(Info),
    Message(Message),
    ProducerAck(ProducerAck),
    Pong,
    Ok,
    Err(String),
}

#[path = "client/error.rs"]
mod error;
pub use error::ClientError;
use error::Result;
#[path = "client/auth.rs"]
mod auth;
#[path = "client/client.rs"]
mod client;
#[path = "client/frame_parser.rs"]
mod frame_parser;
#[path = "client/helpers.rs"]
mod helpers;
use self::{frame_parser::*, helpers::*};

#[cfg(test)]
mod tests;
