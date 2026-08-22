use ed25519_dalek::{Signer, SigningKey};
pub use protocol;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use std::{
    collections::VecDeque, error::Error, fmt, net::SocketAddr, path::Path, path::PathBuf,
    sync::Arc, time::Duration,
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
    durable: bool,
    push_credit_messages: usize,
    pending_messages: VecDeque<Message>,
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
    pub proto: u32,
    pub protocol_versions: Vec<u32>,
    pub auth_required: bool,
    pub nonce: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub subject: String,
    pub sid: String,
    pub reply_to: Option<String>,
    pub ack_subject: Option<String>,
    pub key: Option<Vec<u8>>,
    pub timestamp_ms: Option<u64>,
    pub headers: Vec<(String, String)>,
    pub payload: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerAck {
    pub msg_id: String,
    pub level: protocol::AckLevel,
    pub retained: bool,
    pub seq: Option<u64>,
    pub stream: Option<String>,
    pub partition: Option<u32>,
    pub offset: Option<u64>,
    pub partitioning_epoch: Option<u64>,
    pub leader_epoch: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableMessage {
    pub consumer: String,
    pub subject: String,
    pub reply_to: Option<String>,
    pub headers: Vec<(String, String)>,
    pub stream: String,
    pub partition: u32,
    pub offset: u64,
    pub key: Option<Vec<u8>>,
    pub timestamp_ms: u64,
    pub attempt: u32,
    pub lease_deadline_ms: u64,
    pub seq: u64,
    pub delivery_id: u64,
    pub payload: Vec<u8>,
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
    ConsumerOk {
        operation: String,
        name: String,
    },
    DeliveryControlOk {
        operation: String,
        name: String,
        seq: u64,
        delivery_id: u64,
    },
    Batch {
        name: String,
        messages: usize,
        bytes: usize,
    },
    DurableMessage(DurableMessage),
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
#[path = "client/pull.rs"]
mod pull;
use self::{frame_parser::*, helpers::*};

#[cfg(test)]
mod tests;
