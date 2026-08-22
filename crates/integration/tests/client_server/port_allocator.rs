use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Mutex, OnceLock},
};
use tokio::net::TcpListener;

pub(super) async fn free_addr() -> SocketAddr {
    static RESERVED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    let reserved_ports = RESERVED_PORTS.get_or_init(|| Mutex::new(HashSet::new()));

    loop {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut reserved_ports = reserved_ports.lock().unwrap();
        if reserved_ports.insert(addr.port()) {
            return addr;
        }
    }
}
