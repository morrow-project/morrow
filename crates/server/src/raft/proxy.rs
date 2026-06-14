use super::*;

pub(crate) async fn proxy_stream_to_leader<S>(mut inbound: S, leader: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut outbound = TcpStream::connect(leader)
        .await
        .with_context(|| format!("connecting to leader {leader}"))?;
    tokio::io::copy_bidirectional(&mut inbound, &mut outbound)
        .await
        .context("proxying client connection to leader")?;
    Ok(())
}

pub async fn proxy_to_leader(inbound: TcpStream, leader: SocketAddr) -> Result<()> {
    proxy_stream_to_leader(inbound, leader).await
}
