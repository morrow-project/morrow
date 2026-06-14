use super::*;

pub(super) fn decrement_remaining(remaining: &mut Option<usize>) -> Option<bool> {
    let remaining = remaining.as_mut()?;
    *remaining = remaining.saturating_sub(1);
    Some(*remaining == 0)
}

pub(super) fn http_request_path(request_line: &str) -> Option<&str> {
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    let version = parts.next()?;
    if parts.next().is_some() || method != "GET" || (version != "HTTP/1.1" && version != "HTTP/1.0")
    {
        return None;
    }
    Some(path)
}

pub(super) async fn write_http_not_found<W>(writer: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_http_response(
        writer,
        "404 Not Found",
        "application/json",
        br#"{"error":"not found"}"#,
    )
    .await
}

pub(super) async fn write_http_response<W>(
    writer: &mut W,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let header = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(body).await?;
    Ok(())
}
