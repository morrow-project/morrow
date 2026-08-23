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

pub(super) fn http_query_parameter<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|parameter| {
        let (key, value) = parameter.split_once('=')?;
        (key == name).then_some(value)
    })
}

pub(super) fn http_authorized(request: &[u8], token: &str) -> bool {
    let Ok(request) = std::str::from_utf8(request) else {
        return false;
    };
    let expected = format!("Bearer {token}");
    request
        .split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .any(|(name, value)| {
            name.eq_ignore_ascii_case("authorization")
                && crate::security::constant_time_eq(value.trim(), &expected)
        })
}

pub(super) async fn write_http_unauthorized<W>(writer: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let body = br#"{"error":"unauthorized"}"#;
    let header = format!(
        "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nwww-authenticate: Bearer\r\nconnection: close\r\n\r\n",
        body.len()
    );
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(body).await?;
    Ok(())
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

pub(super) async fn write_http_text_response<W>(
    writer: &mut W,
    status: &str,
    body: &str,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_http_response(writer, status, "text/plain; version=0.0.4", body.as_bytes()).await
}
