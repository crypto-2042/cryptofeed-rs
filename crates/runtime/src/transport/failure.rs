use cryptofeed_core::error::Error;
use std::time::{Duration, SystemTime};

pub(crate) fn retry_after(header: Option<&str>, now: SystemTime) -> Option<Duration> {
    let value = header?.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value.parse::<u64>().ok().map(Duration::from_secs);
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|time| time.duration_since(now).unwrap_or_default())
}

pub(super) fn handshake(error: tokio_tungstenite::tungstenite::Error, proxied: bool) -> Error {
    if let tokio_tungstenite::tungstenite::Error::Http(response) = error {
        return Error::HttpStatus {
            status: response.status().as_u16(),
            retry_after: retry_after(
                response
                    .headers()
                    .get("retry-after")
                    .and_then(|value| value.to_str().ok()),
                SystemTime::now(),
            ),
        };
    }
    if proxied {
        Error::Transport("proxied WebSocket TLS/handshake failed".into())
    } else {
        Error::Transport(error.to_string())
    }
}

pub(super) fn proxy_retry_after(headers: &[u8]) -> Option<Duration> {
    let mut found = None;
    for line in headers.split(|byte| *byte == b'\n').skip(1) {
        let Some((name, value)) = line
            .iter()
            .position(|byte| *byte == b':')
            .map(|index| (&line[..index], &line[index + 1..]))
        else {
            continue;
        };
        if name.eq_ignore_ascii_case(b"retry-after") {
            if found.is_some() {
                return None;
            }
            found = Some(std::str::from_utf8(value).ok()?);
        }
    }
    retry_after(found, SystemTime::now())
}

#[cfg(test)]
mod ws_lifecycle_tests {
    use super::*;
    #[test]
    fn ws_lifecycle_handshake_status_and_hint_omit_untrusted_response_material() {
        for proxied in [true, false] {
            let response = http::Response::builder()
                .status(429)
                .header("Retry-After", "17")
                .header("set-cookie", "secret")
                .body(Some(b"private body".to_vec()))
                .unwrap();
            let error = handshake(
                tokio_tungstenite::tungstenite::Error::Http(Box::new(response)),
                proxied,
            );
            assert!(
                matches!(error,Error::HttpStatus{status:429,retry_after:Some(delay)} if delay==Duration::from_secs(17))
            );
            assert!(!error.to_string().contains("secret"));
            assert!(!error.to_string().contains("private"));
        }
    }
    #[test]
    fn ws_lifecycle_retry_after_is_unsigned_seconds_or_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        for (hint, expected) in [
            ("0", Some(0)),
            ("17", Some(17)),
            ("+17", None),
            ("-1", None),
            ("1.5", None),
            ("", None),
        ] {
            assert_eq!(
                retry_after(Some(hint), now),
                expected.map(Duration::from_secs)
            );
        }
        assert_eq!(
            retry_after(Some("Thu, 01 Jan 1970 00:00:20 GMT"), now),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            retry_after(Some("Thu, 01 Jan 1970 00:00:05 GMT"), now),
            Some(Duration::ZERO)
        );
    }
    #[test]
    fn ws_lifecycle_connect_proxy_hints_are_case_insensitive_and_duplicates_are_ignored() {
        assert_eq!(
            proxy_retry_after(b"HTTP/1.1 503 unavailable\r\nrEtRy-AfTeR: 17\r\n\r\n"),
            Some(Duration::from_secs(17))
        );
        for input in [
            b"HTTP/1.1 503 unavailable\r\nRetry-After: 17\r\nretry-after: 20\r\n\r\n".as_slice(),
            b"HTTP/1.1 503 unavailable\r\nRetry-After: invalid\r\n\r\n",
        ] {
            assert_eq!(proxy_retry_after(input), None);
        }
    }
    #[tokio::test]
    async fn ws_lifecycle_real_http_upgrade_rejection_keeps_status_for_direct_and_proxy_routes() {
        use crate::TransportConfig;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        async fn headers(stream: &mut tokio::net::TcpStream) {
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                bytes.push(stream.read_u8().await.unwrap());
                assert!(bytes.len() < 16384);
            }
        }
        for (proxied, reject_connect) in [(false, false), (true, false), (true, true)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                headers(&mut stream).await;
                if proxied && !reject_connect {
                    stream
                        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        .await
                        .unwrap();
                    headers(&mut stream).await;
                }
                stream.write_all(b"HTTP/1.1 429 untrusted-secret\r\nRetry-After: 17\r\nSet-Cookie: private-secret\r\nContent-Length: 0\r\n\r\n").await.unwrap();
            });
            let (transport, url) = if proxied {
                (
                    TransportConfig::http_proxy(&format!("http://{address}"))
                        .unwrap()
                        .basic_auth("user", "secret")
                        .unwrap(),
                    url::Url::parse("ws://target.invalid/public").unwrap(),
                )
            } else {
                (
                    TransportConfig::direct(),
                    url::Url::parse(&format!("ws://{address}/public")).unwrap(),
                )
            };
            let error = tokio::time::timeout(
                Duration::from_secs(2),
                transport.websocket(&url, Default::default()),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert!(
                matches!(error,Error::HttpStatus{status:429,retry_after:Some(delay)} if delay==Duration::from_secs(17))
            );
            assert!(!error.to_string().contains("secret"));
            server.await.unwrap();
        }
    }
}
