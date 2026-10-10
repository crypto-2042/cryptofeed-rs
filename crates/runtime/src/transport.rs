//! Explicit shared HTTP/WebSocket routing. Proxy credentials are never URL data.
use base64::Engine;
use cryptofeed_core::error::{Error, Result};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::protocol::WebSocketConfig};
use url::Url;

#[derive(Clone, Default)]
pub struct TransportConfig(Option<Arc<HttpProxy>>);
struct HttpProxy {
    endpoint: Url,
    authorization: Option<reqwest::header::HeaderValue>,
    client: reqwest::Client,
    id: u64,
}
impl std::fmt::Debug for TransportConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(if self.0.is_some() {
            "TransportConfig(HTTP proxy; credentials hidden)"
        } else {
            "TransportConfig(direct)"
        })
    }
}
impl TransportConfig {
    pub fn direct() -> Self {
        Self::default()
    }
    /// HTTP proxy endpoint with no credentials, query, fragment or non-root path.
    pub fn http_proxy(endpoint: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .map_err(|_| Error::InvalidConfiguration("invalid HTTP proxy endpoint".into()))?;
        if endpoint.scheme() != "http"
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
        {
            return Err(Error::InvalidConfiguration(
                "proxy requires http://host:port without credentials, path, query or fragment"
                    .into(),
            ));
        }
        Self::build(endpoint, None)
    }
    /// Rebuilds this proxy configuration with Basic authentication. Clones of
    /// the returned configuration reuse its client and isolated catalog cache.
    pub fn basic_auth(self, username: &str, password: &str) -> Result<Self> {
        let proxy = self.0.as_ref().ok_or_else(|| {
            Error::InvalidConfiguration("proxy authentication requires an HTTP proxy".into())
        })?;
        if username.contains(':') {
            return Err(Error::InvalidConfiguration(
                "Basic proxy username cannot contain a colon".into(),
            ));
        }
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        let mut header = reqwest::header::HeaderValue::from_str(&format!("Basic {encoded}"))
            .map_err(|_| Error::InvalidConfiguration("invalid proxy authentication".into()))?;
        header.set_sensitive(true);
        Self::build(proxy.endpoint.clone(), Some(header))
    }
    fn build(endpoint: Url, authorization: Option<reqwest::header::HeaderValue>) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let mut proxy = reqwest::Proxy::all(endpoint.as_str())
            .map_err(|_| Error::InvalidConfiguration("invalid HTTP proxy endpoint".into()))?;
        if let Some(header) = &authorization {
            proxy = proxy.custom_http_auth(header.clone());
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .proxy(proxy)
            .build()
            .map_err(|_| Error::Transport("could not build proxy HTTP client".into()))?;
        Ok(Self(Some(Arc::new(HttpProxy {
            endpoint,
            authorization,
            client,
            id: NEXT.fetch_add(1, Ordering::Relaxed),
        }))))
    }
    pub(crate) fn cache_key(&self, url: &str) -> String {
        match &self.0 {
            Some(proxy) => format!("proxy-{}:{url}", proxy.id),
            None => url.to_owned(),
        }
    }
    pub(crate) fn http_client(&self) -> Result<&reqwest::Client> {
        if let Some(proxy) = &self.0 {
            return Ok(&proxy.client);
        }
        static CLIENT: OnceLock<std::result::Result<reqwest::Client, String>> = OnceLock::new();
        CLIENT
            .get_or_init(|| {
                reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .map_err(|error| error.to_string())
            })
            .as_ref()
            .map_err(|error| Error::Transport(error.clone()))
    }
    pub(crate) async fn websocket(
        &self,
        url: &Url,
        config: WebSocketConfig,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let Some(proxy) = &self.0 else {
            return tokio_tungstenite::connect_async_with_config(url.as_str(), Some(config), false)
                .await
                .map(|(stream, _)| stream)
                .map_err(|error| Error::Transport(error.to_string()));
        };
        let host = socket_host(&proxy.endpoint)?;
        let port = proxy
            .endpoint
            .port_or_known_default()
            .expect("validated HTTP proxy port");
        let mut stream = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|_| Error::Transport("HTTP proxy TCP connection failed".into()))?;
        stream
            .set_nodelay(true)
            .map_err(|_| Error::Transport("HTTP proxy TCP configuration failed".into()))?;
        tunnel(&mut stream, url, proxy.authorization.as_ref()).await?;
        tokio_tungstenite::client_async_tls_with_config(url.as_str(), stream, Some(config), None)
            .await
            .map(|(stream, _)| stream)
            .map_err(|_| Error::Transport("proxied WebSocket TLS/handshake failed".into()))
    }
}
fn socket_host(url: &Url) -> Result<String> {
    match url.host() {
        Some(url::Host::Ipv6(ip)) => Ok(ip.to_string()),
        Some(host) => Ok(host.to_string()),
        None => Err(Error::InvalidConfiguration(
            "connection URL has no host".into(),
        )),
    }
}
async fn tunnel<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    target: &Url,
    authorization: Option<&reqwest::header::HeaderValue>,
) -> Result<()> {
    let host = match target.host() {
        Some(url::Host::Ipv6(ip)) => format!("[{ip}]"),
        Some(host) => host.to_string(),
        None => {
            return Err(Error::InvalidConfiguration(
                "WebSocket URL has no host".into(),
            ));
        }
    };
    let port = target
        .port_or_known_default()
        .ok_or_else(|| Error::InvalidConfiguration("WebSocket URL has no port".into()))?;
    let authority = format!("{host}:{port}");
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(header) = authorization {
        request.push_str("Proxy-Authorization: ");
        request.push_str(header.to_str().expect("generated Basic header"));
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|_| Error::Transport("HTTP proxy CONNECT write failed".into()))?;
    // Read exactly through the header terminator, leaving tunnel/TLS bytes intact.
    let mut total = 0;
    loop {
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            if total >= 16384 {
                return Err(Error::Transport(
                    "HTTP proxy CONNECT headers exceed 16384 bytes".into(),
                ));
            }
            let byte = stream
                .read_u8()
                .await
                .map_err(|_| Error::Transport("HTTP proxy CONNECT response incomplete".into()))?;
            headers.push(byte);
            total += 1;
        }
        let line = headers
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap_or_default();
        let line = std::str::from_utf8(line)
            .map_err(|_| Error::Transport("invalid HTTP proxy CONNECT status".into()))?;
        let mut fields = line.split_whitespace();
        let version = fields.next();
        let status = fields
            .next()
            .filter(|code| code.len() == 3)
            .and_then(|code| code.parse::<u16>().ok());
        if !matches!(version, Some("HTTP/1.0" | "HTTP/1.1")) {
            return Err(Error::Transport("invalid HTTP proxy CONNECT status".into()));
        }
        match status {
            Some(200..=299) => return Ok(()),
            Some(100..=199) => continue,
            Some(407) => {
                return Err(Error::InvalidConfiguration(
                    "HTTP proxy authentication rejected with status 407".into(),
                ));
            }
            Some(status) => {
                return Err(Error::Transport(format!(
                    "HTTP proxy CONNECT rejected with status {status}"
                )));
            }
            None => return Err(Error::Transport("invalid HTTP proxy CONNECT status".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_urls_and_debug_do_not_expose_credentials() {
        for url in [
            "http://private:secret@localhost:80",
            "socks5://localhost:80",
            "https://localhost:80",
            "http://localhost/path",
            "http://localhost/?secret",
            "bad secret",
        ] {
            let error = TransportConfig::http_proxy(url).unwrap_err().to_string();
            assert!(!error.contains("secret"));
            assert!(!error.contains("private"));
        }
        let config = TransportConfig::http_proxy("http://localhost:8080")
            .unwrap()
            .basic_auth("private", "secret")
            .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("private"));
        assert!(!debug.contains("secret"));
        assert!(
            config
                .0
                .as_ref()
                .unwrap()
                .authorization
                .as_ref()
                .unwrap()
                .is_sensitive()
        );
        assert!(
            TransportConfig::direct()
                .basic_auth("private", "secret")
                .is_err()
        );
    }
    #[test]
    fn cloned_routes_share_clients_and_cache_identity_but_separate_routes_do_not() {
        let first = TransportConfig::http_proxy("http://localhost:8080").unwrap();
        let clone = first.clone();
        let separate = TransportConfig::http_proxy("http://localhost:8080").unwrap();
        assert_eq!(
            first.cache_key("https://exchange/catalog"),
            clone.cache_key("https://exchange/catalog")
        );
        assert_ne!(
            first.cache_key("https://exchange/catalog"),
            separate.cache_key("https://exchange/catalog")
        );
        assert!(std::ptr::eq(
            first.http_client().unwrap(),
            clone.http_client().unwrap()
        ));
        assert_eq!(
            TransportConfig::direct().cache_key("https://exchange/catalog"),
            "https://exchange/catalog"
        );
    }
    #[tokio::test]
    async fn connect_auth_ipv6_interim_and_tunnel_bytes_are_preserved() {
        let config = TransportConfig::http_proxy("http://localhost:80")
            .unwrap()
            .basic_auth("user", "pass")
            .unwrap();
        let (mut client, mut server) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(server.read_u8().await.unwrap());
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("CONNECT [::1]:8443 HTTP/1.1\r\nHost: [::1]:8443\r\n"));
            assert!(request.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"));
            server
                .write_all(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Connected\r\n\r\nTLS!")
                .await
                .unwrap();
        });
        tunnel(
            &mut client,
            &Url::parse("wss://[::1]:8443/public").unwrap(),
            config.0.as_ref().unwrap().authorization.as_ref(),
        )
        .await
        .unwrap();
        let mut next = [0; 4];
        client.read_exact(&mut next).await.unwrap();
        assert_eq!(&next, b"TLS!");
        task.await.unwrap();
    }
    #[tokio::test]
    async fn connect_errors_omit_untrusted_status_reasons_and_bodies() {
        for response in [
            "HTTP/1.1 407 secret password\r\n\r\nsecret body",
            "HTTP/1.1 503 secret\r\n\r\n",
            "NOTHTTP secret\r\n\r\n",
        ] {
            let (mut client, mut server) = tokio::io::duplex(1024);
            let task = tokio::spawn(async move {
                server.write_all(response.as_bytes()).await.unwrap();
            });
            let error = tunnel(
                &mut client,
                &Url::parse("wss://example.invalid").unwrap(),
                None,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(!error.contains("secret"));
            assert!(!error.contains("password"));
            if response.starts_with("HTTP/1.1 407") {
                assert!(error.contains("407"));
            }
            if response.starts_with("HTTP/1.1 503") {
                assert!(error.contains("503"));
            }
            task.await.unwrap();
        }
    }
    #[tokio::test]
    async fn connect_headers_are_bounded_and_incomplete_response_fails() {
        for response in [b"HTTP/1.1 200\r\nIncomplete".to_vec(), vec![b'x'; 16385]] {
            let (mut client, mut server) = tokio::io::duplex(32768);
            let task = tokio::spawn(async move {
                server.write_all(&response).await.unwrap();
            });
            assert!(
                tunnel(
                    &mut client,
                    &Url::parse("wss://example.invalid").unwrap(),
                    None
                )
                .await
                .is_err()
            );
            task.await.unwrap();
        }
    }
}
