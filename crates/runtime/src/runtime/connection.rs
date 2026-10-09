use crate::exchange::{bybit::adapter::BybitAdapter, okx::adapter::OkxAdapter};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::{fmt::Display, time::Duration};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use url::Url;

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeartbeatKind {
    None,
    Text(&'static str),
    Gate(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub struct HeartbeatPolicy {
    kind: HeartbeatKind,
    interval: Duration,
    idle_timeout: Duration,
}

impl HeartbeatPolicy {
    pub fn for_exchange(exchange: ExchangeId, url: &Url) -> Self {
        let (kind, interval, idle_timeout) = match exchange {
            // Binance sends a server ping frame only every 3 minutes and the
            // client sends no application heartbeat, so the idle timeout must
            // comfortably exceed that cadence — a quiet symbol (no trades, no
            // bookTicker change) would otherwise be killed as idle and the
            // feed would terminally fail after the retries.
            ExchangeId::Binance => (
                HeartbeatKind::None,
                Duration::from_secs(60),
                Duration::from_secs(240),
            ),
            ExchangeId::Bitget => (
                HeartbeatKind::Text("ping"),
                Duration::from_secs(30),
                Duration::from_secs(60),
            ),
            ExchangeId::Bybit => (
                HeartbeatKind::Text(BybitAdapter::heartbeat_message()),
                Duration::from_secs(BybitAdapter::HEARTBEAT_INTERVAL_SECS),
                Duration::from_secs(45),
            ),
            ExchangeId::Okx => (
                HeartbeatKind::Text(OkxAdapter::heartbeat_message()),
                Duration::from_secs(20),
                Duration::from_secs(OkxAdapter::IDLE_TIMEOUT_SECS),
            ),
            ExchangeId::Gateio => {
                let channel = if url.host_str().is_some_and(|host| host.contains("fx-ws")) {
                    "futures.ping"
                } else {
                    "spot.ping"
                };
                (
                    HeartbeatKind::Gate(channel),
                    Duration::from_secs(20),
                    Duration::from_secs(45),
                )
            }
            ExchangeId::Coinbase | ExchangeId::Kraken => (
                HeartbeatKind::None,
                Duration::from_secs(60),
                DEFAULT_IDLE_TIMEOUT,
            ),
            // A future exchange gets the conservative default (no heartbeat,
            // idle only) until its own policy is verified.
            _ => (
                HeartbeatKind::None,
                Duration::from_secs(60),
                DEFAULT_IDLE_TIMEOUT,
            ),
        };
        Self {
            kind,
            interval,
            idle_timeout,
        }
    }

    pub fn message(self, unix_seconds: u64) -> Option<String> {
        match self.kind {
            HeartbeatKind::None => None,
            HeartbeatKind::Text(text) => Some(text.to_owned()),
            HeartbeatKind::Gate(channel) => Some(format!(
                r#"{{"time":{unix_seconds},"channel":"{channel}"}}"#
            )),
        }
    }

    pub fn is_response(self, text: &str) -> bool {
        if matches!(self.kind, HeartbeatKind::Text("ping")) && text == "pong" {
            return true;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
            return false;
        };
        value.get("op").and_then(|value| value.as_str()) == Some("pong")
            || value.get("ret_msg").and_then(|value| value.as_str()) == Some("pong")
            || value
                .get("channel")
                .and_then(|value| value.as_str())
                .is_some_and(|channel| channel.ends_with(".pong"))
    }

    #[cfg(test)]
    fn for_test(payload: Option<&'static str>, interval: Duration, idle_timeout: Duration) -> Self {
        Self {
            kind: payload.map_or(HeartbeatKind::None, HeartbeatKind::Text),
            interval,
            idle_timeout,
        }
    }
}

pub struct WsConnection {
    pub url: Url,
    heartbeat: HeartbeatPolicy,
    exchange: ExchangeId,
}

impl WsConnection {
    pub fn new(url: Url, exchange: ExchangeId) -> Self {
        let heartbeat = HeartbeatPolicy::for_exchange(exchange, &url);
        Self {
            url,
            heartbeat,
            exchange,
        }
    }

    /// WebSocket handshake timeout: a blackholed endpoint must fail the
    /// connect (and reach the backoff) instead of stalling the task forever.
    const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
    const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
    const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

    pub async fn connect(&self) -> Result<Session<WebSocketStream<MaybeTlsStream<TcpStream>>>> {
        let slot = super::budget::acquire(self.exchange).await?;
        let config = WebSocketConfig {
            max_write_buffer_size: 1024 * 1024,
            max_message_size: Some(Self::MAX_MESSAGE_BYTES),
            max_frame_size: Some(Self::MAX_FRAME_BYTES),
            ..WebSocketConfig::default()
        };
        let (stream, _) = tokio::time::timeout(
            Self::CONNECT_TIMEOUT,
            connect_async_with_config(self.url.as_str(), Some(config), false),
        )
        .await
        .map_err(|_| Error::Transport(format!("websocket connect timed out: {}", self.url)))?
        .map_err(|error| Error::Transport(error.to_string()))?;
        let mut session = Session::new(stream, self.heartbeat);
        session._connection_slot = Some(slot);
        Ok(session)
    }
}

pub struct Session<S> {
    stream: S,
    heartbeat: HeartbeatPolicy,
    last_received: Instant,
    next_heartbeat: Option<Instant>,
    pending_subscriptions: std::collections::VecDeque<String>,
    _connection_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    next_subscription: Instant,
}

impl<S> Session<S> {
    /// Queues paced subscriptions without blocking market reads or heartbeat.
    /// The caller must bound the batch using the connection planner first.
    pub fn queue_subscriptions(&mut self, messages: impl IntoIterator<Item = String>) {
        self.pending_subscriptions.extend(messages);
    }

    pub fn new(stream: S, heartbeat: HeartbeatPolicy) -> Self {
        let now = Instant::now();
        let next_heartbeat = heartbeat
            .message(0)
            .is_some()
            .then_some(now + heartbeat.interval);
        Self {
            stream,
            heartbeat,
            last_received: now,
            next_heartbeat,
            pending_subscriptions: std::collections::VecDeque::new(),
            _connection_slot: None,
            next_subscription: now,
        }
    }
}

impl<S, E> Session<S>
where
    S: Sink<Message, Error = E> + Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: Display,
{
    /// Sends bound by [`Self::SEND_TIMEOUT`] so a stalled socket (full TCP
    /// send buffer) cannot hold the session task open past the shutdown
    /// grace period.
    const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    pub async fn send_text(&mut self, text: &str) -> Result<()> {
        tokio::time::timeout(
            Self::SEND_TIMEOUT,
            self.stream.send(Message::Text(text.to_owned())),
        )
        .await
        .map_err(|_| Error::Transport("websocket send timed out".to_owned()))?
        .map_err(|error| Error::Transport(error.to_string()))
    }

    async fn close(&mut self) {
        let _ =
            tokio::time::timeout(Self::SEND_TIMEOUT, self.stream.send(Message::Close(None))).await;
    }

    pub async fn next_text_or_shutdown(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<String>> {
        loop {
            if *shutdown.borrow() {
                self.close().await;
                return Ok(None);
            }

            let idle_deadline = self.last_received + self.heartbeat.idle_timeout;
            let heartbeat_deadline = self.next_heartbeat.unwrap_or(idle_deadline);
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    match changed {
                        Ok(()) if !*shutdown.borrow() => continue,
                        Ok(()) | Err(_) => {
                            self.close().await;
                            return Ok(None);
                        }
                    }
                }
                _ = tokio::time::sleep_until(idle_deadline) => {
                    return Err(Error::Transport("websocket idle timeout".to_owned()));
                }
                _ = tokio::time::sleep_until(heartbeat_deadline), if self.next_heartbeat.is_some() => {
                    let unix_seconds = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |duration| duration.as_secs());
                    if let Some(message) = self.heartbeat.message(unix_seconds) {
                        self.send_text(&message).await?;
                    }
                    self.next_heartbeat = Some(Instant::now() + self.heartbeat.interval);
                }
                _ = tokio::time::sleep_until(self.next_subscription), if !self.pending_subscriptions.is_empty() => {
                    let mut message = self.pending_subscriptions.pop_front().expect("pending subscription");
                    if matches!(self.heartbeat.kind, HeartbeatKind::Gate(_)) {
                        let mut payload: serde_json::Value = serde_json::from_str(&message)
                            .map_err(|error| Error::Parse(error.to_string()))?;
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |duration| duration.as_secs());
                        payload["time"] = serde_json::json!(now);
                        message = payload.to_string();
                    }
                    self.send_text(&message).await?;
                    self.next_subscription = Instant::now() + Duration::from_millis(250);
                }
                message = self.stream.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            self.last_received = Instant::now();
                            let text = text.to_string();
                            if self.heartbeat.is_response(&text) {
                                continue;
                            }
                            return Ok(Some(text));
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            self.last_received = Instant::now();
                            self.stream.send(Message::Pong(payload)).await
                                .map_err(|error| Error::Transport(error.to_string()))?;
                        }
                        Some(Ok(Message::Pong(_) | Message::Binary(_) | Message::Frame(_))) => {
                            self.last_received = Instant::now();
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            if *shutdown.borrow() {
                                return Ok(None);
                            }
                            return Err(Error::Transport("websocket closed by remote peer".to_owned()));
                        }
                        Some(Err(error)) => return Err(Error::Transport(error.to_string())),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HeartbeatPolicy, Session};
    use cryptofeed_core::{error::Error, exchange::ExchangeId};
    use futures::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio::io::duplex;
    use tokio::sync::watch;
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::Role},
    };
    use url::Url;

    async fn websocket_pair() -> (
        WebSocketStream<tokio::io::DuplexStream>,
        WebSocketStream<tokio::io::DuplexStream>,
    ) {
        let (client, server) = duplex(4096);
        let client = WebSocketStream::from_raw_socket(client, Role::Client, None);
        let server = WebSocketStream::from_raw_socket(server, Role::Server, None);
        tokio::join!(client, server)
    }

    #[tokio::test]
    async fn subscription_queue_is_paced_and_market_reads_continue() {
        let (client, mut server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(None, Duration::from_secs(30), Duration::from_secs(5)),
        );
        session.queue_subscriptions(["first".to_owned(), "second".to_owned()]);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            assert_eq!(
                session
                    .next_text_or_shutdown(&mut shutdown_rx)
                    .await
                    .unwrap(),
                Some("market data".to_owned())
            );
            seen_tx.send(()).unwrap();
            assert!(
                session
                    .next_text_or_shutdown(&mut shutdown_rx)
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        assert!(
            matches!(server.next().await.unwrap().unwrap(), Message::Text(text) if text == "first")
        );
        let first_at = tokio::time::Instant::now();
        server
            .send(Message::Text("market data".into()))
            .await
            .unwrap();
        seen_rx.await.unwrap();
        assert!(
            matches!(server.next().await.unwrap().unwrap(), Message::Text(text) if text == "second")
        );
        assert!(first_at.elapsed() >= Duration::from_millis(200));
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn queued_subscriptions_do_not_delay_heartbeat() {
        let (client, mut server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(
                Some("ping"),
                Duration::from_millis(10),
                Duration::from_secs(5),
            ),
        );
        session.queue_subscriptions(["later subscription".to_owned()]);
        session.next_subscription = tokio::time::Instant::now() + Duration::from_secs(60);
        let (_tx, mut rx) = watch::channel(false);
        let task = tokio::spawn(async move { session.next_text_or_shutdown(&mut rx).await });
        assert!(
            matches!(server.next().await.unwrap().unwrap(), Message::Text(text) if text == "ping")
        );
        server.send(Message::Text("market".into())).await.unwrap();
        assert_eq!(task.await.unwrap().unwrap(), Some("market".to_owned()));
    }

    #[tokio::test]
    async fn shutdown_discards_queued_subscriptions_before_sending() {
        let (client, mut server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(None, Duration::from_secs(30), Duration::from_secs(5)),
        );
        session.queue_subscriptions(["subscribe".to_owned()]);
        let (_tx, mut rx) = watch::channel(true);
        assert!(
            session
                .next_text_or_shutdown(&mut rx)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
    }

    #[tokio::test]
    async fn queued_gate_subscription_regenerates_timestamp_when_sent() {
        let (client, mut server) = websocket_pair().await;
        let url = Url::parse("wss://api.gateio.ws/ws/v4/").unwrap();
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_exchange(ExchangeId::Gateio, &url),
        );
        session.queue_subscriptions([
            r#"{"time":0,"channel":"spot.trades","event":"subscribe","payload":["BTC_USDT"]}"#
                .to_owned(),
        ]);
        let (tx, mut rx) = watch::channel(false);
        let task = tokio::spawn(async move { session.next_text_or_shutdown(&mut rx).await });
        let Message::Text(text) = server.next().await.unwrap().unwrap() else {
            panic!("subscription expected");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(value["time"].as_u64().unwrap() > 0);
        assert_eq!(value["payload"], serde_json::json!(["BTC_USDT"]));
        tx.send(true).unwrap();
        assert!(task.await.unwrap().unwrap().is_none());
    }

    #[test]
    fn heartbeat_policy_uses_exchange_application_payloads() {
        let url = Url::parse("wss://example.test/ws").unwrap();
        assert_eq!(
            HeartbeatPolicy::for_exchange(ExchangeId::Bitget, &url).message(123),
            Some("ping".to_owned())
        );
        assert_eq!(
            HeartbeatPolicy::for_exchange(ExchangeId::Bybit, &url).message(123),
            Some(r#"{"op":"ping"}"#.to_owned())
        );
        assert_eq!(
            HeartbeatPolicy::for_exchange(ExchangeId::Okx, &url).message(123),
            Some("ping".to_owned())
        );
        assert_eq!(
            HeartbeatPolicy::for_exchange(ExchangeId::Gateio, &url).message(123),
            Some(r#"{"time":123,"channel":"spot.ping"}"#.to_owned())
        );
        assert_eq!(
            HeartbeatPolicy::for_exchange(ExchangeId::Binance, &url).message(123),
            None
        );
    }

    #[test]
    fn recognizes_application_pongs() {
        let url = Url::parse("wss://example.test/ws").unwrap();
        for exchange in [
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            let policy = HeartbeatPolicy::for_exchange(exchange, &url);
            let response = match exchange {
                ExchangeId::Bitget | ExchangeId::Okx => "pong",
                ExchangeId::Bybit => r#"{"op":"pong"}"#,
                ExchangeId::Gateio => r#"{"channel":"spot.pong","event":"update"}"#,
                _ => unreachable!(),
            };
            assert!(policy.is_response(response));
        }
    }

    #[tokio::test]
    async fn sends_heartbeat_and_suppresses_pong() {
        let (client, mut server) = websocket_pair().await;
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let policy = HeartbeatPolicy::for_test(
            Some("ping"),
            Duration::from_millis(5),
            Duration::from_millis(100),
        );
        let mut session = Session::new(client, policy);
        let server_task = tokio::spawn(async move {
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                Message::Text("ping".into())
            );
            server.send(Message::Text("pong".into())).await.unwrap();
            server
                .send(Message::Text("market-data".into()))
                .await
                .unwrap();
        });
        assert_eq!(
            session
                .next_text_or_shutdown(&mut shutdown_rx)
                .await
                .unwrap(),
            Some("market-data".to_owned())
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn silent_session_times_out() {
        let (client, _server) = websocket_pair().await;
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let policy =
            HeartbeatPolicy::for_test(None, Duration::from_secs(60), Duration::from_millis(5));
        let mut session = Session::new(client, policy);
        let error = session
            .next_text_or_shutdown(&mut shutdown_rx)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Transport(message) if message.contains("idle")));
    }

    #[tokio::test]
    async fn clean_remote_close_is_reconnectable_error() {
        let (client, mut server) = websocket_pair().await;
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let policy =
            HeartbeatPolicy::for_test(None, Duration::from_secs(60), Duration::from_secs(60));
        let mut session = Session::new(client, policy);
        server.send(Message::Close(None)).await.unwrap();
        let error = session
            .next_text_or_shutdown(&mut shutdown_rx)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Transport(message) if message.contains("closed")));
    }

    #[tokio::test]
    async fn transport_failure_is_reported() {
        let (client, server) = websocket_pair().await;
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let policy =
            HeartbeatPolicy::for_test(None, Duration::from_secs(60), Duration::from_secs(60));
        let mut session = Session::new(client, policy);
        drop(server);

        assert!(matches!(
            session.next_text_or_shutdown(&mut shutdown_rx).await,
            Err(Error::Transport(_))
        ));
    }

    #[tokio::test]
    async fn shutdown_sends_close_and_returns_cleanly() {
        let (client, mut server) = websocket_pair().await;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let policy =
            HeartbeatPolicy::for_test(None, Duration::from_secs(60), Duration::from_secs(60));
        let mut session = Session::new(client, policy);
        shutdown_tx.send(true).unwrap();
        assert_eq!(
            session
                .next_text_or_shutdown(&mut shutdown_rx)
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
    }
}
