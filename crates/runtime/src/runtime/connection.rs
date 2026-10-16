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
    MaybeTlsStream, WebSocketStream,
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
    exchange: Option<ExchangeId>,
    kind: HeartbeatKind,
    interval: Duration,
    idle_timeout: Duration,
}

impl HeartbeatPolicy {
    pub fn for_exchange(exchange: ExchangeId, url: &Url) -> Self {
        let (kind, interval, idle_timeout) = match exchange {
            // Binance futures ping every 3 minutes (Spot every 20 seconds).
            // Keep the shared idle watchdog above the quiet futures cadence.
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
            exchange: Some(exchange),
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
        self.is_response_value(&value)
    }

    fn is_response_value(self, value: &serde_json::Value) -> bool {
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
            exchange: None,
            kind: payload.map_or(HeartbeatKind::None, HeartbeatKind::Text),
            interval,
            idle_timeout,
        }
    }
}

pub struct WsConnection {
    #[cfg(feature = "recording")]
    raw_capture: Option<(
        crate::recording::raw::RawCaptureHandle,
        crate::recording::raw::RawFeedInfo,
    )>,
    pub url: Url,
    transport: crate::transport::TransportConfig,
    options: crate::options::RuntimeOptions,
    heartbeat: HeartbeatPolicy,
    exchange: ExchangeId,
}

impl WsConnection {
    pub fn new(url: Url, exchange: ExchangeId) -> Self {
        let heartbeat = HeartbeatPolicy::for_exchange(exchange, &url);
        Self {
            #[cfg(feature = "recording")]
            raw_capture: None,
            transport: Default::default(),
            options: Default::default(),
            url,
            heartbeat,
            exchange,
        }
    }

    pub(crate) fn capture_feed(self, feed: &crate::exchange::ExchangeFeed) -> Self {
        #[cfg(feature = "recording")]
        {
            let mut connection = self;
            connection.raw_capture = feed.raw_capture.as_ref().map(|capture| {
                (
                    capture.clone(),
                    crate::recording::raw::RawFeedInfo::from_feed(feed),
                )
            });
            connection
        }
        #[cfg(not(feature = "recording"))]
        {
            let _ = feed;
            self
        }
    }
    pub fn transport(mut self, transport: crate::transport::TransportConfig) -> Self {
        self.transport = transport;
        self
    }

    pub fn runtime_options(mut self, options: crate::options::RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    async fn handshake<T>(
        &self,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::time::timeout(self.options.connection_deadline(), future)
            .await
            .map_err(|_| Error::Transport(format!("websocket connect timed out: {}", self.url)))?
    }

    const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
    const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

    pub async fn connect(&self) -> Result<Session<WebSocketStream<MaybeTlsStream<TcpStream>>>> {
        self.transport.check_websocket_available()?;
        let scoped = self
            .transport
            .resource_budget()
            .map(|budget| budget.websocket(self.exchange));
        let slot = super::budget::acquire(self.exchange, scoped).await?;
        let config = WebSocketConfig::default()
            .max_write_buffer_size(1024 * 1024)
            .max_message_size(Some(Self::MAX_MESSAGE_BYTES))
            .max_frame_size(Some(Self::MAX_FRAME_BYTES));
        let stream = match self
            .handshake(self.transport.websocket(&self.url, config))
            .await
        {
            Ok(stream) => stream,
            Err(error) => {
                if let Some(delay) = super::quota::handshake_delay(&error) {
                    super::budget::defer(self.exchange, delay).await?;
                }
                return Err(error);
            }
        };
        let mut session = Session::new(stream, self.heartbeat).runtime_options(self.options);
        session.exchange = Some(self.exchange);
        session._connection_slot = Some(slot);
        #[cfg(feature = "recording")]
        {
            session.raw = self
                .raw_capture
                .as_ref()
                .and_then(|(capture, feed)| capture.session(feed.clone()));
        }
        Ok(session)
    }
}

pub(crate) enum SessionRead {
    Text(String),
    Poll,
    Closed,
}

pub struct Session<S> {
    #[cfg(feature = "recording")]
    raw: Option<crate::recording::raw::RawSession>,
    last_text_received_ts: f64,
    #[cfg(feature = "recording")]
    last_raw_sequence: Option<u64>,
    stream: S,
    exchange: Option<ExchangeId>,
    heartbeat: HeartbeatPolicy,
    last_received: Instant,
    idle_timeout: Option<Duration>,
    next_heartbeat: Option<Instant>,
    pending_subscriptions: std::collections::VecDeque<String>,
    _connection_slot: Option<super::budget::ConnectionSlots>,
    readiness: Option<super::readiness::ConnectionAttempt>,
    next_subscription: Instant,
    sends: super::send_budget::SendBudget,
    retry_progress: Option<std::sync::Arc<super::supervisor::RetryProgress>>,
}

impl<S> Session<S> {
    #[cfg(feature = "recording")]
    pub(crate) fn raw_link(&self) -> Option<crate::recording::raw::RawSessionLink> {
        self.raw.as_ref().map(|raw| raw.link())
    }
    pub(crate) fn processing(&self) {
        #[cfg(feature = "recording")]
        if let (Some(raw), Some(sequence)) = (&self.raw, self.last_raw_sequence) {
            raw.processing(sequence);
        }
    }
    pub(crate) fn received_timestamp(&self) -> f64 {
        self.last_text_received_ts
    }
    /// Sets the receipt watchdog; handshake/callback/retry settings apply at
    /// their owning runtime layers, not to this established session.
    pub fn runtime_options(mut self, options: crate::options::RuntimeOptions) -> Self {
        self.idle_timeout = match options.receipt_policy() {
            crate::options::IdlePolicy::ExchangeDefault => Some(self.heartbeat.idle_timeout),
            crate::options::IdlePolicy::After(timeout) => Some(timeout),
            crate::options::IdlePolicy::Disabled => None,
        };
        self
    }

    pub(crate) fn set_readiness(&mut self, readiness: Option<super::readiness::ConnectionAttempt>) {
        if let Some(readiness) = &readiness {
            readiness.connected();
        }
        self.readiness = readiness;
    }

    pub(crate) fn set_retry_progress(
        &mut self,
        progress: Option<std::sync::Arc<super::supervisor::RetryProgress>>,
    ) {
        self.retry_progress = progress;
    }
    fn subscribed(&self) {
        if self.pending_subscriptions.is_empty() {
            if let Some(progress) = &self.retry_progress {
                progress.subscribed();
            }
        }
    }

    pub(crate) fn record_result(&self, result: &Result<()>) {
        if let Some(readiness) = &self.readiness {
            match result {
                Err(error) => readiness.failed(error, "session"),
                Ok(()) => readiness.disconnected(None),
            }
        }
    }

    /// Queues paced subscriptions without blocking market reads or heartbeat.
    /// The caller must bound the batch using the connection planner first.
    pub fn queue_subscriptions(&mut self, messages: impl IntoIterator<Item = String>) {
        let messages: Vec<_> = messages.into_iter().collect();
        let messages = match self.readiness.as_mut() {
            Some(readiness) => readiness.prepare_messages(messages),
            None => messages,
        };
        self.pending_subscriptions.extend(messages);
    }

    pub fn new(stream: S, heartbeat: HeartbeatPolicy) -> Self {
        let now = Instant::now();
        let next_heartbeat = heartbeat
            .message(0)
            .is_some()
            .then_some(now + heartbeat.interval);
        Self {
            #[cfg(feature = "recording")]
            raw: None,
            last_text_received_ts: 0.0,
            #[cfg(feature = "recording")]
            last_raw_sequence: None,
            stream,
            exchange: None,
            heartbeat,
            last_received: now,
            idle_timeout: Some(heartbeat.idle_timeout),
            next_heartbeat,
            pending_subscriptions: std::collections::VecDeque::new(),
            _connection_slot: None,
            readiness: None,
            next_subscription: now,
            sends: super::send_budget::SendBudget::new(heartbeat.exchange),
            retry_progress: None,
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
        let control = super::send_budget::is_control(text);
        tokio::time::sleep_until(self.sends.text_ready(Instant::now(), control)).await;
        self.sends.text_started(Instant::now(), control);
        let result = tokio::time::timeout(
            Self::SEND_TIMEOUT,
            self.stream.send(Message::Text(text.to_owned().into())),
        )
        .await
        .map_err(|_| Error::Transport("websocket send timed out".to_owned()))?
        .map_err(|error| Error::Transport(error.to_string()));
        #[cfg(feature = "recording")]
        if result.is_ok() {
            if let Some(raw) = &self.raw {
                raw.text(
                    text,
                    serde_json::from_str::<serde_json::Value>(text)
                        .ok()
                        .as_ref(),
                    false,
                    super::current_timestamp(),
                );
            }
        }
        result
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
            match self.next_event_or_shutdown(shutdown, None).await? {
                SessionRead::Text(text) => return Ok(Some(text)),
                SessionRead::Closed => return Ok(None),
                SessionRead::Poll => {}
            }
        }
    }

    /// Poll inside the session's read select, never by cancelling a whole
    /// heartbeat/subscription/Pong write future from an outer timer.
    pub(crate) async fn next_event_or_shutdown(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
        poll_at: Option<Instant>,
    ) -> Result<SessionRead> {
        let result = self.next_text_inner(shutdown, poll_at).await;
        #[cfg(feature = "recording")]
        if matches!(result, Err(_) | Ok(SessionRead::Closed)) {
            if let Some(raw) = &mut self.raw {
                raw.close(matches!(result, Ok(SessionRead::Closed)));
            }
        }
        if matches!(result, Ok(SessionRead::Text(_))) {
            self.subscribed();
        }
        if let Some(readiness) = &self.readiness {
            match &result {
                Err(error) => readiness.disconnected(Some(error.to_string())),
                Ok(SessionRead::Closed) => readiness.disconnected(None),
                _ => {}
            }
        }
        result
    }

    async fn next_text_inner(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
        poll_at: Option<Instant>,
    ) -> Result<SessionRead> {
        loop {
            if *shutdown.borrow() {
                self.close().await;
                return Ok(SessionRead::Closed);
            }

            let idle_deadline = self
                .idle_timeout
                .map(|timeout| self.last_received + timeout);
            let now = Instant::now();
            let heartbeat_deadline = self
                .next_heartbeat
                .unwrap_or(now)
                .max(self.sends.text_ready(now, false));
            let subscription_control = self
                .pending_subscriptions
                .front()
                .is_some_and(|message| super::send_budget::is_control(message));
            let subscription_deadline = self
                .next_subscription
                .max(self.sends.text_ready(now, subscription_control));
            let acknowledgement_deadline = self
                .readiness
                .as_ref()
                .and_then(|readiness| readiness.deadline());
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    match changed {
                        Ok(()) if !*shutdown.borrow() => continue,
                        Ok(()) | Err(_) => {
                            self.close().await;
                            return Ok(SessionRead::Closed);
                        }
                    }
                }
                _ = tokio::time::sleep_until(idle_deadline.unwrap_or_else(Instant::now)), if idle_deadline.is_some() => {
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
                _ = tokio::time::sleep_until(subscription_deadline), if !self.pending_subscriptions.is_empty() => {
                    let mut message = self.pending_subscriptions.pop_front().expect("pending subscription");
                    if matches!(self.heartbeat.kind, HeartbeatKind::Gate(_)) {
                        let mut payload: serde_json::Value = serde_json::from_str(&message)
                            .map_err(|error| Error::Parse(error.to_string()))?;
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |duration| duration.as_secs());
                        payload["time"] = serde_json::json!(now);
                        message = payload.to_string();
                    }
                    self.send_text(&message).await?;
                    if let Some(readiness) = self.readiness.as_mut() { readiness.sent(&message, Instant::now()); }
                    self.subscribed();
                    self.next_subscription = Instant::now() + Duration::from_millis(250);
                }
                _ = tokio::time::sleep_until(acknowledgement_deadline.unwrap_or_else(Instant::now)), if acknowledgement_deadline.is_some() => {
                    return Err(Error::Transport("subscription acknowledgement timed out".to_owned()));
                }
                _ = tokio::time::sleep_until(poll_at.unwrap_or_else(Instant::now)), if poll_at.is_some() => {
                    return Ok(SessionRead::Poll);
                }
                message = self.stream.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            self.last_text_received_ts=super::current_timestamp();
                            self.last_received = Instant::now();
                            if let Some(readiness) = &self.readiness { readiness.received(); }
                            let text = text.to_string();
                            let value = serde_json::from_str::<serde_json::Value>(&text).ok();
                            #[cfg(feature="recording")] if let Some(raw)=&self.raw{self.last_raw_sequence=raw.text(&text,value.as_ref(),true,self.last_text_received_ts);}
                            if text == "pong" && matches!(self.heartbeat.kind, HeartbeatKind::Text("ping"))
                                || value.as_ref().is_some_and(|value| self.heartbeat.is_response_value(value)) { continue; }
                            if let (Some(readiness), Some(value)) = (self.readiness.as_mut(), value.as_ref()) {
                                match readiness.control(value) {
                                    Ok(true) => continue,
                                    Ok(false) => {},
                                    Err(error) => {
                                        if let Error::RateLimited { retry_after, .. } = &error {
                                            super::budget::defer(readiness.exchange(), *retry_after).await?;
                                        }
                                        return Err(error);
                                    }
                                }
                            }
                            return Ok(SessionRead::Text(text));
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            self.last_received = Instant::now();
                            if let Some(readiness) = &self.readiness { readiness.received(); }
                            self.sends.pong_started(Instant::now())?;
                            tokio::time::timeout(Self::SEND_TIMEOUT, self.stream.send(Message::Pong(payload))).await
                                .map_err(|_| Error::Transport("websocket pong timed out".to_owned()))?
                                .map_err(|error| Error::Transport(error.to_string()))?;
                        }
                        Some(Ok(Message::Pong(_) | Message::Binary(_) | Message::Frame(_))) => {
                            self.last_received = Instant::now();
                            if let Some(readiness) = &self.readiness { readiness.received(); }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            if *shutdown.borrow() { return Ok(SessionRead::Closed); }
                            let exchange = self.exchange.or_else(|| self.readiness.as_ref().map(|attempt| attempt.exchange()));
                            return Err(super::quota::close_error(exchange, frame.map(|frame| u16::from(frame.code))));
                        }
                        None => {
                            if *shutdown.borrow() { return Ok(SessionRead::Closed); }
                            return Err(super::quota::close_error(self.exchange, None));
                        }
                        Some(Err(error)) => return Err(Error::Transport(error.to_string())),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;
    #[tokio::test]
    async fn ws_lifecycle_rejected_handshake_releases_scoped_capacity_for_next_session() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                bytes.push(first.read_u8().await.unwrap());
                assert!(bytes.len() < 16384);
            }
            first
                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            drop(first);
            let (second, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(second).await.unwrap();
        });
        let budget = crate::ResourceBudget::new(
            crate::ResourcePolicy::default()
                .websocket(1, Duration::from_millis(1100))
                .unwrap(),
        );
        let scope = budget.websocket(ExchangeId::Coinbase);
        let connection = WsConnection::new(
            Url::parse(&format!("ws://{address}/public")).unwrap(),
            ExchangeId::Coinbase,
        )
        .transport(crate::TransportConfig::direct().with_resource_budget(budget));
        assert!(matches!(
            connection.connect().await,
            Err(Error::HttpStatus { status: 403, .. })
        ));
        assert_eq!(scope.slots.available_permits(), 1);
        let session = tokio::time::timeout(Duration::from_secs(5), connection.connect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(scope.slots.available_permits(), 0);
        drop(session);
        assert_eq!(scope.slots.available_permits(), 1);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn configured_handshake_deadline_bounds_stalled_establishment_and_keeps_results() {
        let connection = WsConnection::new(
            Url::parse("wss://example.invalid/public").unwrap(),
            ExchangeId::Okx,
        )
        .runtime_options(
            crate::options::RuntimeOptions::default()
                .connect_timeout(Duration::from_millis(10))
                .unwrap(),
        );
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            connection.handshake(std::future::pending::<Result<()>>()),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(error, Error::Transport(message) if message.contains("websocket connect timed out"))
        );
        assert_eq!(connection.handshake(async { Ok(42) }).await.unwrap(), 42);
        assert!(
            matches!(connection.handshake(async { Err::<(), _>(Error::Transport("original".into())) }).await,
            Err(Error::Transport(message)) if message == "original")
        );
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
    use tokio::time::Instant;
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
    async fn ws_lifecycle_close_frames_preserve_codes_withdraw_readiness_and_scope_venue_rules() {
        use crate::feed::{FeedId, FeedIdentity, FeedState};
        for (exchange, code, permanent) in [
            (ExchangeId::Okx, 4009, true),
            (ExchangeId::Binance, 4009, false),
            (ExchangeId::Okx, 1000, false),
            (ExchangeId::Okx, 1009, true),
        ] {
            let monitor = std::sync::Arc::new(crate::runtime::readiness::FeedMonitor::new(
                FeedIdentity {
                    id: FeedId::allocate(),
                    generation: 1,
                },
                exchange,
                None,
            ));
            monitor.configure(1, vec![]);
            monitor.lifecycle(FeedState::Started);
            let tracker = monitor.register("synthetic".into(), vec![]);
            let attempt = tracker.begin();
            attempt.connected();
            let (client, mut server) = websocket_pair().await;
            let mut session = Session::new(
                client,
                HeartbeatPolicy::for_test(None, Duration::from_secs(60), Duration::from_secs(90)),
            );
            session.set_readiness(Some(attempt));
            server
                .send(Message::Close(Some(
                    tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: code.into(),
                        reason: "untrusted secret reason".into(),
                    },
                )))
                .await
                .unwrap();
            let (_tx, mut rx) = watch::channel(false);
            let error = session.next_text_or_shutdown(&mut rx).await.unwrap_err();
            assert!(matches!(error,Error::WebSocketClosed{code:Some(found),..} if found==code));
            assert_eq!(crate::runtime::supervisor::is_permanent(&error), permanent);
            assert!(!error.to_string().contains("secret"));
            let state = monitor.snapshot();
            assert!(!state.connections[0].connected);
            assert!(
                state.connections[0]
                    .last_error
                    .as_ref()
                    .unwrap()
                    .contains(&code.to_string())
            );
        }
    }
    #[tokio::test]
    async fn ws_lifecycle_shutdown_wins_over_a_pending_terminal_close() {
        let (client, mut server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(None, Duration::from_secs(60), Duration::from_secs(90)),
        );
        server
            .send(Message::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: 1009.into(),
                    reason: "".into(),
                },
            )))
            .await
            .unwrap();
        let (_tx, mut rx) = watch::channel(true);
        assert!(
            session
                .next_text_or_shutdown(&mut rx)
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn missing_acknowledgement_times_out_and_updates_retained_state() {
        use crate::feed::{FeedId, FeedIdentity, FeedState};
        let monitor = std::sync::Arc::new(crate::runtime::readiness::FeedMonitor::new(
            FeedIdentity {
                id: FeedId::allocate(),
                generation: 1,
            },
            ExchangeId::Okx,
            None,
        ));
        monitor.configure(1, vec![]);
        monitor.lifecycle(FeedState::Started);
        let tracker = monitor.register("wss://ws.okx.com/ws/v5/public".into(), vec![]);
        let mut attempt = tracker.begin();
        let messages = attempt.prepare_messages(vec![
            serde_json::json!({"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]})
                .to_string(),
        ]);
        attempt.sent(
            &messages[0],
            tokio::time::Instant::now() - Duration::from_secs(31),
        );
        let (client, _server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(None, Duration::from_secs(60), Duration::from_secs(90)),
        );
        session.set_readiness(Some(attempt));
        let (_tx, mut rx) = watch::channel(false);
        let error = session.next_text_or_shutdown(&mut rx).await.unwrap_err();
        assert!(error.to_string().contains("acknowledgement timed out"));
        let state = monitor.snapshot();
        assert!(!state.is_ready());
        assert!(!state.connections[0].connected);
        assert!(
            state.connections[0]
                .last_error
                .as_ref()
                .unwrap()
                .contains("acknowledgement timed out")
        );
    }

    #[tokio::test]
    async fn custom_idle_deadline_times_out_without_transport_receipts() {
        let (client, _server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(None, Duration::from_secs(30), Duration::from_secs(60)),
        )
        .runtime_options(
            crate::options::RuntimeOptions::default()
                .idle_policy(crate::options::IdlePolicy::After(Duration::from_millis(20)))
                .unwrap(),
        );
        let (_stop, mut shutdown) = watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            session.next_text_or_shutdown(&mut shutdown),
        )
        .await
        .unwrap();
        assert!(
            matches!(result, Err(Error::Transport(message)) if message == "websocket idle timeout")
        );
    }

    #[tokio::test]
    async fn disabling_idle_watchdog_keeps_heartbeats_and_shutdown_active() {
        let (client, mut server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(
                Some("ping"),
                Duration::from_millis(10),
                Duration::from_millis(30),
            ),
        )
        .runtime_options(
            crate::options::RuntimeOptions::default()
                .idle_policy(crate::options::IdlePolicy::Disabled)
                .unwrap(),
        );
        let (stop, mut shutdown) = watch::channel(false);
        let task = tokio::spawn(async move { session.next_text_or_shutdown(&mut shutdown).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            for _ in 0..3 { assert!(matches!(server.next().await.unwrap().unwrap(), Message::Text(value) if value == "ping")); }
        }).await.unwrap();
        stop.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn subscription_queue_is_paced_and_market_reads_continue() {
        let (client, mut server) = websocket_pair().await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(None, Duration::from_secs(30), Duration::from_secs(5)),
        );
        let progress = std::sync::Arc::new(super::super::supervisor::RetryProgress::default());
        session.set_retry_progress(Some(progress.clone()));
        let progress_in_task = progress.clone();
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
            assert!(!progress_in_task.take()); // A partial subscribe send is not successful initialization.
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
        assert!(progress.take()); // All initial subscription writes completed.
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shared_send_budget_reserves_one_pong_and_keeps_market_reads_live() {
        let (client, mut server) = websocket_pair().await;
        let url = Url::parse("wss://stream.binance.com/ws").unwrap();
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_exchange(ExchangeId::Binance, &url),
        );
        let start = Instant::now();
        for _ in 0..4 {
            session.send_text("control").await.unwrap();
            assert!(matches!(
                server.next().await.unwrap().unwrap(),
                Message::Text(_)
            ));
        }
        session.queue_subscriptions(["fifth".to_owned()]);
        server
            .send(Message::Ping(vec![1, 2, 3].into()))
            .await
            .unwrap();
        server.send(Message::Text("market".into())).await.unwrap();
        let (stop, mut shutdown) = watch::channel(false);
        assert_eq!(
            session.next_text_or_shutdown(&mut shutdown).await.unwrap(),
            Some("market".into())
        );
        assert!(
            matches!(server.next().await.unwrap().unwrap(), Message::Pong(payload) if payload == vec![1,2,3])
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
        let task = tokio::spawn(async move { session.next_text_or_shutdown(&mut shutdown).await });
        assert!(
            matches!(server.next().await.unwrap().unwrap(), Message::Text(text) if text == "fifth")
        );
        assert!(Instant::now() - start >= Duration::from_millis(1250));
        stop.send(true).unwrap();
        assert!(task.await.unwrap().unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn hourly_budget_wait_preserves_heartbeat_reads_and_shutdown() {
        let (client, mut server) = websocket_pair().await;
        let url = Url::parse("wss://ws.bitget.com/v3/ws/public").unwrap();
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_exchange(ExchangeId::Bitget, &url),
        )
        .runtime_options(
            crate::options::RuntimeOptions::default()
                .idle_policy(crate::options::IdlePolicy::Disabled)
                .unwrap(),
        );
        let start = Instant::now();
        for index in 0..240 {
            session
                .sends
                .text_started(start + Duration::from_secs(index), true);
        }
        tokio::time::advance(Duration::from_secs(240)).await;
        session.queue_subscriptions([r#"{"op":"subscribe","args":[]}"#.to_owned()]);
        server.send(Message::Text("market".into())).await.unwrap();
        let (stop, mut shutdown) = watch::channel(false);
        assert_eq!(
            session.next_text_or_shutdown(&mut shutdown).await.unwrap(),
            Some("market".into())
        );
        assert!(
            matches!(server.next().await.unwrap().unwrap(), Message::Text(text) if text == "ping")
        );
        assert_eq!(session.pending_subscriptions.len(), 1);
        stop.send(true).unwrap();
        assert!(
            session
                .next_text_or_shutdown(&mut shutdown)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        assert!(Instant::now() - start < Duration::from_secs(3600));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_direct_send_before_admission_consumes_no_request() {
        let (client, mut server) = websocket_pair().await;
        let url = Url::parse("wss://ws.okx.com/ws/v5/public").unwrap();
        let mut session =
            Session::new(client, HeartbeatPolicy::for_exchange(ExchangeId::Okx, &url));
        session
            .send_text(r#"{"op":"subscribe","args":[]}"#)
            .await
            .unwrap();
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));
        {
            let cancelled = session.send_text(r#"{"op":"subscribe","args":[]}"#);
            tokio::pin!(cancelled);
            assert!(futures::poll!(cancelled.as_mut()).is_pending());
        }
        tokio::time::advance(Duration::from_millis(250)).await;
        session
            .send_text(r#"{"op":"subscribe","args":[]}"#)
            .await
            .unwrap();
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));
        assert_eq!(session.sends.control_count_for_test(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn ping_burst_drops_session_before_flushing_an_over_budget_automatic_pong() {
        let (client, mut server) = websocket_pair().await;
        let url = Url::parse("wss://stream.binance.com/ws").unwrap();
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_exchange(ExchangeId::Binance, &url),
        );
        for index in 0..6 {
            server
                .send(Message::Ping(vec![index].into()))
                .await
                .unwrap();
        }
        let (_stop, mut shutdown) = watch::channel(false);
        let error = session
            .next_text_or_shutdown(&mut shutdown)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Transport(message) if message.contains("ping burst")));
        drop(session);
        for index in 0..5 {
            assert!(
                matches!(server.next().await.unwrap().unwrap(), Message::Pong(payload) if payload == vec![index])
            );
        }
        assert!(matches!(server.next().await, None | Some(Err(_))));
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
        assert!(matches!(
            error,
            Error::WebSocketClosed {
                code: None,
                retryable: true
            }
        ));
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

#[cfg(all(test, feature = "recording"))]
mod raw_tests {
    use super::*;
    use crate::{
        exchange::okx::Okx,
        recording::raw::{RawFeedInfo, RawObservationKind, RawPayload, raw_capture_channel},
    };
    use tokio_tungstenite::tungstenite::protocol::Role;
    #[tokio::test]
    async fn observes_inbound_pong_before_filtering_and_uses_identical_receipt_clock() {
        let (client, server) = tokio::io::duplex(8192);
        let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut session = Session::new(
            client,
            HeartbeatPolicy::for_test(
                Some("ping"),
                Duration::from_secs(20),
                Duration::from_secs(45),
            ),
        );
        let (capture, mut rx) = raw_capture_channel(16, 4096).unwrap();
        session.raw = capture.session(RawFeedInfo::from_feed(
            &Okx::new().trade().symbol("BTC-USDT").build(),
        ));
        server.send(Message::Text("pong".into())).await.unwrap();
        server
            .send(Message::Text(r#"{"data":{"px":"100"}}"#.into()))
            .await
            .unwrap();
        let (_tx, mut shutdown) = watch::channel(false);
        let text = session
            .next_text_or_shutdown(&mut shutdown)
            .await
            .unwrap()
            .unwrap();
        assert!(text.contains("100"));
        rx.recv().await.unwrap().unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().unwrap().kind,
            RawObservationKind::Received(RawPayload::Heartbeat(_))
        ));
        let data = rx.recv().await.unwrap().unwrap();
        assert_eq!(data.observed_ts, session.received_timestamp());
        session
            .send_text(r#"{"op":"subscribe","apiKey":"fake-secret"}"#)
            .await
            .unwrap();
        let sent = rx.recv().await.unwrap().unwrap();
        assert!(matches!(sent.kind, RawObservationKind::Sent(_)));
        assert!(
            !serde_json::to_string(&sent)
                .unwrap()
                .contains("fake-secret")
        );
    }
}
