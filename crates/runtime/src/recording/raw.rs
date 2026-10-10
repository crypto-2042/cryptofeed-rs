//! Bounded, sanitized observation of actual public WS text before parsing.
//! This transport stream is not yet a persisted raw recording/replay file.
use crate::{exchange::ExchangeFeed, feed::FeedIdentity};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::{Channel, ExchangeId},
    symbol::Symbol,
};
use serde::Serialize;
use serde_json::Value;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;

/// Public routing inputs only; transport URLs/credentials and handlers are absent.
#[derive(Clone, Debug, Serialize)]
pub struct RawFeedInfo {
    pub sdk_version: String,
    pub identity: Option<FeedIdentity>,
    pub exchange: ExchangeId,
    pub channels: Vec<Channel>,
    pub symbols: Vec<Symbol>,
    pub exchange_symbols: Vec<String>,
    pub channel_subscriptions: Vec<(Channel, Vec<Symbol>)>,
    pub candle_interval: String,
    pub candle_policy: Option<String>,
    pub l2_book_depth: Option<u16>,
    pub l2_book_interval: Option<String>,
}
impl RawFeedInfo {
    pub(crate) fn from_feed(feed: &ExchangeFeed) -> Self {
        Self {
            sdk_version: env!("CARGO_PKG_VERSION").into(),
            identity: feed.identity(),
            exchange: feed.exchange,
            channels: feed.channels.clone(),
            symbols: feed.symbols.clone(),
            exchange_symbols: feed.exchange_symbols.clone(),
            channel_subscriptions: feed.channel_subscriptions.clone(),
            candle_interval: feed.candle_interval.clone(),
            candle_policy: {
                #[cfg(feature = "candles")]
                {
                    Some(format!("{:?}", feed.candle_policy))
                }
                #[cfg(not(feature = "candles"))]
                {
                    None
                }
            },
            l2_book_depth: feed.l2_book_depth,
            l2_book_interval: feed.l2_book_interval.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct RawSessionInfo {
    pub session_id: u64,
    pub feed: RawFeedInfo,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawPayload {
    Json { value: Value, redacted: bool },
    Heartbeat(String),
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawObservationKind {
    Connected,
    Sent(RawPayload),
    Received(RawPayload),
    Closed { clean: bool },
}
#[derive(Clone, Debug, Serialize)]
pub struct RawObservation {
    pub version: u16,
    pub sequence: u64,
    pub elapsed_ns: u64,
    pub observed_ts: f64,
    pub session: Arc<RawSessionInfo>,
    pub kind: RawObservationKind,
}
#[derive(Clone, Copy, Debug)]
enum Failure {
    Full,
    Closed,
    TooLarge,
    Malformed,
    Private,
    Sequence,
}
impl Failure {
    fn error(self) -> Error {
        Error::Protocol(format!(
            "raw capture incomplete: {}",
            match self {
                Self::Full => "queue overflow",
                Self::Closed => "consumer closed",
                Self::TooLarge => "message limit",
                Self::Malformed => "unrecognized text/depth",
                Self::Private => "private operation rejected",
                Self::Sequence => "sequence limit",
            }
        ))
    }
}
struct State {
    sequence: u64,
    failure: Option<Failure>,
}
struct Shared {
    state: Mutex<State>,
    started: tokio::time::Instant,
    maximum: usize,
}
#[derive(Clone)]
pub struct RawCaptureHandle(Arc<Shared>, mpsc::Sender<RawObservation>);
impl std::fmt::Debug for RawCaptureHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RawCaptureHandle(public sanitized observations)")
    }
}
pub struct RawCaptureReceiver {
    receiver: mpsc::Receiver<RawObservation>,
    shared: Arc<Shared>,
}
/// Capacity 1..1024; logical text limit 256 bytes..8 MiB. Producers never await disk.
pub fn raw_capture_channel(
    capacity: usize,
    max_message_bytes: usize,
) -> Result<(RawCaptureHandle, RawCaptureReceiver)> {
    if !(1..=1024).contains(&capacity) || !(256..=8 * 1024 * 1024).contains(&max_message_bytes) {
        return Err(Error::InvalidConfiguration(
            "raw capture bounds require capacity 1..1024 and message bytes 256..8MiB".into(),
        ));
    }
    let (sender, receiver) = mpsc::channel(capacity);
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            sequence: 0,
            failure: None,
        }),
        started: tokio::time::Instant::now(),
        maximum: max_message_bytes,
    });
    Ok((
        RawCaptureHandle(shared.clone(), sender),
        RawCaptureReceiver { receiver, shared },
    ))
}
impl RawCaptureReceiver {
    /// Sticky failures invalidate the observed prefix; no messages are silently skipped.
    pub async fn recv(&mut self) -> Result<Option<RawObservation>> {
        if let Some(failure) = self.shared.state.lock().expect("raw capture state").failure {
            return Err(failure.error());
        }
        let observation = self.receiver.recv().await;
        if let Some(failure) = self.shared.state.lock().expect("raw capture state").failure {
            return Err(failure.error());
        }
        Ok(observation)
    }
}
impl RawCaptureHandle {
    fn fail(&self, failure: Failure) {
        let mut state = self.0.state.lock().expect("raw capture state");
        state.failure.get_or_insert(failure);
    }
    fn failed(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("raw capture state")
            .failure
            .is_some()
    }
    pub(crate) fn session(&self, feed: RawFeedInfo) -> Option<RawSession> {
        if self.failed() {
            return None;
        }
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let session = RawSession {
            capture: self.clone(),
            info: Arc::new(RawSessionInfo {
                session_id: NEXT.fetch_add(1, Ordering::Relaxed),
                feed,
            }),
            closed: false,
        };
        session.emit(
            RawObservationKind::Connected,
            super::super::runtime::current_timestamp(),
        );
        Some(session)
    }
    fn emit(&self, session: Arc<RawSessionInfo>, kind: RawObservationKind, observed_ts: f64) {
        let mut state = self.0.state.lock().expect("raw capture state");
        if state.failure.is_some() {
            return;
        }
        let Some(sequence) = state.sequence.checked_add(1) else {
            state.failure = Some(Failure::Sequence);
            return;
        };
        let Ok(elapsed_ns) = u64::try_from(self.0.started.elapsed().as_nanos()) else {
            state.failure = Some(Failure::Sequence);
            return;
        };
        let observation = RawObservation {
            version: 1,
            sequence,
            elapsed_ns,
            observed_ts,
            session,
            kind,
        };
        match self.1.try_send(observation) {
            Ok(()) => state.sequence = sequence,
            Err(mpsc::error::TrySendError::Full(_)) => state.failure = Some(Failure::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => state.failure = Some(Failure::Closed),
        }
    }
}
pub(crate) struct RawSession {
    capture: RawCaptureHandle,
    info: Arc<RawSessionInfo>,
    closed: bool,
}
impl RawSession {
    fn emit(&self, kind: RawObservationKind, observed_ts: f64) {
        self.capture.emit(self.info.clone(), kind, observed_ts);
    }
    pub(crate) fn text(
        &self,
        text: &str,
        parsed: Option<&Value>,
        received: bool,
        observed_ts: f64,
    ) {
        if self.capture.failed() {
            return;
        }
        if text.len() > self.capture.0.maximum {
            self.capture.fail(Failure::TooLarge);
            return;
        }
        let payload = if let Some(value) = parsed {
            let mut redacted = false;
            match sanitize(value, 0, &mut redacted) {
                Ok(value) => RawPayload::Json { value, redacted },
                Err(failure) => {
                    self.capture.fail(failure);
                    return;
                }
            }
        } else if matches!(text, "ping" | "pong") {
            RawPayload::Heartbeat(text.into())
        } else {
            self.capture.fail(Failure::Malformed);
            return;
        };
        self.emit(
            if received {
                RawObservationKind::Received(payload)
            } else {
                RawObservationKind::Sent(payload)
            },
            observed_ts,
        );
    }
    pub(crate) fn close(&mut self, clean: bool) {
        if !self.closed {
            self.closed = true;
            self.emit(
                RawObservationKind::Closed { clean },
                super::super::runtime::current_timestamp(),
            );
        }
    }
}
impl Drop for RawSession {
    fn drop(&mut self) {
        self.close(false);
    }
}
fn key(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}
fn sanitize(value: &Value, depth: u8, redacted: &mut bool) -> std::result::Result<Value, Failure> {
    if depth > 32 {
        return Err(Failure::Malformed);
    }
    Ok(match value {
        Value::Object(values) => {
            for (name, value) in values {
                if matches!(key(name).as_str(), "op" | "event" | "action")
                    && value.as_str().is_some_and(|value| {
                        matches!(value.to_ascii_lowercase().as_str(), "auth" | "login")
                    })
                {
                    return Err(Failure::Private);
                }
            }
            for (name, value) in values {
                if matches!(key(name).as_str(), "channel" | "topic")
                    && value.as_str().is_some_and(|topic| {
                        topic.split('.').any(|part| {
                            matches!(
                                key(part).as_str(),
                                "orders"
                                    | "ordersalgo"
                                    | "account"
                                    | "balances"
                                    | "positions"
                                    | "usertrades"
                                    | "execution"
                                    | "wallet"
                            )
                        })
                    })
                {
                    return Err(Failure::Private);
                }
            }
            let mut output = serde_json::Map::new();
            for (name, value) in values {
                let name_key = key(name);
                let credential = matches!(
                    name_key.as_str(),
                    "apikey"
                        | "key"
                        | "accesskey"
                        | "secretkey"
                        | "apisecret"
                        | "secret"
                        | "password"
                        | "passphrase"
                        | "signature"
                        | "sign"
                        | "authorization"
                        | "proxyauthorization"
                        | "token"
                        | "accesstoken"
                        | "refreshtoken"
                        | "cookie"
                        | "setcookie"
                        | "listenkey"
                        | "auth"
                );
                let diagnostic =
                    matches!(name_key.as_str(), "msg" | "message" | "retmsg" | "error")
                        && value.as_str().is_some_and(|text| {
                            !matches!(text, "pong" | "success" | "ok" | "OK" | "")
                        });
                if credential || diagnostic {
                    *redacted = true;
                    output.insert(name.clone(), Value::String("[REDACTED]".into()));
                } else {
                    output.insert(name.clone(), sanitize(value, depth + 1, redacted)?);
                }
            }
            Value::Object(output)
        }
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| sanitize(value, depth + 1, redacted))
                .collect::<std::result::Result<_, _>>()?,
        ),
        _ => value.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::okx::Okx;
    use serde_json::json;
    fn info() -> RawFeedInfo {
        RawFeedInfo::from_feed(
            &Okx::new()
                .trade()
                .symbol("BTC-USDT")
                .exchange_symbol("BTC-USDT")
                .build(),
        )
    }
    #[tokio::test]
    async fn exact_payload_redaction_and_context_have_no_transport_credentials() {
        let (handle, mut rx) = raw_capture_channel(8, 4096).unwrap();
        let session = handle.session(info()).unwrap();
        let mut value:Value=serde_json::from_str(r#"{"data":{"px":100.1234567890123456789012345,"sz":"1","api_key":"never-write"},"error":{"code":7,"message":"secret diagnostic"},"headers":{"Authorization":"Bearer bad"}}"#).unwrap();
        value["signature"] = json!("fake-signature");
        let text = serde_json::to_string(&value).unwrap();
        session.text(&text, Some(&value), true, 1.25);
        assert!(matches!(
            rx.recv().await.unwrap().unwrap().kind,
            RawObservationKind::Connected
        ));
        let frame = rx.recv().await.unwrap().unwrap();
        assert_eq!(frame.sequence, 2);
        assert_eq!(frame.observed_ts, 1.25);
        let encoded = serde_json::to_string(&frame).unwrap();
        for secret in [
            "never-write",
            "secret diagnostic",
            "Bearer bad",
            "fake-signature",
        ] {
            assert!(!encoded.contains(secret));
        }
        assert!(encoded.contains("100.1234567890123456789012345"));
        assert_eq!(frame.session.feed.symbols[0], Symbol::spot("BTC", "USDT"));
        assert_eq!(frame.session.feed.exchange_symbols[0], "BTC-USDT");
        assert!(!encoded.contains("proxy"));
        let RawObservationKind::Received(RawPayload::Json { value, redacted }) = frame.kind else {
            panic!("json")
        };
        assert!(redacted);
        assert_eq!(value["error"]["code"], 7);
        assert_eq!(value["data"]["sz"], "1");
    }
    #[tokio::test]
    async fn full_queue_is_sticky_and_does_not_silently_deliver_prefix() {
        let (handle, mut rx) = raw_capture_channel(1, 256).unwrap();
        let session = handle.session(info()).unwrap();
        session.text("pong", None, true, 1.0);
        assert!(rx.recv().await.is_err());
        assert!(rx.recv().await.is_err());
    }
    #[tokio::test]
    async fn handles_drop_closes_stream_and_session_drop_emits_one_close() {
        let (handle, mut rx) = raw_capture_channel(8, 256).unwrap();
        let mut session = handle.session(info()).unwrap();
        let id = session.info.session_id;
        session.close(true);
        drop(session);
        drop(handle);
        let first = rx.recv().await.unwrap().unwrap();
        assert_eq!(first.session.session_id, id);
        assert!(matches!(
            rx.recv().await.unwrap().unwrap().kind,
            RawObservationKind::Closed { clean: true }
        ));
        assert!(rx.recv().await.unwrap().is_none());
    }
    #[tokio::test]
    async fn private_operations_unknown_text_and_limits_fail_without_payload_echo() {
        for text in [
            r#"{"op":"auth","args":["private-key","private-signature"]}"#,
            r#"{"channel":"spot.orders","result":{"id":"private-order"}}"#,
            "arbitrary private text",
        ] {
            let (handle, mut rx) = raw_capture_channel(8, 1024).unwrap();
            let session = handle.session(info()).unwrap();
            let value = serde_json::from_str(text).ok();
            session.text(text, value.as_ref(), true, 1.0);
            let error = rx.recv().await.unwrap_err().to_string();
            assert!(!error.contains("private-key"));
            assert!(!error.contains("private-order"));
            assert!(!error.contains("arbitrary private text"));
        }
        let (handle, mut rx) = raw_capture_channel(8, 256).unwrap();
        let session = handle.session(info()).unwrap();
        session.text(&"x".repeat(257), None, true, 1.0);
        assert!(rx.recv().await.is_err());
    }
    #[tokio::test]
    async fn reconnects_have_distinct_sessions_and_observation_sequence_is_global() {
        let (handle, mut rx) = raw_capture_channel(8, 1024).unwrap();
        let first = handle.session(info()).unwrap();
        let a = first.info.session_id;
        drop(first);
        let second = handle.session(info()).unwrap();
        assert_ne!(a, second.info.session_id);
        drop(second);
        drop(handle);
        let mut sequence = 0;
        let mut elapsed = 0;
        while let Some(frame) = rx.recv().await.unwrap() {
            sequence += 1;
            assert_eq!(frame.sequence, sequence);
            assert!(frame.elapsed_ns >= elapsed);
            elapsed = frame.elapsed_ns;
        }
        assert_eq!(sequence, 4);
    }
    #[test]
    fn pong_control_is_retained_and_sanitization_depth_is_bounded() {
        let mut redacted = false;
        let p = json!({"ret_msg":"pong","msg":"success"});
        assert_eq!(sanitize(&p, 0, &mut redacted).unwrap(), p);
        assert!(!redacted);
        let mut p = json!(1);
        for _ in 0..34 {
            p = json!([p]);
        }
        assert!(sanitize(&p, 0, &mut redacted).is_err());
        assert!(raw_capture_channel(0, 1024).is_err());
        assert!(raw_capture_channel(1, 255).is_err());
    }
}
