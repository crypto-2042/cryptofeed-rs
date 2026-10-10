use crate::feed::{ConnectionInfo, FeedIdentity, FeedSnapshot, FeedState, FeedStatus};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::{Channel, ExchangeId},
    symbol::Symbol,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::{
    sync::broadcast,
    time::{Duration, Instant},
};

#[derive(Clone)]
struct ConnectionRecord {
    info: ConnectionInfo,
    books: HashSet<String>,
    configured: bool,
    disconnected: bool,
}
struct State {
    lifecycle: FeedState,
    expected: usize,
    books: HashSet<String>,
    synced: HashMap<String, (u64, u64)>,
    connections: BTreeMap<u64, ConnectionRecord>,
    observed: HashMap<Channel, HashSet<Symbol>>,
    events: u64,
    last_event: Option<std::time::SystemTime>,
}

pub(crate) struct FeedMonitor {
    #[cfg(feature = "orderbook")]
    pub books: Option<Arc<crate::books::BookStore>>,
    pub identity: FeedIdentity,
    pub exchange: ExchangeId,
    status: Option<broadcast::Sender<FeedStatus>>,
    state: Mutex<State>,
}

impl FeedMonitor {
    pub fn new(
        identity: FeedIdentity,
        exchange: ExchangeId,
        status: Option<broadcast::Sender<FeedStatus>>,
    ) -> Self {
        Self {
            #[cfg(feature = "orderbook")]
            books: None,
            identity,
            exchange,
            status,
            state: Mutex::new(State {
                lifecycle: FeedState::Preparing,
                expected: 0,
                books: HashSet::new(),
                synced: HashMap::new(),
                connections: BTreeMap::new(),
                observed: HashMap::new(),
                events: 0,
                last_event: None,
            }),
        }
    }
    pub fn configure(&self, expected: usize, books: Vec<String>) {
        let mut state = self.state.lock().expect("feed monitor lock");
        state.expected = expected;
        state.books = books.into_iter().collect();
    }
    fn emit(&self, state: FeedState) {
        if let Some(sender) = &self.status {
            let _ = sender.send(FeedStatus::Lifecycle {
                identity: self.identity,
                exchange: self.exchange,
                state,
            });
        }
    }
    pub fn lifecycle(&self, lifecycle: FeedState) {
        let mut state = self.state.lock().expect("feed monitor lock");
        if matches!(lifecycle, FeedState::Degraded { .. })
            && matches!(
                state.lifecycle,
                FeedState::Stopping
                    | FeedState::Stopped { .. }
                    | FeedState::Failed { .. }
                    | FeedState::Cancelled { .. }
            )
        {
            return;
        }
        #[cfg(feature = "orderbook")]
        if let Some(books) = &self.books {
            match &lifecycle {
                FeedState::Started => books.activate(self.identity),
                FeedState::Stopping
                | FeedState::Stopped { .. }
                | FeedState::Failed { .. }
                | FeedState::Cancelled { .. } => books.retire(self.identity),
                _ => {}
            }
        }
        state.lifecycle = lifecycle.clone();
        self.emit(lifecycle.clone());
        drop(state);
        if let FeedState::Failed { error } | FeedState::Degraded { error } = &lifecycle {
            tracing::error!(?self.identity, ?self.exchange, %error, "managed feed status");
        }
    }
    fn update(&self, update: impl FnOnce(&mut State)) {
        let mut state = self.state.lock().expect("feed monitor lock");
        update(&mut state);
        if matches!(
            state.lifecycle,
            FeedState::Preparing
                | FeedState::Stopping
                | FeedState::Stopped { .. }
                | FeedState::Failed { .. }
                | FeedState::Cancelled { .. }
                | FeedState::Degraded { .. }
        ) {
            return;
        }
        let all_connected = state.expected > 0
            && state.connections.len() == state.expected
            && state
                .connections
                .values()
                .all(|connection| connection.info.connected);
        let confirmed = all_connected
            && state.connections.values().all(|connection| {
                connection.configured
                    && connection.info.subscriptions_confirmed
                        == connection.info.subscriptions_expected
            });
        let mut transitions = Vec::new();
        if confirmed {
            if !matches!(state.lifecycle, FeedState::Subscribed | FeedState::Ready) {
                transitions.push(FeedState::Subscribed);
            }
            let next = if state.books.len() == state.synced.len() {
                FeedState::Ready
            } else {
                FeedState::Subscribed
            };
            if state.lifecycle != next && transitions.last() != Some(&next) {
                transitions.push(next);
            }
        } else {
            let next = if state
                .connections
                .values()
                .any(|connection| connection.disconnected || connection.info.epoch > 1)
            {
                FeedState::Reconnecting
            } else {
                FeedState::Connecting
            };
            if state.lifecycle != next {
                transitions.push(next);
            }
        }
        // Publish transitions under the same lock as state mutation so a
        // concurrent stop cannot be followed by a stale Ready notification.
        for transition in transitions {
            state.lifecycle = transition.clone();
            self.emit(transition);
        }
    }
    pub fn snapshot(&self) -> FeedSnapshot {
        let state = self.state.lock().expect("feed monitor lock");
        let mut book_counts = HashMap::new();
        for owner in state.synced.values() {
            *book_counts.entry(*owner).or_insert(0) += 1;
        }
        FeedSnapshot {
            identity: self.identity,
            exchange: self.exchange,
            state: state.lifecycle.clone(),
            connections_expected: state.expected,
            connections: state
                .connections
                .values()
                .map(|record| {
                    let mut info = record.info.clone();
                    info.books_synchronized = book_counts
                        .get(&(info.id, info.epoch))
                        .copied()
                        .unwrap_or(0);
                    info
                })
                .collect(),
            books_expected: state.books.len(),
            books_synchronized: state.synced.len(),
            observed_pairs: state.observed.values().map(HashSet::len).sum(),
            observed_events: state.events,
            last_event_at: state.last_event,
        }
    }
    pub fn register(self: &Arc<Self>, url: String, books: Vec<String>) -> ConnectionTracker {
        let mut state = self.state.lock().expect("feed monitor lock");
        let id = state.connections.len() as u64 + 1;
        let books: HashSet<_> = books.into_iter().collect();
        state.connections.insert(
            id,
            ConnectionRecord {
                info: ConnectionInfo {
                    id,
                    epoch: 0,
                    url,
                    connected: false,
                    subscriptions_expected: 0,
                    subscriptions_confirmed: 0,
                    books_expected: books.len(),
                    books_synchronized: 0,
                    last_error: None,
                    last_received_at: None,
                },
                books,
                configured: false,
                disconnected: false,
            },
        );
        ConnectionTracker {
            monitor: self.clone(),
            id,
        }
    }
    pub fn book_synced(&self, id: u64, epoch: u64, symbol: &str) {
        self.update(|state| {
            if state.connections.get(&id).is_some_and(|record| {
                record.info.epoch == epoch && record.info.connected && record.books.contains(symbol)
            }) && state.books.contains(symbol)
            {
                state.synced.insert(symbol.to_owned(), (id, epoch));
            }
        });
    }
    pub fn invalidate_book(&self, id: u64, epoch: u64, symbol: &str) {
        self.update(|state| {
            #[cfg(feature = "orderbook")]
            if let Some(books) = &self.books {
                books.invalidate(self.identity, id, epoch, Some(symbol));
            }
            if state.synced.get(symbol) == Some(&(id, epoch)) {
                state.synced.remove(symbol);
            }
        });
    }
    pub fn observe(&self, channel: Channel, symbol: &Symbol, owner: Option<(u64, u64)>) {
        let mut state = self.state.lock().expect("feed monitor lock");
        if owner.is_some_and(|(id, epoch)| {
            !state
                .connections
                .get(&id)
                .is_some_and(|record| record.info.epoch == epoch && record.info.connected)
        }) {
            return;
        }
        state.events += 1;
        state.last_event = Some(std::time::SystemTime::now());
        let symbols = state.observed.entry(channel).or_default();
        if !symbols.contains(symbol) {
            symbols.insert(symbol.clone());
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionTracker {
    monitor: Arc<FeedMonitor>,
    pub id: u64,
}

impl ConnectionTracker {
    pub fn begin(&self) -> ConnectionAttempt {
        let mut epoch = 0;
        self.monitor.update(|state| {
            let record = state
                .connections
                .get_mut(&self.id)
                .expect("registered connection");
            record.info.epoch += 1;
            epoch = record.info.epoch;
            record.info.connected = false;
            record.info.last_received_at = None;
            record.info.subscriptions_expected = 0;
            record.info.subscriptions_confirmed = 0;
            record.configured = false;
            #[cfg(feature = "orderbook")]
            if let Some(books) = &self.monitor.books {
                books.begin(self.monitor.identity, self.id, epoch, &record.books);
            }
            state.synced.retain(|_, owner| owner.0 != self.id);
        });
        ConnectionAttempt {
            tracker: self.clone(),
            epoch,
            pending: Vec::new(),
            sequence: 0,
        }
    }
}

struct Pending {
    weight: usize,
    key: String,
    argument: Option<Value>,
    sent: Option<Instant>,
    confirmed: bool,
}
pub(crate) struct ConnectionAttempt {
    tracker: ConnectionTracker,
    pub epoch: u64,
    pending: Vec<Pending>,
    sequence: u64,
}

fn arg_key(exchange: ExchangeId, argument: &Value) -> String {
    let fields: &[&str] = if exchange == ExchangeId::Bitget {
        &["instType", "topic", "symbol"]
    } else {
        &["channel", "instId", "instType"]
    };
    let mut selected = serde_json::Map::new();
    for key in fields {
        if let Some(value) = argument.get(*key) {
            selected.insert((*key).to_owned(), value.clone());
        }
    }
    Value::Object(selected).to_string()
}

fn argument_matches(exchange: ExchangeId, request: &Value, acknowledgement: &Value) -> bool {
    let fields: &[&str] = if exchange == ExchangeId::Bitget {
        &["instType", "topic", "symbol"]
    } else {
        &["channel", "instId", "instType"]
    };
    fields.iter().all(|field| {
        request
            .get(*field)
            .is_none_or(|value| acknowledgement.get(*field) == Some(value))
    }) && request
        .get("interval")
        .zip(acknowledgement.get("interval"))
        .is_none_or(|(requested, echoed)| requested == echoed)
}

impl ConnectionAttempt {
    fn update(&self, update: impl FnOnce(&mut State)) {
        self.tracker.monitor.update(|state| {
            if state
                .connections
                .get(&self.tracker.id)
                .is_some_and(|record| record.info.epoch == self.epoch)
            {
                update(state);
            }
        });
    }
    pub fn received(&self) {
        let mut state = self
            .tracker
            .monitor
            .state
            .lock()
            .expect("feed monitor lock");
        if let Some(record) = state.connections.get_mut(&self.tracker.id) {
            if record.info.epoch == self.epoch {
                record.info.last_received_at = Some(std::time::SystemTime::now());
            }
        }
    }

    pub fn connected(&self) {
        self.update(|state| {
            let record = state
                .connections
                .get_mut(&self.tracker.id)
                .expect("connection");
            record.info.connected = true;
            record.info.last_error = None;
            record.disconnected = false;
        });
    }
    fn keys(&self, payload: &Value) -> Vec<(String, Option<Value>)> {
        match self.tracker.monitor.exchange {
            ExchangeId::Binance => vec![(format!("b:{}", payload["id"]), None)],
            ExchangeId::Bybit => vec![(format!("y:{}", payload["req_id"]), None)],
            ExchangeId::Gateio => vec![(
                format!("g:{}:{}", payload["id"], payload["channel"]),
                payload.get("payload").cloned(),
            )],
            exchange @ (ExchangeId::Bitget | ExchangeId::Okx) => payload["args"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|argument| (arg_key(exchange, argument), Some(argument.clone())))
                .collect(),
            _ => Vec::new(),
        }
    }
    pub fn prepare_messages(&mut self, messages: Vec<String>) -> Vec<String> {
        let messages = messages
            .into_iter()
            .map(|message| {
                let mut payload: Value =
                    serde_json::from_str(&message).expect("adapter subscription JSON");
                self.sequence += 1;
                match self.tracker.monitor.exchange {
                    ExchangeId::Bybit => payload["req_id"] = json!(format!("cf-{}", self.sequence)),
                    ExchangeId::Gateio => payload["id"] = json!(self.sequence),
                    _ => {}
                }
                let weight = match self.tracker.monitor.exchange {
                    ExchangeId::Binance => payload["params"].as_array().map_or(0, Vec::len),
                    ExchangeId::Bybit => payload["args"].as_array().map_or(0, Vec::len),
                    _ => 1,
                };
                for (key, argument) in self.keys(&payload) {
                    if !self.pending.iter().any(|pending| pending.key == key) {
                        self.pending.push(Pending {
                            weight,
                            key,
                            argument,
                            sent: None,
                            confirmed: false,
                        });
                    }
                }
                payload.to_string()
            })
            .collect();
        let expected = self.pending.iter().map(|pending| pending.weight).sum();
        self.update(|state| {
            let record = state
                .connections
                .get_mut(&self.tracker.id)
                .expect("connection");
            record.info.subscriptions_expected = expected;
            record.configured = expected > 0;
        });
        messages
    }
    pub fn sent(&mut self, message: &str, at: Instant) {
        let payload: Value = serde_json::from_str(message).expect("subscription JSON");
        for (key, _) in self.keys(&payload) {
            if let Some(pending) = self
                .pending
                .iter_mut()
                .find(|pending| pending.key == key && pending.sent.is_none())
            {
                pending.sent = Some(at);
            }
        }
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.pending
            .iter()
            .filter(|pending| !pending.confirmed)
            .filter_map(|pending| pending.sent.map(|sent| sent + Duration::from_secs(30)))
            .min()
    }
    pub fn control(&mut self, payload: &Value) -> Result<bool> {
        let exchange = self.tracker.monitor.exchange;
        let is_control = match exchange {
            ExchangeId::Binance => {
                (payload.get("id").is_some() && payload.get("result").is_some())
                    || payload.get("code").is_some()
            }
            ExchangeId::Bybit => payload["op"] == "subscribe",
            ExchangeId::Bitget | ExchangeId::Okx => {
                payload["event"] == "subscribe" || payload["event"] == "error"
            }
            ExchangeId::Gateio => payload["event"] == "subscribe",
            _ => false,
        };
        if !is_control {
            return Ok(false);
        }
        let rejected = match exchange {
            ExchangeId::Binance => payload.get("code").is_some() || !payload["result"].is_null(),
            ExchangeId::Bybit => {
                payload["success"] != true
                    || payload
                        .pointer("/data/failTopics")
                        .and_then(Value::as_array)
                        .is_some_and(|topics| !topics.is_empty())
            }
            ExchangeId::Bitget | ExchangeId::Okx => {
                payload["event"] == "error"
                    || payload
                        .get("code")
                        .is_some_and(|code| code != "0" && code != "00000")
            }
            ExchangeId::Gateio => {
                payload.get("error").is_some_and(|error| !error.is_null())
                    || (payload.get("error") != Some(&Value::Null)
                        && payload.pointer("/result/status").and_then(Value::as_str)
                            != Some("success"))
            }
            _ => false,
        };
        if rejected {
            let error =
                Error::Subscription(format!("{exchange:?}: rejected subscription: {payload}"));
            self.disconnected(Some(error.to_string()));
            return Err(error);
        }
        let key = match exchange {
            ExchangeId::Binance => format!("b:{}", payload["id"]),
            ExchangeId::Bybit => format!("y:{}", payload["req_id"]),
            ExchangeId::Gateio => format!("g:{}:{}", payload["id"], payload["channel"]),
            _ => arg_key(exchange, &payload["arg"]),
        };
        for pending in &mut self.pending {
            let matches = if matches!(exchange, ExchangeId::Bitget | ExchangeId::Okx) {
                pending
                    .argument
                    .as_ref()
                    .is_some_and(|argument| argument_matches(exchange, argument, &payload["arg"]))
            } else {
                pending.key == key
            };
            if !matches || pending.sent.is_none() {
                continue;
            }
            if exchange == ExchangeId::Gateio
                && payload
                    .get("payload")
                    .is_some_and(|argument| pending.argument.as_ref() != Some(argument))
            {
                continue;
            }
            pending.confirmed = true;
        }
        let confirmed = self
            .pending
            .iter()
            .filter(|pending| pending.confirmed)
            .map(|pending| pending.weight)
            .sum();
        self.update(|state| {
            state
                .connections
                .get_mut(&self.tracker.id)
                .expect("connection")
                .info
                .subscriptions_confirmed = confirmed
        });
        Ok(true)
    }
    pub fn disconnected(&self, error: Option<String>) {
        self.update(|state| {
            let record = state
                .connections
                .get_mut(&self.tracker.id)
                .expect("connection");
            record.info.connected = false;
            record.info.subscriptions_confirmed = 0;
            record.disconnected = true;
            #[cfg(feature = "orderbook")]
            if let Some(books) = &self.tracker.monitor.books {
                books.invalidate(
                    self.tracker.monitor.identity,
                    self.tracker.id,
                    self.epoch,
                    None,
                );
            }
            if error.is_some() {
                record.info.last_error = error;
            }
            state.synced.retain(|_, owner| owner.0 != self.tracker.id);
        });
    }
}
impl Drop for ConnectionAttempt {
    fn drop(&mut self) {
        self.disconnected(None);
    }
}

pub(crate) fn track(feed: &mut crate::exchange::ExchangeFeed, url: &str, books: Vec<String>) {
    feed.retry_progress = Some(Default::default());
    if let Some(monitor) = &feed.monitor {
        let mut endpoint = url::Url::parse(url).expect("adapter websocket URL");
        endpoint.set_query(None);
        feed.connection_tracker = Some(monitor.register(endpoint.to_string(), books));
    }
}

pub(crate) fn begin(feed: &mut crate::exchange::ExchangeFeed) -> Option<ConnectionAttempt> {
    let attempt = feed
        .connection_tracker
        .as_ref()
        .map(ConnectionTracker::begin);
    feed.connection_epoch = attempt.as_ref().map(|attempt| attempt.epoch);
    attempt
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::{FeedId, FeedIdentity, FeedState};
    use cryptofeed_core::exchange::ExchangeId;
    use serde_json::json;

    fn monitor(exchange: ExchangeId, books: Vec<String>) -> Arc<FeedMonitor> {
        let monitor = Arc::new(FeedMonitor::new(
            FeedIdentity {
                id: FeedId::allocate(),
                generation: 1,
            },
            exchange,
            None,
        ));
        monitor.configure(1, books);
        monitor.lifecycle(FeedState::Started);
        monitor
    }

    #[test]
    fn acknowledgements_and_books_are_distinct_readiness_steps() {
        let monitor = monitor(ExchangeId::Okx, vec!["BTC-USDT".into()]);
        let tracker = monitor.register(
            "wss://ws.okx.com/ws/v5/public".into(),
            vec!["BTC-USDT".into()],
        );
        let mut attempt = tracker.begin();
        attempt.connected();
        let messages = attempt.prepare_messages(vec![
            json!({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}).to_string(),
        ]);
        let ack = json!({"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}});
        attempt.control(&ack).unwrap(); // Unsent requests cannot be confirmed.
        assert_eq!(monitor.snapshot().connections[0].subscriptions_confirmed, 0);
        attempt.sent(&messages[0], Instant::now());
        attempt.control(&ack).unwrap();
        assert_eq!(monitor.snapshot().state, FeedState::Subscribed);
        monitor.book_synced(tracker.id, attempt.epoch, "BTC-USDT");
        assert!(monitor.snapshot().is_ready());
        monitor.invalidate_book(tracker.id, attempt.epoch, "BTC-USDT");
        assert!(!monitor.snapshot().is_ready());
    }

    #[test]
    fn reconnect_and_stale_attempts_cannot_retain_or_manufacture_readiness() {
        let monitor = monitor(ExchangeId::Binance, vec![]);
        let tracker = monitor.register("wss://stream.binance.com/stream".into(), vec![]);
        let mut old = tracker.begin();
        old.connected();
        let messages = old.prepare_messages(vec![
            json!({"method":"SUBSCRIBE","params":["btcusdt@aggTrade"],"id":1}).to_string(),
        ]);
        old.sent(&messages[0], Instant::now());
        old.control(&json!({"result":null,"id":1})).unwrap();
        assert!(monitor.snapshot().is_ready());
        let mut current = tracker.begin();
        assert!(!monitor.snapshot().is_ready());
        current.connected();
        let messages = current.prepare_messages(messages);
        current.sent(&messages[0], Instant::now());
        old.control(&json!({"result":null,"id":1})).unwrap();
        assert_eq!(monitor.snapshot().connections[0].subscriptions_confirmed, 0);
        current.control(&json!({"result":null,"id":1})).unwrap();
        drop(old);
        assert!(monitor.snapshot().is_ready());
        monitor.lifecycle(FeedState::Stopping);
        current.control(&json!({"result":null,"id":1})).unwrap();
        assert_eq!(monitor.snapshot().state, FeedState::Stopping);
    }

    #[test]
    fn every_connection_must_confirm_and_data_alone_is_not_readiness() {
        let monitor = monitor(ExchangeId::Okx, vec![]);
        monitor.configure(2, vec![]);
        let mut attempts = Vec::new();
        for symbol in ["BTC-USDT", "ETH-USDT"] {
            let tracker = monitor.register("wss://ws.okx.com/ws/v5/public".into(), vec![]);
            let mut attempt = tracker.begin();
            attempt.connected();
            let messages = attempt.prepare_messages(vec![
                json!({"op":"subscribe","args":[{"channel":"trades","instId":symbol}]}).to_string(),
            ]);
            attempt.sent(&messages[0], Instant::now());
            attempts.push(attempt);
        }
        monitor.observe(Channel::Trade, &Symbol::spot("BTC", "USDT"), None);
        attempts[0]
            .control(&json!({"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"}}))
            .unwrap();
        assert!(!monitor.snapshot().is_ready());
        assert_eq!(monitor.snapshot().observed_events, 1);
        attempts[1]
            .control(&json!({"event":"subscribe","arg":{"channel":"trades","instId":"ETH-USDT"}}))
            .unwrap();
        assert!(monitor.snapshot().is_ready());
        drop(attempts.remove(0));
        assert!(!monitor.snapshot().is_ready());
    }

    #[test]
    fn correlated_batch_confirms_its_topics_and_rejection_withdraws_ready() {
        let monitor = monitor(ExchangeId::Bybit, vec![]);
        let tracker = monitor.register("wss://stream.bybit.com/v5/public/spot".into(), vec![]);
        let mut attempt = tracker.begin();
        attempt.connected();
        let messages = attempt.prepare_messages(vec![
            json!({"op":"subscribe","args":["publicTrade.BTCUSDT","publicTrade.ETHUSDT"]})
                .to_string(),
        ]);
        attempt.sent(&messages[0], Instant::now());
        let request: Value = serde_json::from_str(&messages[0]).unwrap();
        attempt
            .control(&json!({"op":"subscribe","success":true,"req_id":"unknown"}))
            .unwrap();
        assert_eq!(monitor.snapshot().connections[0].subscriptions_confirmed, 0);
        attempt
            .control(&json!({"op":"subscribe","success":true,"req_id":request["req_id"]}))
            .unwrap();
        assert_eq!(monitor.snapshot().connections[0].subscriptions_confirmed, 2);
        assert!(monitor.snapshot().is_ready());
        assert!(attempt.control(&json!({"op":"subscribe","success":false,"req_id":request["req_id"],"ret_msg":"invalid topic"})).is_err());
        assert!(!monitor.snapshot().is_ready());
    }

    #[test]
    fn argument_acknowledgements_require_the_requested_product_topic_and_symbol() {
        let monitor = monitor(ExchangeId::Bitget, vec![]);
        let tracker = monitor.register("wss://ws.bitget.com/v3/ws/public".into(), vec![]);
        let mut attempt = tracker.begin();
        attempt.connected();
        let messages = attempt.prepare_messages(vec![json!({"op":"subscribe","args":[{"instType":"spot","topic":"ticker","symbol":"BTCUSDT"},{"instType":"spot","topic":"ticker","symbol":"ETHUSDT"}]}).to_string()]);
        attempt.sent(&messages[0], Instant::now());
        for arg in [
            json!({"instType":"spot","topic":"ticker","symbol":"OTHERUSDT"}),
            json!({"instType":"usdt-futures","topic":"ticker","symbol":"BTCUSDT"}),
            json!({"instType":"spot","topic":"publicTrade","symbol":"BTCUSDT"}),
        ] {
            attempt
                .control(&json!({"event":"subscribe","arg":arg}))
                .unwrap();
        }
        assert_eq!(monitor.snapshot().connections[0].subscriptions_confirmed, 0);
        for symbol in ["BTCUSDT", "BTCUSDT", "ETHUSDT"] {
            attempt.control(&json!({"event":"subscribe","arg":{"instType":"spot","topic":"ticker","symbol":symbol}})).unwrap();
        }
        assert_eq!(monitor.snapshot().connections[0].subscriptions_confirmed, 2);
        assert!(monitor.snapshot().is_ready());
    }

    #[test]
    fn stale_book_snapshot_and_degraded_connections_cannot_report_ready() {
        let monitor = monitor(ExchangeId::Okx, vec!["BTC-USDT".into()]);
        let tracker = monitor.register(
            "wss://ws.okx.com/ws/v5/public".into(),
            vec!["BTC-USDT".into()],
        );
        let old = tracker.begin();
        let mut current = tracker.begin();
        current.connected();
        let messages = current.prepare_messages(vec![
            json!({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}).to_string(),
        ]);
        current.sent(&messages[0], Instant::now());
        current
            .control(&json!({"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}}))
            .unwrap();
        monitor.book_synced(tracker.id, old.epoch, "BTC-USDT");
        assert!(!monitor.snapshot().is_ready());
        monitor.lifecycle(FeedState::Degraded {
            error: "another route failed".into(),
        });
        monitor.book_synced(tracker.id, current.epoch, "BTC-USDT");
        assert!(!monitor.snapshot().is_ready());
        assert!(matches!(
            monitor.snapshot().state,
            FeedState::Degraded { .. }
        ));
    }

    #[test]
    fn concurrent_stop_cannot_be_followed_by_a_stale_ready_notification() {
        for _ in 0..32 {
            let (sender, mut receiver) = broadcast::channel(32);
            let monitor = Arc::new(FeedMonitor::new(
                FeedIdentity {
                    id: FeedId::allocate(),
                    generation: 1,
                },
                ExchangeId::Okx,
                Some(sender),
            ));
            monitor.configure(1, vec![]);
            monitor.lifecycle(FeedState::Started);
            let tracker = monitor.register("wss://ws.okx.com/ws/v5/public".into(), vec![]);
            let mut attempt = tracker.begin();
            attempt.connected();
            let messages = attempt.prepare_messages(vec![
                json!({"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]})
                    .to_string(),
            ]);
            attempt.sent(&messages[0], Instant::now());
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(|| { barrier.wait(); attempt.control(&json!({"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"}})).unwrap(); });
                scope.spawn(|| {
                    barrier.wait();
                    monitor.lifecycle(FeedState::Stopping);
                });
            });
            let mut stopping = false;
            while let Ok(FeedStatus::Lifecycle { state, .. }) = receiver.try_recv() {
                if stopping {
                    assert!(!matches!(
                        state,
                        FeedState::Ready | FeedState::Subscribed | FeedState::Reconnecting
                    ));
                }
                if state == FeedState::Stopping {
                    stopping = true;
                }
            }
            assert!(stopping);
            assert_eq!(monitor.snapshot().state, FeedState::Stopping);
        }
    }

    #[test]
    fn extra_acknowledgement_metadata_does_not_change_requested_identity() {
        let monitor = monitor(ExchangeId::Okx, vec![]);
        let tracker = monitor.register("wss://ws.okx.com/ws/v5/public".into(), vec![]);
        let mut attempt = tracker.begin();
        attempt.connected();
        let messages = attempt.prepare_messages(vec![
            json!({"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}).to_string(),
        ]);
        attempt.sent(&messages[0], Instant::now());
        attempt.control(&json!({"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT","instType":"SPOT"},"connId":"sanitized"})).unwrap();
        assert!(monitor.snapshot().is_ready());
    }

    #[test]
    fn correlated_batches_do_not_accept_unknown_or_duplicate_replies() {
        for exchange in [ExchangeId::Bybit, ExchangeId::Gateio] {
            let monitor = monitor(exchange, vec![]);
            let tracker = monitor.register("wss://test/".into(), vec![]);
            let mut attempt = tracker.begin();
            attempt.connected();
            let inputs = if exchange == ExchangeId::Bybit {
                vec![
                    json!({"op":"subscribe","args":["publicTrade.BTCUSDT"]}).to_string(),
                    json!({"op":"subscribe","args":["publicTrade.ETHUSDT"]}).to_string(),
                ]
            } else {
                vec![
                    json!({"event":"subscribe","channel":"spot.trades","payload":["BTC_USDT"]})
                        .to_string(),
                    json!({"event":"subscribe","channel":"spot.trades","payload":["ETH_USDT"]})
                        .to_string(),
                ]
            };
            let messages = attempt.prepare_messages(inputs);
            for message in &messages {
                attempt.sent(message, Instant::now());
            }
            let first: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
            let ack = if exchange == ExchangeId::Bybit {
                json!({"op":"subscribe","success":true,"req_id":first["req_id"]})
            } else {
                json!({"event":"subscribe","channel":"spot.trades","id":first["id"],"result":{"status":"success"}})
            };
            attempt.control(&ack).unwrap();
            attempt.control(&ack).unwrap();
            assert_eq!(monitor.snapshot().connections[0].subscriptions_confirmed, 1);
            assert!(!monitor.snapshot().is_ready());
        }
    }
}
