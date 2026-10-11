//! Public HTTP JSON-result observation; no request/response headers or wire bodies.
use super::{
    encode,
    raw::{RawSnapshotFailure, sanitize},
};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;
use url::Url;
mod file;
pub use file::{HttpRecordingReader, HttpRecordingWriter, record_http_stream};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum HttpOutcome {
    Json {
        value: Value,
        redacted: bool,
    },
    Failure {
        kind: RawSnapshotFailure,
        retry_after: Option<std::time::Duration>,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum HttpObservationKind {
    Request {
        request_id: u64,
        exchange: ExchangeId,
        url: String,
    },
    Finished {
        request_id: u64,
        outcome: HttpOutcome,
    },
    Cancelled {
        request_id: u64,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpObservation {
    pub sequence: u64,
    pub elapsed_ns: u64,
    pub observed_ts: f64,
    pub kind: HttpObservationKind,
}
fn invalid() -> Error {
    Error::MalformedData("invalid/incomplete/unsafe public HTTP recording".into())
}
fn public_url(exchange: ExchangeId, input: &str) -> Result<()> {
    if input.len() > 2048 {
        return Err(invalid());
    }
    let url = Url::parse(input).map_err(|_| invalid())?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port().is_some_and(|port| port != 443)
    {
        return Err(invalid());
    }
    let host = url.host_str().ok_or_else(invalid)?;
    let path = url.path();
    let allowed = match exchange {
        ExchangeId::Binance => {
            let operation = if matches!(host, "api.binance.com" | "data-api.binance.vision") {
                path.strip_prefix("/api/v3/")
            } else if host == "fapi.binance.com" {
                path.strip_prefix("/fapi/v1/")
            } else if host == "dapi.binance.com" {
                path.strip_prefix("/dapi/v1/")
            } else {
                None
            };
            operation.is_some_and(|op| {
                matches!(
                    op,
                    "exchangeInfo"
                        | "ticker/bookTicker"
                        | "depth"
                        | "trades"
                        | "aggTrades"
                        | "klines"
                        | "fundingRate"
                )
            })
        }
        ExchangeId::Bitget => {
            host == "api.bitget.com"
                && path.strip_prefix("/api/v3/market/").is_some_and(|op| {
                    matches!(
                        op,
                        "instruments"
                            | "orderbook"
                            | "tickers"
                            | "fills"
                            | "history-candles"
                            | "history-fund-rate"
                    )
                })
        }
        ExchangeId::Bybit => {
            host == "api.bybit.com"
                && path.strip_prefix("/v5/market/").is_some_and(|op| {
                    matches!(
                        op,
                        "instruments-info"
                            | "orderbook"
                            | "tickers"
                            | "recent-trade"
                            | "kline"
                            | "funding/history"
                    )
                })
        }
        ExchangeId::Okx => {
            host == "openapi.okx.com"
                && (matches!(
                    path,
                    "/api/v5/public/instruments" | "/api/v5/public/funding-rate-history"
                ) || path.strip_prefix("/api/v5/market/").is_some_and(|op| {
                    matches!(
                        op,
                        "books" | "ticker" | "trades" | "history-trades" | "history-candles"
                    )
                }))
        }
        ExchangeId::Gateio => {
            host == "api.gateio.ws"
                && [
                    "/api/v4/spot/",
                    "/api/v4/futures/usdt/",
                    "/api/v4/futures/btc/",
                    "/api/v4/delivery/usdt/",
                ]
                .iter()
                .any(|prefix| {
                    path.strip_prefix(prefix).is_some_and(|op| {
                        matches!(
                            op,
                            "currency_pairs"
                                | "contracts"
                                | "order_book"
                                | "tickers"
                                | "trades"
                                | "candlesticks"
                                | "funding_rate"
                        )
                    })
                })
        }
        _ => false,
    };
    if !allowed {
        return Err(invalid());
    }
    let mut seen = std::collections::HashSet::new();
    for (key, value) in url.query_pairs() {
        if !seen.insert(key.to_string())
            || value.len() > 1024
            || !matches!(
                key.as_ref(),
                "symbol"
                    | "symbols"
                    | "category"
                    | "status"
                    | "cursor"
                    | "limit"
                    | "instType"
                    | "instId"
                    | "instFamily"
                    | "currency_pair"
                    | "contract"
                    | "interval"
                    | "start"
                    | "end"
                    | "startTime"
                    | "endTime"
                    | "from"
                    | "to"
                    | "after"
                    | "before"
                    | "fromId"
                    | "type"
                    | "with_id"
                    | "sz"
                    | "bar"
                    | "page"
                    | "offset"
            )
        {
            return Err(invalid());
        }
    }
    Ok(())
}
fn validate_outcome(outcome: &HttpOutcome) -> Result<()> {
    match outcome {
        HttpOutcome::Json { value, redacted } => {
            let mut changed = false;
            let clean = sanitize(value, 0, &mut changed).map_err(|_| invalid())?;
            if clean != *value || changed && !redacted {
                return Err(invalid());
            }
        }
        HttpOutcome::Failure {
            kind: RawSnapshotFailure::HttpStatus { status },
            ..
        } if !(100..=599).contains(status) || (200..=299).contains(status) => return Err(invalid()),
        HttpOutcome::Failure {
            kind,
            retry_after: Some(_),
        } if !matches!(kind, RawSnapshotFailure::HttpStatus { .. }) => return Err(invalid()),
        _ => {}
    }
    Ok(())
}
struct State {
    sequence: u64,
    requests: u64,
    failed: bool,
}
struct Shared {
    id: u64,
    started: tokio::time::Instant,
    maximum: usize,
    state: Mutex<State>,
    failure: tokio::sync::Notify,
}
#[derive(Clone)]
pub struct HttpCaptureHandle(Arc<Shared>, mpsc::Sender<HttpObservation>);
pub struct HttpCaptureReceiver {
    shared: Arc<Shared>,
    receiver: mpsc::Receiver<HttpObservation>,
}
pub fn http_capture_channel(
    capacity: usize,
    max_record_bytes: usize,
) -> Result<(HttpCaptureHandle, HttpCaptureReceiver)> {
    if !(1..=64).contains(&capacity) || !(256..=64 * 1024 * 1024).contains(&max_record_bytes) {
        return Err(Error::InvalidConfiguration(
            "HTTP capture capacity must be 1..=64 and record bytes 256..=64MiB".into(),
        ));
    }
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let shared = Arc::new(Shared {
        id: NEXT.fetch_add(1, Ordering::Relaxed),
        failure: tokio::sync::Notify::new(),
        started: tokio::time::Instant::now(),
        maximum: max_record_bytes,
        state: Mutex::new(State {
            sequence: 0,
            requests: 0,
            failed: false,
        }),
    });
    let (sender, receiver) = mpsc::channel(capacity);
    Ok((
        HttpCaptureHandle(shared.clone(), sender),
        HttpCaptureReceiver { shared, receiver },
    ))
}
impl HttpCaptureReceiver {
    pub async fn recv(&mut self) -> Result<Option<HttpObservation>> {
        if self.shared.state.lock().expect("HTTP capture lock").failed {
            return Err(invalid());
        }
        let value = tokio::select! {
            _ = self.shared.failure.notified() => return Err(invalid()),
            value = self.receiver.recv() => value,
        };
        if self.shared.state.lock().expect("HTTP capture lock").failed {
            return Err(invalid());
        }
        Ok(value)
    }
}
impl HttpCaptureHandle {
    pub(crate) fn id(&self) -> u64 {
        self.0.id
    }
    fn emit_locked(&self, state: &mut State, kind: HttpObservationKind, observed_ts: f64) {
        if state.failed {
            return;
        }
        let Ok(elapsed_ns) = u64::try_from(self.0.started.elapsed().as_nanos()) else {
            state.failed = true;
            self.0.failure.notify_one();
            return;
        };
        let record = HttpObservation {
            sequence: state.sequence + 1,
            elapsed_ns,
            observed_ts,
            kind,
        };
        if encode(&record, self.0.maximum).is_err() || self.1.try_send(record).is_err() {
            state.failed = true;
            self.0.failure.notify_one();
            return;
        }
        state.sequence += 1;
    }
    fn emit(&self, kind: HttpObservationKind, observed_ts: f64) {
        let mut state = self.0.state.lock().expect("HTTP capture lock");
        self.emit_locked(&mut state, kind, observed_ts);
    }
    pub(crate) fn begin(&self, url: &str, exchange: ExchangeId) -> Option<RequestGuard> {
        let mut state = self.0.state.lock().expect("HTTP capture lock");
        if state.failed {
            return None;
        }
        if public_url(exchange, url).is_err() {
            state.failed = true;
            self.0.failure.notify_one();
            return None;
        }
        state.requests += 1;
        let id = state.requests;
        self.emit_locked(
            &mut state,
            HttpObservationKind::Request {
                request_id: id,
                exchange,
                url: url.into(),
            },
            crate::runtime::current_timestamp(),
        );
        (!state.failed).then(|| RequestGuard {
            capture: self.clone(),
            id,
            finished: false,
        })
    }
}
pub(crate) struct RequestGuard {
    capture: HttpCaptureHandle,
    id: u64,
    finished: bool,
}
impl RequestGuard {
    pub(crate) fn finish(mut self, result: &Result<Value>) {
        self.finished = true;
        let observed_ts = crate::runtime::current_timestamp();
        let outcome = match result {
            Ok(value) => {
                let mut redacted = false;
                let clean = match sanitize(value, 0, &mut redacted) {
                    Ok(value) => value,
                    Err(_) => {
                        self.capture
                            .0
                            .state
                            .lock()
                            .expect("HTTP capture lock")
                            .failed = true;
                        self.capture.0.failure.notify_one();
                        return;
                    }
                };
                HttpOutcome::Json {
                    value: clean,
                    redacted,
                }
            }
            Err(error) => HttpOutcome::Failure {
                kind: RawSnapshotFailure::from_error(error),
                retry_after: match error {
                    Error::HttpStatus { retry_after, .. } => *retry_after,
                    _ => None,
                },
            },
        };
        self.capture.emit(
            HttpObservationKind::Finished {
                request_id: self.id,
                outcome,
            },
            observed_ts,
        );
    }
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.capture.emit(
                HttpObservationKind::Cancelled {
                    request_id: self.id,
                },
                crate::runtime::current_timestamp(),
            );
        }
    }
}
#[cfg(test)]
mod tests;
