//! Validated payload injection, not a transport/cache-timer simulation.
use super::*;
use crate::recording::RecordingSummary;
use std::collections::{BTreeMap, HashMap, VecDeque};
use tokio::io::AsyncBufRead;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpReplayReport {
    pub source: RecordingSummary,
    pub requests: u64,
    pub consumed: u64,
    /// Original captured starts with no outcome, not active replay operations.
    pub unfinished: u64,
    pub cancelled: u64,
    /// Invalid/missing/exhausted/over-budget request attempts; none consume input.
    pub mismatches: u64,
}
struct Entry {
    outcome: Option<End>,
    body_bytes: usize,
}
enum End {
    Finished(HttpOutcome),
    Cancelled,
}
struct Responses {
    entries: VecDeque<Entry>,
    catalog_namespaces: std::collections::HashSet<(Option<u64>, Option<u64>)>,
}
struct State {
    responses: HashMap<(ExchangeId, String), Responses>,
    report: HttpReplayReport,
}
struct Shared {
    id: u64,
    state: Mutex<State>,
}
/// Clones share one consumption cursor. Build a new instance from a fresh reader
/// to restart. Invalid requests do not skip entries or affect unrelated URL keys.
#[derive(Clone)]
pub struct HttpReplay(Arc<Shared>);
impl std::fmt::Debug for HttpReplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpReplay(validated public payloads)")
    }
}
struct Count(usize);
impl std::io::Write for Count {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(super) async fn load<R: AsyncBufRead + Unpin>(
    reader: &mut HttpRecordingReader<R>,
) -> Result<HttpReplay> {
    let mut requests = BTreeMap::new();
    while let Some(record) = reader.next_observation().await? {
        match record.kind {
            HttpObservationKind::Request {
                request_id,
                exchange,
                url,
            } => {
                requests.insert(
                    request_id,
                    (
                        (exchange, url),
                        Entry {
                            outcome: None,
                            body_bytes: 0,
                        },
                    ),
                );
            }
            HttpObservationKind::Finished {
                request_id,
                outcome,
            } => {
                let entry = &mut requests
                    .get_mut(&request_id)
                    .expect("validated request ID")
                    .1;
                if let HttpOutcome::Json { value, .. } = &outcome {
                    let mut count = Count(0);
                    serde_json::to_writer(&mut count, value).map_err(|_| invalid())?;
                    entry.body_bytes = count.0;
                }
                entry.outcome = Some(End::Finished(outcome));
            }
            HttpObservationKind::Cancelled { request_id } => {
                requests
                    .get_mut(&request_id)
                    .expect("validated request ID")
                    .1
                    .outcome = Some(End::Cancelled)
            }
        }
    }
    let mut report = HttpReplayReport {
        source: reader.summary().ok_or_else(invalid)?,
        requests: requests.len() as u64,
        consumed: 0,
        unfinished: 0,
        cancelled: 0,
        mismatches: 0,
    };
    let mut responses: HashMap<_, Responses> = HashMap::new();
    for (_, (key, entry)) in requests {
        if entry.outcome.is_none() {
            report.unfinished += 1;
        }
        if matches!(entry.outcome, Some(End::Cancelled)) {
            report.cancelled += 1;
        }
        responses
            .entry(key)
            .or_insert_with(|| Responses {
                entries: VecDeque::new(),
                catalog_namespaces: Default::default(),
            })
            .entries
            .push_back(entry);
    }
    static NEXT: AtomicU64 = AtomicU64::new(1);
    Ok(HttpReplay(Arc::new(Shared {
        id: NEXT.fetch_add(1, Ordering::Relaxed),
        state: Mutex::new(State { responses, report }),
    })))
}
impl HttpReplay {
    pub fn report(&self) -> HttpReplayReport {
        self.0.state.lock().expect("HTTP replay lock").report
    }
    /// Audit at the caller's chosen end: all archived starts must be consumed and
    /// no unmatched attempt may have occurred. Captured prefix reason is retained.
    pub fn verify_complete(&self) -> Result<HttpReplayReport> {
        let report = self.report();
        if report.consumed != report.requests || report.mismatches != 0 {
            return Err(Error::Protocol(
                "HTTP replay has unmatched or unconsumed requests".into(),
            ));
        }
        Ok(report)
    }
    pub(crate) fn id(&self) -> u64 {
        self.0.id
    }
    pub(crate) fn check_catalog_fetch(
        &self,
        exchange: ExchangeId,
        url: &str,
        namespace: (Option<u64>, Option<u64>),
        refresh: bool,
    ) -> Result<()> {
        let mut state = self.0.state.lock().expect("HTTP replay lock");
        if !refresh
            && state
                .responses
                .get(&(exchange, url.to_owned()))
                .is_some_and(|responses| responses.catalog_namespaces.contains(&namespace))
        {
            state.report.mismatches += 1;
            return Err(Error::InvalidConfiguration("HTTP replay catalog cache expired or was evicted; retain the snapshot or explicitly refresh".into()));
        }
        Ok(())
    }
    pub(crate) fn catalog_loaded(
        &self,
        exchange: ExchangeId,
        url: &str,
        namespace: (Option<u64>, Option<u64>),
    ) {
        let mut state = self.0.state.lock().expect("HTTP replay lock");
        if let Some(responses) = state.responses.get_mut(&(exchange, url.to_owned())) {
            responses.catalog_namespaces.insert(namespace);
        }
    }
    pub(crate) fn take(&self, exchange: ExchangeId, url: &str, max_bytes: usize) -> Result<Value> {
        let mut state = self.0.state.lock().expect("HTTP replay lock");
        if public_url(exchange, url).is_err() {
            state.report.mismatches += 1;
            return Err(Error::InvalidConfiguration(
                "HTTP replay request scope is invalid".into(),
            ));
        }
        let key = (exchange, url.to_owned());
        let entry = state
            .responses
            .get(&key)
            .and_then(|responses| responses.entries.front());
        let error = match entry {
            None => Some("HTTP replay request is missing or exhausted"),
            Some(entry) if entry.outcome.is_none() => {
                Some("HTTP replay request has no captured outcome")
            }
            Some(entry) if entry.body_bytes > max_bytes => {
                Some("HTTP replay response exceeds request body budget")
            }
            _ => None,
        };
        if let Some(error) = error {
            state.report.mismatches += 1;
            return Err(Error::InvalidConfiguration(error.into()));
        }
        let entry = state
            .responses
            .get_mut(&key)
            .expect("matched key")
            .entries
            .pop_front()
            .expect("matched entry");
        state.report.consumed += 1;
        match entry.outcome.expect("completed entry") {
            End::Finished(HttpOutcome::Failure {
                configuration_error: Some(true),
                ..
            }) => Err(Error::InvalidConfiguration(
                "recorded HTTP configuration failure".into(),
            )),
            End::Finished(HttpOutcome::Json { value, .. }) => Ok(value),
            End::Finished(HttpOutcome::Failure {
                kind: RawSnapshotFailure::HttpStatus { status },
                retry_after,
                ..
            }) => Err(Error::HttpStatus {
                status,
                retry_after,
            }),
            End::Finished(HttpOutcome::Failure {
                kind: RawSnapshotFailure::Transport,
                ..
            }) => Err(Error::Transport("recorded HTTP transport failure".into())),
            End::Finished(HttpOutcome::Failure {
                kind: RawSnapshotFailure::MalformedResponse,
                ..
            }) => Err(Error::MalformedData(
                "recorded malformed HTTP JSON result".into(),
            )),
            End::Finished(HttpOutcome::Failure { .. }) => {
                Err(Error::Protocol("recorded HTTP failure".into()))
            }
            End::Cancelled => Err(Error::InvalidConfiguration(
                "recorded HTTP request was cancelled".into(),
            )),
        }
    }
}
#[cfg(test)]
mod tests;
