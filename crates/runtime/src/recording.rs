//! Versioned, bounded normalized-event recording and offline replay.
pub mod http;
pub mod raw;
mod sink;
use crate::feed::{AnchoredEnvelope, EventAnchor, FeedEnvelope, FeedEvent, FeedIdentity};
use cryptofeed_core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{future::Future, io::Write, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt},
    sync::{broadcast, watch},
};

const FORMAT: &str = "cryptofeed-rs.normalized";
const FOOTER_RESERVE: usize = 256;
const IO_DEADLINE: Duration = Duration::from_secs(5);
#[derive(Clone, Copy, Debug)]
pub struct RecordingLimits {
    max_events: u64,
    max_bytes: u64,
    max_record_bytes: usize,
}
impl Default for RecordingLimits {
    fn default() -> Self {
        Self {
            max_events: 100_000,
            max_bytes: 256 * 1024 * 1024,
            max_record_bytes: 1024 * 1024,
        }
    }
}
impl RecordingLimits {
    pub fn new(max_events: u64, max_bytes: u64, max_record_bytes: usize) -> Result<Self> {
        if max_events == 0
            || max_record_bytes < 256
            || max_record_bytes as u64 > max_bytes.saturating_sub(FOOTER_RESERVE as u64)
        {
            return Err(Error::InvalidConfiguration(
                "recording limits require events>0, record>=256 bytes and room for footer".into(),
            ));
        }
        Ok(Self {
            max_events,
            max_bytes,
            max_record_bytes,
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingEnd {
    Complete,
    Stopped,
    LimitReached,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedEvent {
    pub sequence: u64,
    pub elapsed_ns: u64,
    pub identity: FeedIdentity,
    pub event: FeedEvent,
}
/// A normalized v2 record retaining its original producer anchor.
#[derive(Clone, Debug)]
pub struct AnchoredRecordedEvent {
    pub anchor: EventAnchor,
    pub record: RecordedEvent,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header {
        format: String,
        version: u16,
    },
    Event {
        record: Box<RecordedEvent>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        anchor: Option<EventAnchor>,
    },
    End {
        events: u64,
        reason: RecordingEnd,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordingSummary {
    pub events: u64,
    pub bytes: u64,
    pub end: RecordingEnd,
}
fn invalid() -> Error {
    Error::MalformedData("invalid/incomplete recording or disabled event category".into())
}
fn limit() -> Error {
    Error::InvalidConfiguration("recording resource limit reached".into())
}
fn io(error: std::io::Error) -> Error {
    Error::Transport(format!("recording I/O: {error}"))
}
struct Buffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("record size limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode<T: Serialize>(record: &T, maximum: usize) -> Result<Vec<u8>> {
    let mut buffer = Buffer {
        bytes: Vec::new(),
        limit: maximum.saturating_sub(1),
    };
    serde_json::to_writer(&mut buffer, record).map_err(|_| limit())?;
    buffer.bytes.push(b'\n');
    Ok(buffer.bytes)
}
async fn bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    bytes: &mut u64,
    limits: RecordingLimits,
) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let buffer = reader.fill_buf().await.map_err(io)?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(invalid())
            };
        }
        let count = buffer
            .iter()
            .position(|b| *b == b'\n')
            .map_or(buffer.len(), |i| i + 1);
        if count > limits.max_record_bytes.saturating_sub(line.len())
            || count as u64 > limits.max_bytes.saturating_sub(*bytes)
        {
            return Err(limit());
        }
        let end = buffer[count - 1] == b'\n';
        line.extend_from_slice(&buffer[..count]);
        reader.consume(count);
        *bytes += count as u64;
        if end {
            return Ok(Some(line));
        }
    }
}
fn validate_event(record: &RecordedEvent) -> Result<()> {
    if record.identity.id.as_u64() == 0 || record.identity.generation == 0 {
        return Err(invalid());
    }
    // serde_json otherwise silently turns non-finite floats into null.
    let times: Vec<f64> = match record.event {
        #[cfg(feature = "ticker")]
        FeedEvent::Ticker(ref v) => vec![v.exchange_ts, v.received_ts],
        #[cfg(feature = "trade")]
        FeedEvent::Trade(ref v) => vec![v.exchange_ts, v.received_ts],
        #[cfg(feature = "orderbook")]
        FeedEvent::L2Book(ref v) => vec![v.exchange_ts(), v.received_ts()],
        #[cfg(feature = "orderbook")]
        FeedEvent::L1Book(ref v) => vec![v.exchange_ts, v.received_ts],
        #[cfg(feature = "candles")]
        FeedEvent::Candle(ref v) => vec![v.exchange_ts, v.received_ts, v.start, v.end],
        #[cfg(feature = "funding")]
        FeedEvent::Funding(ref v) => {
            let mut t = vec![v.exchange_ts, v.received_ts];
            t.extend(v.next_funding_time);
            t
        }
        #[cfg(feature = "markprice")]
        FeedEvent::MarkPrice(ref v) => {
            let mut t = vec![v.exchange_ts, v.received_ts];
            t.extend(v.next_funding_time);
            t
        }
        #[cfg(feature = "liquidations")]
        FeedEvent::Liquidation(ref v) => vec![v.exchange_ts, v.received_ts],
        #[cfg(feature = "openinterest")]
        FeedEvent::OpenInterest(ref v) => vec![v.exchange_ts, v.received_ts],
        #[cfg(feature = "index")]
        FeedEvent::IndexPrice(ref v) => vec![v.exchange_ts, v.received_ts],
    };
    if times.iter().any(|t| !t.is_finite()) {
        return Err(invalid());
    }
    Ok(())
}
/// Generic async writer: callers choose files (prefer create_new), memory or pipes.
/// Cancelled/failed writes poison the writer, preventing a misleading footer.
pub struct RecordingWriter<W> {
    writer: W,
    limits: RecordingLimits,
    events: u64,
    bytes: u64,
    elapsed_ns: u64,
    version: u16,
    poisoned: bool,
}
impl<W: AsyncWrite + Unpin> RecordingWriter<W> {
    pub async fn new(writer: W, limits: RecordingLimits) -> Result<Self> {
        Self::with_version(writer, limits, 1).await
    }
    /// Creates a v2 tape requiring an anchor on every event. Legacy v1 output
    /// and public FeedEnvelope/RecordedEvent construction remain unchanged.
    pub async fn new_anchored(writer: W, limits: RecordingLimits) -> Result<Self> {
        Self::with_version(writer, limits, 2).await
    }
    async fn with_version(writer: W, limits: RecordingLimits, version: u16) -> Result<Self> {
        let mut this = Self {
            writer,
            limits,
            events: 0,
            bytes: 0,
            elapsed_ns: 0,
            version,
            poisoned: false,
        };
        let bytes = encode(
            &Record::Header {
                format: FORMAT.into(),
                version,
            },
            limits.max_record_bytes,
        )?;
        this.write_line(&bytes).await?;
        Ok(this)
    }
    async fn write_line(&mut self, bytes: &[u8]) -> Result<()> {
        if self.poisoned {
            return Err(invalid());
        }
        self.poisoned = true;
        tokio::time::timeout(IO_DEADLINE, self.writer.write_all(bytes))
            .await
            .map_err(|_| Error::Transport("recording write deadline".into()))?
            .map_err(io)?;
        self.bytes += bytes.len() as u64;
        self.poisoned = false;
        Ok(())
    }
    /// Returns false on a configured event/byte/line limit, without writing a prefix.
    pub async fn append(&mut self, envelope: FeedEnvelope, elapsed: Duration) -> Result<bool> {
        self.append_inner(envelope, None, elapsed).await
    }
    pub async fn append_anchored(
        &mut self,
        envelope: AnchoredEnvelope,
        elapsed: Duration,
    ) -> Result<bool> {
        self.append_inner(
            FeedEnvelope {
                identity: envelope.anchor.identity,
                event: envelope.event,
            },
            Some(envelope.anchor),
            elapsed,
        )
        .await
    }
    async fn append_inner(
        &mut self,
        envelope: FeedEnvelope,
        anchor: Option<EventAnchor>,
        elapsed: Duration,
    ) -> Result<bool> {
        if self.poisoned || (self.version == 2) != anchor.is_some() {
            return Err(invalid());
        }
        if anchor.is_some_and(|a| a.connection == 0 || a.epoch == 0) {
            return Err(invalid());
        }
        if self.events >= self.limits.max_events {
            return Ok(false);
        }
        let elapsed_ns = u64::try_from(elapsed.as_nanos()).map_err(|_| invalid())?;
        if elapsed_ns < self.elapsed_ns {
            return Err(invalid());
        }
        let record = RecordedEvent {
            sequence: self.events + 1,
            elapsed_ns,
            identity: envelope.identity,
            event: envelope.event,
        };
        validate_event(&record)?;
        let bytes = match encode(
            &Record::Event {
                record: Box::new(record),
                anchor,
            },
            self.limits.max_record_bytes,
        ) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(false),
        };
        if bytes.len() as u64 + FOOTER_RESERVE as u64
            > self.limits.max_bytes.saturating_sub(self.bytes)
        {
            return Ok(false);
        }
        self.write_line(&bytes).await?;
        self.events += 1;
        self.elapsed_ns = elapsed_ns;
        Ok(true)
    }
    /// Flush confirms async writer acceptance, not filesystem fsync/durable storage.
    pub async fn finish(mut self, end: RecordingEnd) -> Result<RecordingSummary> {
        let bytes = encode(
            &Record::End {
                events: self.events,
                reason: end,
            },
            self.limits.max_record_bytes,
        )?;
        if bytes.len() as u64 > self.limits.max_bytes.saturating_sub(self.bytes) {
            return Err(limit());
        }
        self.write_line(&bytes).await?;
        tokio::time::timeout(IO_DEADLINE, self.writer.flush())
            .await
            .map_err(|_| Error::Transport("recording flush deadline".into()))?
            .map_err(io)?;
        Ok(RecordingSummary {
            events: self.events,
            bytes: self.bytes,
            end,
        })
    }
}
async fn stopped(receiver: &mut watch::Receiver<bool>) {
    let _ = receiver.wait_for(|value| *value).await;
}
/// Stream capture is strict about broadcast lag; dropped records invalidate the file.
pub async fn record_stream<W: AsyncWrite + Unpin>(
    writer: W,
    mut receiver: broadcast::Receiver<FeedEnvelope>,
    limits: RecordingLimits,
    mut shutdown: watch::Receiver<bool>,
) -> Result<RecordingSummary> {
    let started = tokio::time::Instant::now();
    let mut writer = RecordingWriter::new(writer, limits).await?;
    let end = loop {
        if *shutdown.borrow() {
            break RecordingEnd::Stopped;
        }
        let envelope = tokio::select! {biased;
            _=stopped(&mut shutdown)=>break RecordingEnd::Stopped,
            result=receiver.recv()=>match result {
                Ok(event)=>event,
                Err(broadcast::error::RecvError::Closed)=>break RecordingEnd::Complete,
                Err(broadcast::error::RecvError::Lagged(count))=>return Err(Error::Protocol(format!("recording lost {count} broadcast events"))),
            }
        };
        let appended = tokio::select! {biased;
            _=stopped(&mut shutdown)=>return Err(Error::Transport("recording interrupted during append".into())),
            result=writer.append(envelope,started.elapsed())=>result?,
        };
        if !appended || writer.events >= limits.max_events {
            break RecordingEnd::LimitReached;
        }
    };
    writer.finish(end).await
}

/// Stream capture is strict about broadcast lag; dropped records invalidate the file.
pub async fn record_anchored_stream<W: AsyncWrite + Unpin>(
    writer: W,
    mut receiver: broadcast::Receiver<AnchoredEnvelope>,
    limits: RecordingLimits,
    mut shutdown: watch::Receiver<bool>,
) -> Result<RecordingSummary> {
    let started = tokio::time::Instant::now();
    let mut writer = RecordingWriter::new_anchored(writer, limits).await?;
    let end = loop {
        if *shutdown.borrow() {
            break RecordingEnd::Stopped;
        }
        let envelope = tokio::select! {biased;
            _=stopped(&mut shutdown)=>break RecordingEnd::Stopped,
            result=receiver.recv()=>match result {
                Ok(event)=>event,
                Err(broadcast::error::RecvError::Closed)=>break RecordingEnd::Complete,
                Err(broadcast::error::RecvError::Lagged(count))=>return Err(Error::Protocol(format!("recording lost {count} broadcast events"))),
            }
        };
        let appended = tokio::select! {biased;
            _=stopped(&mut shutdown)=>return Err(Error::Transport("recording interrupted during append".into())),
            result=writer.append_anchored(envelope,started.elapsed())=>result?,
        };
        if !appended || writer.events >= limits.max_events {
            break RecordingEnd::LimitReached;
        }
    };
    writer.finish(end).await
}

pub struct RecordingReader<R> {
    reader: R,
    limits: RecordingLimits,
    events: u64,
    bytes: u64,
    elapsed_ns: u64,
    header: bool,
    version: u16,
    last_anchor: Option<EventAnchor>,
    finished: Option<RecordingSummary>,
    poisoned: bool,
    replay_active: bool,
}
impl<R: AsyncBufRead + Unpin> RecordingReader<R> {
    pub fn new(reader: R, limits: RecordingLimits) -> Self {
        Self {
            reader,
            limits,
            events: 0,
            bytes: 0,
            elapsed_ns: 0,
            header: false,
            version: 0,
            last_anchor: None,
            finished: None,
            poisoned: false,
            replay_active: false,
        }
    }
    pub fn summary(&self) -> Option<RecordingSummary> {
        self.finished
    }
    async fn line(&mut self) -> Result<Option<Vec<u8>>> {
        bounded_line(&mut self.reader, &mut self.bytes, self.limits).await
    }
    async fn next_inner(&mut self) -> Result<Option<RecordedEvent>> {
        if !self.header {
            let line = self.line().await?.ok_or_else(invalid)?;
            match serde_json::from_slice::<Record>(&line).map_err(|_| invalid())? {
                Record::Header {
                    format,
                    version: version @ (1 | 2),
                } if format == FORMAT => {
                    self.header = true;
                    self.version = version;
                }
                _ => return Err(invalid()),
            }
        }
        let line = self.line().await?.ok_or_else(invalid)?;
        match serde_json::from_slice::<Record>(&line).map_err(|_| invalid())? {
            Record::Event { record, anchor } => {
                if (self.version == 2) != anchor.is_some()
                    || anchor.is_some_and(|a| {
                        a.identity != record.identity || a.connection == 0 || a.epoch == 0
                    })
                {
                    return Err(invalid());
                }
                self.last_anchor = anchor;
                if self.events >= self.limits.max_events
                    || record.sequence != self.events + 1
                    || record.elapsed_ns < self.elapsed_ns
                {
                    return Err(invalid());
                }
                validate_event(&record)?;
                self.events += 1;
                self.elapsed_ns = record.elapsed_ns;
                Ok(Some(*record))
            }
            Record::End { events, reason } if events == self.events => {
                if self.line().await?.is_some() {
                    return Err(invalid());
                }
                self.finished = Some(RecordingSummary {
                    events,
                    bytes: self.bytes,
                    end: reason,
                });
                Ok(None)
            }
            _ => Err(invalid()),
        }
    }
    /// A cancelled or failed read cannot be resumed on the same reader.
    pub async fn next_event(&mut self) -> Result<Option<RecordedEvent>> {
        if self.replay_active {
            return Err(invalid());
        }
        self.read_event().await
    }
    /// Reads v2 anchors without reconstructing them from timestamps or state.
    /// A v1 tape cannot provide connection evidence and is rejected here.
    pub async fn next_anchored_event(&mut self) -> Result<Option<AnchoredRecordedEvent>> {
        let record = self.next_event().await?;
        if self.version != 2 {
            self.poisoned = true;
            return Err(invalid());
        }
        record
            .map(|record| {
                Ok(AnchoredRecordedEvent {
                    anchor: self.last_anchor.ok_or_else(invalid)?,
                    record,
                })
            })
            .transpose()
    }
    async fn read_event(&mut self) -> Result<Option<RecordedEvent>> {
        if self.poisoned {
            return Err(invalid());
        }
        if self.finished.is_some() {
            return Ok(None);
        }
        self.poisoned = true;
        let result = tokio::time::timeout(IO_DEADLINE, self.next_inner())
            .await
            .map_err(|_| Error::Transport("recording read deadline".into()))?;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    /// Sequential callbacks; no HTTP, sessions, live counters or book-cache writes.
    pub async fn replay<F, Fut>(
        &mut self,
        options: ReplayOptions,
        shutdown: watch::Receiver<bool>,
        mut callback: F,
    ) -> Result<Option<RecordingSummary>>
    where
        F: FnMut(RecordedEvent) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.replay_checked(options, shutdown, move |record, _| callback(record), false)
            .await
    }
    /// Sequential replay preserving every captured connection anchor. Requires v2.
    pub async fn replay_anchored<F, Fut>(
        &mut self,
        options: ReplayOptions,
        shutdown: watch::Receiver<bool>,
        mut callback: F,
    ) -> Result<Option<RecordingSummary>>
    where
        F: FnMut(AnchoredRecordedEvent) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.replay_checked(
            options,
            shutdown,
            move |record, anchor| {
                callback(AnchoredRecordedEvent {
                    record,
                    anchor: anchor.expect("validated v2 anchor"),
                })
            },
            true,
        )
        .await
    }
    async fn replay_checked<F, Fut>(
        &mut self,
        options: ReplayOptions,
        shutdown: watch::Receiver<bool>,
        callback: F,
        require_anchor: bool,
    ) -> Result<Option<RecordingSummary>>
    where
        F: FnMut(RecordedEvent, Option<EventAnchor>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if self.replay_active || self.poisoned || self.header {
            return Err(Error::InvalidConfiguration(
                "replay requires a fresh recording reader".into(),
            ));
        }
        self.replay_active = true;
        let result = self
            .replay_inner(options, shutdown, callback, require_anchor)
            .await;
        if matches!(result, Ok(Some(_))) {
            self.replay_active = false;
        }
        result
    }
    async fn replay_inner<F, Fut>(
        &mut self,
        options: ReplayOptions,
        mut shutdown: watch::Receiver<bool>,
        mut callback: F,
        require_anchor: bool,
    ) -> Result<Option<RecordingSummary>>
    where
        F: FnMut(RecordedEvent, Option<EventAnchor>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        use futures::FutureExt;
        let started = tokio::time::Instant::now();
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            let next = tokio::select! {biased;_=stopped(&mut shutdown)=>return Ok(None),next=self.read_event()=>next?};
            if require_anchor && self.version != 2 {
                return Err(invalid());
            }
            let Some(record) = next else {
                return Ok(self.summary());
            };
            if options.timing == ReplayTiming::Recorded {
                let delay = Duration::from_nanos(record.elapsed_ns);
                let deadline = started.checked_add(delay).ok_or_else(invalid)?;
                tokio::select! {biased;_=stopped(&mut shutdown)=>return Ok(None),_=tokio::time::sleep_until(deadline)=>{}}
            }
            let callback =
                std::panic::AssertUnwindSafe(async { callback(record, self.last_anchor).await })
                    .catch_unwind();
            tokio::select! {biased;
                _=stopped(&mut shutdown)=>return Ok(None),
                result=tokio::time::timeout(options.callback_deadline,callback)=>{
                    result.map_err(|_|Error::Protocol("replay callback deadline".into()))?
                        .map_err(|_|Error::Protocol("replay callback panicked".into()))??;
                }
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayTiming {
    Immediate,
    Recorded,
}
#[derive(Clone, Copy, Debug)]
pub struct ReplayOptions {
    timing: ReplayTiming,
    callback_deadline: Duration,
}
impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            timing: ReplayTiming::Immediate,
            callback_deadline: Duration::from_secs(5),
        }
    }
}
impl ReplayOptions {
    pub fn timing(mut self, timing: ReplayTiming) -> Self {
        self.timing = timing;
        self
    }
    pub fn callback_deadline(mut self, deadline: Duration) -> Result<Self> {
        if deadline.is_zero() || std::time::Instant::now().checked_add(deadline).is_none() {
            return Err(Error::InvalidConfiguration(
                "replay callback deadline must be positive".into(),
            ));
        }
        self.callback_deadline = deadline;
        Ok(self)
    }
}

#[cfg(test)]
const NORMALIZED_FIXTURE:&[u8]=br#"{"header":{"format":"cryptofeed-rs.normalized","version":1}}
{"event":{"record":{"sequence":1,"elapsed_ns":1000000000,"identity":{"id":1,"generation":1},"event":{"trade":{"exchange":"Binance","symbol":{"value":"BTC-USDT","kind":"Spot"},"side":"buy","amount":"0.1234567890123456789012345678","price":"100.1234567890123456789012345","exchange_ts":1.0,"received_ts":2.0,"id":"synthetic-public-execution","implied_volatility":null}}}}}
{"end":{"events":1,"reason":"complete"}}
"#;

#[cfg(all(test, feature = "trade"))]
mod tests {
    use super::*;
    use crate::feed::{FeedId, FeedIdentity};
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use cryptofeed_trade::{Side, Trade};
    use serde_json::{Value, json};
    fn envelope() -> FeedEnvelope {
        FeedEnvelope {
            identity: FeedIdentity {
                id: FeedId::allocate(),
                generation: 1,
            },
            event: FeedEvent::Trade(Trade {
                exchange: ExchangeId::Binance,
                symbol: Symbol::spot("BTC", "USDT"),
                side: Side::Buy,
                price: rust_decimal::Decimal::from_str_exact("100.1234567890123456789012345")
                    .unwrap(),
                amount: rust_decimal::Decimal::from_str_exact("0.1234567890123456789012345678")
                    .unwrap(),
                exchange_ts: 1.0,
                received_ts: 2.0,
                id: Some("synthetic-public-execution".into()),
                implied_volatility: None,
            }),
        }
    }
    async fn bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        writer
            .append(envelope(), Duration::from_secs(1))
            .await
            .unwrap();
        writer
            .append(envelope(), Duration::from_secs(3))
            .await
            .unwrap();
        writer.finish(RecordingEnd::Complete).await.unwrap();
        bytes
    }
    #[tokio::test]
    async fn roundtrip_retains_order_source_precision_and_original_clocks() {
        let bytes = bytes().await;
        let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
        let first = reader.next_event().await.unwrap().unwrap();
        assert_eq!(first.sequence, 1);
        assert_eq!(first.elapsed_ns, 1_000_000_000);
        let FeedEvent::Trade(trade) = first.event else {
            panic!("trade")
        };
        assert_eq!(trade.price.to_string(), "100.1234567890123456789012345");
        assert_eq!(trade.amount.to_string(), "0.1234567890123456789012345678");
        assert_eq!(trade.exchange_ts, 1.0);
        assert_eq!(trade.received_ts, 2.0);
        let next = reader.next_event().await.unwrap().unwrap();
        assert_eq!(next.sequence, 2);
        assert_ne!(first.identity, next.identity);
        assert!(reader.next_event().await.unwrap().is_none());
        assert_eq!(
            reader.summary().unwrap(),
            RecordingSummary {
                events: 2,
                bytes: bytes.len() as u64,
                end: RecordingEnd::Complete
            }
        );
    }
    #[tokio::test]
    async fn malformed_truncated_wrong_sequence_and_extra_records_are_rejected() {
        let bytes = bytes().await;
        let lines: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for mutation in 0..8 {
            let mut records = lines.clone();
            match mutation {
                0 => records[0]["header"]["version"] = json!(2),
                1 => records[1]["event"]["record"]["sequence"] = json!(2),
                2 => records[2]["event"]["record"]["elapsed_ns"] = json!(0),
                3 => records[3]["end"]["events"] = json!(99),
                4 => {
                    records.pop();
                }
                5 => records.push(records[3].clone()),
                6 => records[1]["event"]["record"]["identity"]["generation"] = json!(0),
                7 => records[1]["event"]["record"]["event"] = json!({"unknown":{}}),
                _ => unreachable!(),
            }
            let mut bytes = records
                .iter()
                .map(|r| serde_json::to_string(r).unwrap() + "\n")
                .collect::<String>()
                .into_bytes();
            if mutation == 4 {
                bytes.pop();
            }
            let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
            let mut failed = false;
            for _ in 0..5 {
                match reader.next_event().await {
                    Err(_) => {
                        failed = true;
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(_)) => {}
                }
            }
            assert!(failed, "mutation {mutation}");
            assert!(reader.summary().is_none());
            assert!(reader.next_event().await.is_err());
        }
    }
    #[tokio::test]
    async fn resource_caps_leave_valid_deliberate_prefix_and_bound_reader() {
        let limits = RecordingLimits::new(1, 2048, 1024).unwrap();
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new(&mut bytes, limits).await.unwrap();
        assert!(writer.append(envelope(), Duration::ZERO).await.unwrap());
        assert!(!writer.append(envelope(), Duration::ZERO).await.unwrap());
        writer.finish(RecordingEnd::LimitReached).await.unwrap();
        let mut reader = RecordingReader::new(bytes.as_slice(), limits);
        assert!(reader.next_event().await.unwrap().is_some());
        assert!(reader.next_event().await.unwrap().is_none());
        assert_eq!(reader.summary().unwrap().end, RecordingEnd::LimitReached);
        let mut large = envelope();
        if let FeedEvent::Trade(ref mut trade) = large.event {
            trade.id = Some("x".repeat(4096));
        }
        let limits = RecordingLimits::new(10, 2048, 512).unwrap();
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new(&mut bytes, limits).await.unwrap();
        assert!(!writer.append(large, Duration::ZERO).await.unwrap());
        assert_eq!(
            writer
                .finish(RecordingEnd::LimitReached)
                .await
                .unwrap()
                .events,
            0
        );
        let bytes = vec![b'x'; 1025];
        let mut reader = RecordingReader::new(bytes.as_slice(), limits);
        assert!(reader.next_event().await.is_err());
        assert!(reader.bytes <= 2048);
    }
    #[tokio::test]
    async fn nonfinite_clocks_and_reversed_capture_time_do_not_write_events() {
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        let mut event = envelope();
        if let FeedEvent::Trade(ref mut trade) = event.event {
            trade.received_ts = f64::NAN;
        }
        assert!(writer.append(event, Duration::ZERO).await.is_err());
        writer
            .append(envelope(), Duration::from_secs(2))
            .await
            .unwrap();
        assert!(
            writer
                .append(envelope(), Duration::from_secs(1))
                .await
                .is_err()
        );
        assert_eq!(
            writer.finish(RecordingEnd::Stopped).await.unwrap().events,
            1
        );
    }
    #[tokio::test]
    async fn broadcast_lag_fails_without_footer_and_exact_event_cap_does_not_wait() {
        let (tx, rx) = broadcast::channel(1);
        tx.send(envelope()).unwrap();
        tx.send(envelope()).unwrap();
        let (_stop_tx, stop) = watch::channel(false);
        let mut bytes = Vec::new();
        assert!(
            record_stream(&mut bytes, rx, RecordingLimits::default(), stop)
                .await
                .is_err()
        );
        let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
        assert!(reader.next_event().await.is_err());
        let (tx, rx) = broadcast::channel(2);
        tx.send(envelope()).unwrap();
        let (_stop_tx, stop) = watch::channel(false);
        let mut bytes = Vec::new();
        let summary = record_stream(
            &mut bytes,
            rx,
            RecordingLimits::new(1, 2048, 1024).unwrap(),
            stop,
        )
        .await
        .unwrap();
        assert_eq!(summary.events, 1);
        assert_eq!(summary.end, RecordingEnd::LimitReached);
        drop(tx);
    }
    #[tokio::test(start_paused = true)]
    async fn recorded_replay_keeps_absolute_schedule_and_callback_order() {
        let bytes = bytes().await;
        let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
        let (_tx, rx) = watch::channel(false);
        let started = tokio::time::Instant::now();
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = observed.clone();
        let summary = reader
            .replay(
                ReplayOptions::default().timing(ReplayTiming::Recorded),
                rx,
                move |record| {
                    let seen = seen.clone();
                    async move {
                        seen.lock()
                            .unwrap()
                            .push((record.sequence, started.elapsed()));
                        if record.sequence == 1 {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        Ok(())
                    }
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.events, 2);
        assert_eq!(
            *observed.lock().unwrap(),
            vec![(1, Duration::from_secs(1)), (2, Duration::from_secs(3))]
        );
    }
    #[tokio::test(start_paused = true)]
    async fn callback_errors_panics_and_deadlines_stop_replay() {
        for mode in 0..4 {
            let bytes = bytes().await;
            let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
            let (_tx, rx) = watch::channel(false);
            let mut calls = 0;
            let result = reader
                .replay(ReplayOptions::default(), rx, |_| {
                    calls += 1;
                    if mode == 3 {
                        panic!("callback construction panic");
                    }
                    async move {
                        match mode {
                            0 => Err(Error::Protocol("business failure".into())),
                            1 => panic!("callback panic"),
                            _ => std::future::pending::<Result<()>>().await,
                        }
                    }
                })
                .await;
            assert!(result.is_err());
            assert_eq!(calls, 1);
            assert!(reader.next_event().await.is_err());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn dropped_or_cancelled_replay_cannot_skip_a_pending_record_on_resume() {
        let bytes = bytes().await;
        let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
        let (tx, rx) = watch::channel(false);
        let mut calls = 0;
        let mut replay = Box::pin(reader.replay(
            ReplayOptions::default().timing(ReplayTiming::Recorded),
            rx,
            |_| {
                calls += 1;
                std::future::ready(Ok(()))
            },
        ));
        assert!(futures::poll!(replay.as_mut()).is_pending());
        tx.send(true).unwrap();
        assert!(replay.await.unwrap().is_none());
        assert_eq!(calls, 0);
        assert!(reader.next_event().await.is_err());
        let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
        let (_tx, rx) = watch::channel(false);
        let mut replay = Box::pin(reader.replay(
            ReplayOptions::default().timing(ReplayTiming::Recorded),
            rx,
            |_| std::future::ready(Ok(())),
        ));
        assert!(futures::poll!(replay.as_mut()).is_pending());
        drop(replay);
        assert!(reader.next_event().await.is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn partial_write_and_read_cancellation_poison_state() {
        let (writer, _unread) = tokio::io::duplex(128);
        let mut writer = RecordingWriter::new(writer, RecordingLimits::default())
            .await
            .unwrap();
        let mut append = Box::pin(writer.append(envelope(), Duration::ZERO));
        assert!(futures::poll!(append.as_mut()).is_pending());
        drop(append);
        assert!(writer.finish(RecordingEnd::Stopped).await.is_err());
        let (_writer, reader) = tokio::io::duplex(128);
        let mut reader = RecordingReader::new(
            tokio::io::BufReader::new(reader),
            RecordingLimits::default(),
        );
        let mut next = Box::pin(reader.next_event());
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert!(reader.next_event().await.is_err());
    }
    #[tokio::test]
    async fn committed_normalized_fixture_replays_without_network() {
        let bytes = NORMALIZED_FIXTURE;
        let mut reader = RecordingReader::new(bytes, RecordingLimits::default());
        let (_tx, rx) = watch::channel(false);
        let mut calls = 0;
        let summary = reader
            .replay(ReplayOptions::default(), rx, |record| {
                assert_eq!(record.identity.id.as_u64(), 1);
                assert_eq!(record.sequence, 1);
                calls += 1;
                std::future::ready(Ok(()))
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(summary.end, RecordingEnd::Complete);
    }
    #[tokio::test]
    async fn every_enabled_normalized_category_roundtrips() {
        let base = json!({"exchange":"Okx","symbol":Symbol::perpetual("BTC","USDT"),"exchange_ts":1.0,"received_ts":2.0});
        let mut cases = vec![("trade", json!({"side":"buy","price":"100","amount":"1"}))];
        #[cfg(feature = "ticker")]
        cases.push(("ticker", json!({"bid":"100","ask":"101"})));
        #[cfg(feature="candles")]cases.push(("candle",json!({"start":0.0,"end":60.0,"interval":"1m","open":"100","close":"101","high":"102","low":"99","volume":"1","closed":false})));
        #[cfg(feature = "funding")]
        cases.push(("funding", json!({"rate":"-0.0001","next_funding_time":3.0})));
        #[cfg(feature = "liquidations")]
        cases.push((
            "liquidation",
            json!({"side":"sell","quantity":"1","price":"100","status":"Filled"}),
        ));
        #[cfg(feature = "markprice")]
        cases.push(("mark_price", json!({"price":"100","next_funding_time":3.0})));
        #[cfg(feature = "openinterest")]
        cases.push((
            "open_interest",
            json!({"open_interest":"10","coin_quantity":"1","value_usd":"100"}),
        ));
        #[cfg(feature = "index")]
        cases.push(("index_price", json!({"price":"100"})));
        #[cfg(feature = "orderbook")]
        {
            cases.push((
                "l1_book",
                json!({"bid":{"price":"100","amount":"1"},"ask":{"price":"101","amount":"2"}}),
            ));
            cases.push((
                "l2_book_snapshot",
                json!({"bids":[{"price":"100","amount":"1"}],"asks":[]}),
            ));
            cases.push((
                "l2_book_delta",
                json!({"bids":[],"asks":[{"price":"101","amount":"0"}]}),
            ));
        }
        for (tag, fields) in cases {
            let mut value = base.clone();
            value
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            let (tag, value) = match tag {
                "l2_book_snapshot" => ("l2_book", json!({"Snapshot":value})),
                "l2_book_delta" => ("l2_book", json!({"Delta":value})),
                _ => (tag, value),
            };
            let event: FeedEvent = serde_json::from_value(json!({tag:value})).unwrap();
            let expected = serde_json::to_value(&event).unwrap();
            let mut bytes = Vec::new();
            let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
                .await
                .unwrap();
            writer
                .append(
                    FeedEnvelope {
                        identity: envelope().identity,
                        event,
                    },
                    Duration::ZERO,
                )
                .await
                .unwrap();
            writer.finish(RecordingEnd::Complete).await.unwrap();
            let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
            assert_eq!(
                serde_json::to_value(reader.next_event().await.unwrap().unwrap().event).unwrap(),
                expected,
                "{tag}"
            );
            assert!(reader.next_event().await.unwrap().is_none());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn io_deadlines_poison_pending_operations() {
        let (writer, _reader) = tokio::io::duplex(128);
        let mut writer = RecordingWriter::new(writer, RecordingLimits::default())
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        assert!(writer.append(envelope(), Duration::ZERO).await.is_err());
        assert!(started.elapsed() >= IO_DEADLINE);
        assert!(writer.finish(RecordingEnd::Stopped).await.is_err());
        let (_writer, reader) = tokio::io::duplex(128);
        let mut reader = RecordingReader::new(
            tokio::io::BufReader::new(reader),
            RecordingLimits::default(),
        );
        assert!(reader.next_event().await.is_err());
        assert!(reader.next_event().await.is_err());
    }
    #[tokio::test]
    async fn false_shutdown_updates_do_not_stop_capture_and_true_stops_empty_capture() {
        let (tx, rx) = broadcast::channel(2);
        tx.send(envelope()).unwrap();
        drop(tx);
        let (stop_tx, stop) = watch::channel(false);
        stop_tx.send(false).unwrap();
        let mut bytes = Vec::new();
        let summary = record_stream(&mut bytes, rx, RecordingLimits::default(), stop)
            .await
            .unwrap();
        assert_eq!(summary.events, 1);
        assert_eq!(summary.end, RecordingEnd::Complete);
        let (_tx, rx) = broadcast::channel(2);
        let (_stop_tx, stop) = watch::channel(true);
        let mut bytes = Vec::new();
        let summary = record_stream(&mut bytes, rx, RecordingLimits::default(), stop)
            .await
            .unwrap();
        assert_eq!(summary.events, 0);
        assert_eq!(summary.end, RecordingEnd::Stopped);
    }
}

#[cfg(all(test, not(feature = "trade")))]
#[tokio::test]
async fn disabled_recorded_category_is_an_error_not_a_skipped_event() {
    let bytes = NORMALIZED_FIXTURE;
    let mut reader = RecordingReader::new(bytes, RecordingLimits::default());
    assert!(reader.next_event().await.is_err());
    assert!(reader.summary().is_none());
}

#[cfg(all(test, feature = "trade"))]
mod anchored_tests {
    use super::*;

    fn envelope(connection: u64, epoch: u64) -> AnchoredEnvelope {
        let record: Record =
            serde_json::from_slice(NORMALIZED_FIXTURE.split(|b| *b == b'\n').nth(1).unwrap())
                .unwrap();
        let Record::Event { record, .. } = record else {
            panic!()
        };
        AnchoredEnvelope {
            anchor: EventAnchor {
                identity: record.identity,
                connection,
                epoch,
            },
            event: record.event,
        }
    }

    const FIXTURE: &[u8] = br#"{"header":{"format":"cryptofeed-rs.normalized","version":2}}
{"event":{"record":{"sequence":1,"elapsed_ns":1000000000,"identity":{"id":1,"generation":1},"event":{"trade":{"exchange":"Binance","symbol":{"value":"BTC-USDT","kind":"Spot"},"side":"buy","amount":"0.1234567890123456789012345678","price":"100.1234567890123456789012345","exchange_ts":1.0,"received_ts":2.0,"id":"synthetic-public-execution","implied_volatility":null}}},"anchor":{"identity":{"id":1,"generation":1},"connection":1,"epoch":10}}}
{"end":{"events":1,"reason":"complete"}}
"#;

    #[tokio::test]
    async fn anchored_v2_reference_matches_inline_codec_assertion() {
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new_anchored(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        writer
            .append_anchored(envelope(1, 10), Duration::from_secs(1))
            .await
            .unwrap();
        writer.finish(RecordingEnd::Complete).await.unwrap();
        assert_eq!(bytes, FIXTURE);
    }

    async fn tape() -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new_anchored(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        // Original ordering, including an old epoch published late, is preserved.
        for (connection, epoch) in [(1, 10), (1, 11), (2, 1), (1, 10)] {
            assert!(
                writer
                    .append_anchored(envelope(connection, epoch), Duration::from_secs(1))
                    .await
                    .unwrap()
            );
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        bytes
    }

    #[tokio::test]
    async fn anchored_roundtrip_and_repeated_replay_preserve_original_labels_and_clocks() {
        let bytes = tape().await;
        let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
        for (connection, epoch) in [(1, 10), (1, 11), (2, 1), (1, 10)] {
            let event = reader.next_anchored_event().await.unwrap().unwrap();
            assert_eq!(
                (event.anchor.connection, event.anchor.epoch),
                (connection, epoch)
            );
            assert_eq!(event.anchor.identity, event.record.identity);
            assert_eq!(event.record.elapsed_ns, 1_000_000_000);
        }
        assert!(reader.next_anchored_event().await.unwrap().is_none());
        let mut runs = Vec::new();
        for _ in 0..2 {
            let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
            let mut events = Vec::new();
            let (_stop, shutdown) = watch::channel(false);
            let summary = reader
                .replay_anchored(ReplayOptions::default(), shutdown, |event| {
                    events.push((event.anchor, serde_json::to_string(&event.record).unwrap()));
                    std::future::ready(Ok(()))
                })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(summary.events, 4);
            runs.push(events);
        }
        assert_eq!(runs[0], runs[1]);
    }

    #[tokio::test]
    async fn anchored_codec_rejects_missing_mismatched_zero_and_v1_anchors() {
        let bytes = tape().await;
        let lines: Vec<_> = bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap())
            .collect();
        for case in 0..6 {
            let mut lines = lines.clone();
            match case {
                0 => {
                    lines[1]["event"].as_object_mut().unwrap().remove("anchor");
                }
                1 => {
                    lines[1]["event"]["anchor"]["identity"]["generation"] = 2.into();
                }
                2 => {
                    lines[1]["event"]["anchor"]["connection"] = 0.into();
                }
                3 => {
                    lines[1]["event"]["anchor"]["epoch"] = 0.into();
                }
                4 => {
                    lines[0]["header"]["version"] = 1.into();
                }
                _ => {
                    lines[1]["event"]["anchor"]["unknown"] = true.into();
                }
            }
            let bytes = lines
                .iter()
                .map(|line| serde_json::to_string(line).unwrap() + "\n")
                .collect::<String>();
            let mut reader = RecordingReader::new(bytes.as_bytes(), RecordingLimits::default());
            assert!(reader.next_anchored_event().await.is_err(), "case {case}");
            assert!(reader.next_anchored_event().await.is_err());
        }
        let mut reader = RecordingReader::new(NORMALIZED_FIXTURE, RecordingLimits::default());
        let (_stop, shutdown) = watch::channel(false);
        assert!(
            reader
                .replay_anchored(ReplayOptions::default(), shutdown, |_| async {
                    panic!("v1 must not call anchored consumer")
                })
                .await
                .is_err()
        );
        let mut bytes = Vec::new();
        let mut writer = RecordingWriter::new(&mut bytes, RecordingLimits::default())
            .await
            .unwrap();
        assert!(
            writer
                .append_anchored(envelope(1, 1), Duration::ZERO)
                .await
                .is_err()
        );
        let mut writer = RecordingWriter::new_anchored(Vec::new(), RecordingLimits::default())
            .await
            .unwrap();
        let event = envelope(1, 1);
        assert!(
            writer
                .append(
                    FeedEnvelope {
                        identity: event.anchor.identity,
                        event: event.event
                    },
                    Duration::ZERO
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn anchored_stream_rejects_ring_loss_and_certifies_closed_drain() {
        for capacity in [1, 4] {
            let (sender, receiver) = broadcast::channel(capacity);
            for epoch in 1..=3 {
                sender.send(envelope(1, epoch)).unwrap();
            }
            drop(sender);
            let (_stop, shutdown) = watch::channel(false);
            let mut bytes = Vec::new();
            let result =
                record_anchored_stream(&mut bytes, receiver, RecordingLimits::default(), shutdown)
                    .await;
            if capacity == 1 {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap().events, 3);
                let mut reader = RecordingReader::new(bytes.as_slice(), RecordingLimits::default());
                for epoch in 1..=3 {
                    assert_eq!(
                        reader
                            .next_anchored_event()
                            .await
                            .unwrap()
                            .unwrap()
                            .anchor
                            .epoch,
                        epoch
                    );
                }
                assert!(reader.next_anchored_event().await.unwrap().is_none());
            }
        }
    }
}
