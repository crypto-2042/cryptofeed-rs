//! Validated raw-observation segments. Protocol execution is a separate layer.
use super::{
    RawCaptureReceiver, RawObservation, RawObservationKind, RawPayload, RawSessionInfo, sanitize,
};
use crate::recording::{
    FOOTER_RESERVE, IO_DEADLINE, RecordingEnd, RecordingLimits, RecordingSummary, bounded_line,
    encode,
};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::InstrumentKind,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::{
    io::{AsyncBufRead, AsyncWrite, AsyncWriteExt},
    sync::watch,
};
const FORMAT: &str = "cryptofeed-rs.raw-ws";
#[derive(Clone, Copy, Debug)]
pub struct RawRecordingLimits {
    records: RecordingLimits,
    max_sessions: usize,
}
impl Default for RawRecordingLimits {
    fn default() -> Self {
        Self {
            records: RecordingLimits::default(),
            max_sessions: 1024,
        }
    }
}
impl RawRecordingLimits {
    pub fn new(records: RecordingLimits, max_sessions: usize) -> Result<Self> {
        if !(1..=100_000).contains(&max_sessions) {
            return Err(Error::InvalidConfiguration(
                "raw recording session limit must be 1..100000".into(),
            ));
        }
        Ok(Self {
            records,
            max_sessions,
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header { format: String, version: u16 },
    Observation { record: RawObservation },
    End { events: u64, reason: RecordingEnd },
}
fn invalid() -> Error {
    Error::MalformedData("invalid/incomplete/unsafe raw recording".into())
}
fn io(error: std::io::Error) -> Error {
    Error::Transport(format!("raw recording I/O: {error}"))
}
fn validate_payload(payload: &RawPayload) -> Result<()> {
    match payload {
        RawPayload::Heartbeat(text) if matches!(text.as_str(), "ping" | "pong") => Ok(()),
        RawPayload::Json { value, redacted } => {
            let mut changed = false;
            let clean = sanitize(value, 0, &mut changed).map_err(|_| invalid())?;
            if clean != *value || changed && !*redacted {
                return Err(invalid());
            }
            Ok(())
        }
        _ => Err(invalid()),
    }
}
fn validate_context(info: &RawSessionInfo) -> Result<()> {
    let feed = &info.feed;
    if info.session_id == 0
        || feed.sdk_version.is_empty()
        || feed.sdk_version.len() > 64
        || !matches!(
            feed.exchange,
            ExchangeId::Binance
                | ExchangeId::Bitget
                | ExchangeId::Bybit
                | ExchangeId::Okx
                | ExchangeId::Gateio
        )
        || feed
            .identity
            .is_some_and(|id| id.id.as_u64() == 0 || id.generation == 0)
        || feed.channels.is_empty()
        || feed.symbols.is_empty()
        || feed.symbols.len() != feed.exchange_symbols.len()
        || feed.exchange_symbols.iter().any(String::is_empty)
    {
        return Err(invalid());
    }
    let channels: HashSet<_> = feed.channels.iter().collect();
    let symbols: HashSet<_> = feed.symbols.iter().collect();
    if channels.len() != feed.channels.len() || symbols.len() != feed.symbols.len() {
        return Err(invalid());
    }
    let product = feed.symbols[0].kind();
    if !matches!(
        product,
        InstrumentKind::Spot | InstrumentKind::Perpetual | InstrumentKind::Futures
    ) || feed.symbols.iter().any(|symbol| symbol.kind() != product)
    {
        return Err(invalid());
    }
    if feed
        .candle_policy
        .as_deref()
        .is_some_and(|policy| !matches!(policy, "All" | "ClosedOnly" | "ClosedOrUnknown"))
    {
        return Err(invalid());
    }
    if !feed.channel_subscriptions.is_empty() {
        let mut seen = HashSet::new();
        let mut union = HashSet::new();
        for (channel, selected) in &feed.channel_subscriptions {
            if !channels.contains(channel) || !seen.insert(channel) || selected.is_empty() {
                return Err(invalid());
            }
            let mut pairs = HashSet::new();
            for symbol in selected {
                if !symbols.contains(symbol) || !pairs.insert(symbol) {
                    return Err(invalid());
                }
                union.insert(symbol);
            }
        }
        if seen != channels || union != symbols {
            return Err(invalid());
        }
    }
    Ok(())
}
#[derive(Default)]
struct TraceState {
    sequence: u64,
    elapsed_ns: u64,
    sessions: HashMap<u64, (Arc<RawSessionInfo>, bool)>,
}
impl TraceState {
    fn apply(&mut self, record: &RawObservation, max_sessions: usize) -> Result<()> {
        if record.version != 1
            || record.sequence != self.sequence + 1
            || record.elapsed_ns < self.elapsed_ns
            || !record.observed_ts.is_finite()
            || record.observed_ts < 0.0
        {
            return Err(invalid());
        }
        validate_context(&record.session)?;
        let id = record.session.session_id;
        match &record.kind {
            RawObservationKind::Connected => {
                if self.sessions.contains_key(&id) {
                    return Err(invalid());
                }
                if self.sessions.len() >= max_sessions {
                    return Err(Error::InvalidConfiguration(
                        "raw recording session limit".into(),
                    ));
                }
                self.sessions.insert(id, (record.session.clone(), false));
            }
            kind => {
                let Some((info, closed)) = self.sessions.get_mut(&id) else {
                    return Err(invalid());
                };
                if *closed || info.as_ref() != record.session.as_ref() {
                    return Err(invalid());
                }
                match kind {
                    RawObservationKind::Sent(payload) | RawObservationKind::Received(payload) => {
                        validate_payload(payload)?
                    }
                    RawObservationKind::Closed { .. } => *closed = true,
                    _ => unreachable!(),
                }
            }
        }
        self.sequence = record.sequence;
        self.elapsed_ns = record.elapsed_ns;
        Ok(())
    }
    fn ended(&self, end: RecordingEnd) -> bool {
        end != RecordingEnd::Complete || self.sessions.values().all(|(_, closed)| *closed)
    }
}
pub struct RawRecordingWriter<W> {
    writer: W,
    limits: RawRecordingLimits,
    state: TraceState,
    bytes: u64,
    poisoned: bool,
}
impl<W: AsyncWrite + Unpin> RawRecordingWriter<W> {
    pub async fn new(writer: W, limits: RawRecordingLimits) -> Result<Self> {
        let mut this = Self {
            writer,
            limits,
            state: TraceState::default(),
            bytes: 0,
            poisoned: false,
        };
        let bytes = encode(
            &Record::Header {
                format: FORMAT.into(),
                version: 1,
            },
            limits.records.max_record_bytes,
        )?;
        this.write(&bytes).await?;
        Ok(this)
    }
    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if self.poisoned {
            return Err(invalid());
        }
        self.poisoned = true;
        tokio::time::timeout(IO_DEADLINE, self.writer.write_all(bytes))
            .await
            .map_err(|_| Error::Transport("raw recording write deadline".into()))?
            .map_err(io)?;
        self.bytes += bytes.len() as u64;
        self.poisoned = false;
        Ok(())
    }
    /// False means a deliberate budget cutoff; no prefix of the rejected line is written.
    pub async fn append(&mut self, observation: RawObservation) -> Result<bool> {
        if self.poisoned {
            return Err(invalid());
        }
        if self.state.sequence >= self.limits.records.max_events {
            return Ok(false);
        }
        if matches!(observation.kind, RawObservationKind::Connected)
            && !self
                .state
                .sessions
                .contains_key(&observation.session.session_id)
            && self.state.sessions.len() >= self.limits.max_sessions
        {
            return Ok(false);
        }
        let record = Record::Observation {
            record: observation,
        };
        let bytes = match encode(&record, self.limits.records.max_record_bytes) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(false),
        };
        if bytes.len() as u64 + FOOTER_RESERVE as u64
            > self.limits.records.max_bytes.saturating_sub(self.bytes)
        {
            return Ok(false);
        }
        let Record::Observation { record } = record else {
            unreachable!()
        };
        self.state.apply(&record, self.limits.max_sessions)?;
        self.write(&bytes).await?;
        Ok(true)
    }
    pub async fn finish(mut self, end: RecordingEnd) -> Result<RecordingSummary> {
        if self.poisoned || !self.state.ended(end) {
            return Err(invalid());
        }
        let bytes = encode(
            &Record::End {
                events: self.state.sequence,
                reason: end,
            },
            self.limits.records.max_record_bytes,
        )?;
        self.write(&bytes).await?;
        tokio::time::timeout(IO_DEADLINE, self.writer.flush())
            .await
            .map_err(|_| Error::Transport("raw recording flush deadline".into()))?
            .map_err(io)?;
        Ok(RecordingSummary {
            events: self.state.sequence,
            bytes: self.bytes,
            end,
        })
    }
}
pub async fn record_raw_stream<W: AsyncWrite + Unpin>(
    writer: W,
    mut input: RawCaptureReceiver,
    limits: RawRecordingLimits,
    mut shutdown: watch::Receiver<bool>,
) -> Result<RecordingSummary> {
    let mut writer = RawRecordingWriter::new(writer, limits).await?;
    let end = loop {
        if *shutdown.borrow() {
            break RecordingEnd::Stopped;
        }
        let observation = tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>break RecordingEnd::Stopped,result=input.recv()=>match result?{Some(value)=>value,None=>break RecordingEnd::Complete}};
        let appended = tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Err(Error::Transport("raw recording interrupted during append".into())),result=writer.append(observation)=>result?};
        if !appended || writer.state.sequence >= limits.records.max_events {
            break RecordingEnd::LimitReached;
        }
    };
    writer.finish(end).await
}
pub struct RawRecordingReader<R> {
    reader: R,
    limits: RawRecordingLimits,
    state: TraceState,
    bytes: u64,
    header: bool,
    poisoned: bool,
    finished: Option<RecordingSummary>,
}
impl<R: AsyncBufRead + Unpin> RawRecordingReader<R> {
    pub fn new(reader: R, limits: RawRecordingLimits) -> Self {
        Self {
            reader,
            limits,
            state: TraceState::default(),
            bytes: 0,
            header: false,
            poisoned: false,
            finished: None,
        }
    }
    pub fn summary(&self) -> Option<RecordingSummary> {
        self.finished
    }
    async fn line(&mut self) -> Result<Option<Vec<u8>>> {
        bounded_line(&mut self.reader, &mut self.bytes, self.limits.records).await
    }
    async fn next_inner(&mut self) -> Result<Option<RawObservation>> {
        if !self.header {
            let line = self.line().await?.ok_or_else(invalid)?;
            match serde_json::from_slice::<Record>(&line).map_err(|_| invalid())? {
                Record::Header { format, version: 1 } if format == FORMAT => self.header = true,
                _ => return Err(invalid()),
            }
        }
        let line = self.line().await?.ok_or_else(invalid)?;
        match serde_json::from_slice::<Record>(&line).map_err(|_| invalid())? {
            Record::Observation { mut record } => {
                if self.state.sequence >= self.limits.records.max_events {
                    return Err(invalid());
                }
                self.state.apply(&record, self.limits.max_sessions)?;
                record.session = self.state.sessions[&record.session.session_id].0.clone();
                Ok(Some(record))
            }
            Record::End { events, reason }
                if events == self.state.sequence && self.state.ended(reason) =>
            {
                if self.line().await?.is_some() {
                    return Err(invalid());
                }
                let summary = RecordingSummary {
                    events,
                    bytes: self.bytes,
                    end: reason,
                };
                self.finished = Some(summary);
                Ok(None)
            }
            _ => Err(invalid()),
        }
    }
    /// Errors/cancellation poison state; reopen the file rather than skipping a line.
    pub async fn next_observation(&mut self) -> Result<Option<RawObservation>> {
        if self.poisoned {
            return Err(invalid());
        }
        if self.finished.is_some() {
            return Ok(None);
        }
        self.poisoned = true;
        let result = tokio::time::timeout(IO_DEADLINE, self.next_inner())
            .await
            .map_err(|_| Error::Transport("raw recording read deadline".into()))?;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exchange::okx::Okx,
        recording::raw::{RawFeedInfo, raw_capture_channel},
    };
    use serde_json::{Value, json};
    async fn observations() -> Vec<RawObservation> {
        let (capture, mut input) = raw_capture_channel(16, 4096).unwrap();
        let feed = Okx::new()
            .trade()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT")
            .build();
        let mut session = capture.session(RawFeedInfo::from_feed(&feed)).unwrap();
        let payload: Value = serde_json::from_str(
            r#"{"data":{"px":100.1234567890123456789012345,"api_key":"synthetic-do-not-write"}}"#,
        )
        .unwrap();
        session.text(
            &serde_json::to_string(&payload).unwrap(),
            Some(&payload),
            true,
            1.25,
        );
        session.close(true);
        drop(session);
        drop(capture);
        let mut result = Vec::new();
        while let Some(record) = input.recv().await.unwrap() {
            result.push(record);
        }
        result
    }
    async fn bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
            .await
            .unwrap();
        for record in observations().await {
            assert!(writer.append(record).await.unwrap());
        }
        writer.finish(RecordingEnd::Complete).await.unwrap();
        bytes
    }
    #[tokio::test]
    async fn roundtrip_keeps_sanitized_precision_context_and_validated_eof() {
        let bytes = bytes().await;
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(!text.contains("synthetic-do-not-write"));
        assert!(text.contains("100.1234567890123456789012345"));
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        let first = reader.next_observation().await.unwrap().unwrap();
        let second = reader.next_observation().await.unwrap().unwrap();
        assert_eq!(second.observed_ts, 1.25);
        let RawObservationKind::Received(RawPayload::Json { value, redacted }) = &second.kind
        else {
            panic!("json")
        };
        assert!(*redacted);
        assert_eq!(
            value["data"]["px"].to_string(),
            "100.1234567890123456789012345"
        );

        assert!(Arc::ptr_eq(&first.session, &second.session));
        assert!(matches!(
            reader.next_observation().await.unwrap().unwrap().kind,
            RawObservationKind::Closed { clean: true }
        ));
        assert!(reader.next_observation().await.unwrap().is_none());
        assert_eq!(
            reader.summary().unwrap(),
            RecordingSummary {
                events: 3,
                bytes: bytes.len() as u64,
                end: RecordingEnd::Complete
            }
        );
    }
    #[tokio::test]
    async fn malformed_versions_gaps_lifecycle_mutation_and_private_data_fail() {
        let bytes = bytes().await;
        let original: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for mutation in 0..11 {
            let mut lines = original.clone();
            match mutation {
                0 => lines[0]["header"]["version"] = json!(2),
                1 => lines[2]["observation"]["record"]["sequence"] = json!(5),
                2 => {
                    lines[2]["observation"]["record"]["session"]["feed"]["sdk_version"] =
                        json!("changed")
                }
                3 => lines[2]["observation"]["record"]["session"]["feed"]["symbols"] = json!([]),
                4 => {
                    lines[2]["observation"]["record"]["kind"]["received"]["json"]["value"]["data"]
                        ["api_key"] = json!("unsafe-secret")
                }
                5 => {
                    lines[2]["observation"]["record"]["kind"]["received"]["json"]["redacted"] =
                        json!(false)
                }
                6 => {
                    lines.pop();
                }
                7 => lines.push(lines[4].clone()),
                8 => {
                    lines[3]["observation"]["record"]["kind"] =
                        json!({"received":{"heartbeat":"pong"}})
                }
                9 => {
                    lines[2]["observation"]["record"]["kind"]["received"]["json"]["value"] =
                        json!({"op":"auth","args":["unsafe-secret"]})
                }
                10 => lines[4]["end"]["events"] = json!(99),
                _ => unreachable!(),
            }
            let text = lines
                .iter()
                .map(|line| serde_json::to_string(line).unwrap() + "\n")
                .collect::<String>();
            let mut reader =
                RawRecordingReader::new(text.as_bytes(), RawRecordingLimits::default());
            let mut error = None;
            for _ in 0..6 {
                match reader.next_observation().await {
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(_)) => {}
                }
            }
            let error = error.unwrap_or_else(|| panic!("mutation {mutation} accepted"));
            assert!(!error.to_string().contains("unsafe-secret"));
            assert!(reader.summary().is_none());
            assert!(reader.next_observation().await.is_err());
        }
    }
    #[tokio::test]
    async fn writer_rejects_unsafe_or_out_of_order_rows_before_io() {
        let mut observations = observations().await;
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
            .await
            .unwrap();
        writer.append(observations.remove(0)).await.unwrap();
        let mut bad = observations[0].clone();
        bad.kind = RawObservationKind::Received(RawPayload::Json {
            value: json!({"apiKey":"unsafe-secret"}),
            redacted: false,
        });
        assert!(writer.append(bad).await.is_err());
        let mut bad = observations[0].clone();
        bad.sequence = 99;
        assert!(writer.append(bad).await.is_err());
        assert!(writer.finish(RecordingEnd::Stopped).await.is_ok());
        assert!(!String::from_utf8(bytes).unwrap().contains("unsafe-secret"));
    }
    #[tokio::test]
    async fn intentional_prefixes_are_distinct_from_complete_open_sessions() {
        let records = observations().await;
        let limits =
            RawRecordingLimits::new(RecordingLimits::new(1, 4096, 2048).unwrap(), 1).unwrap();
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, limits).await.unwrap();
        assert!(writer.append(records[0].clone()).await.unwrap());
        assert!(!writer.append(records[1].clone()).await.unwrap());
        writer.finish(RecordingEnd::LimitReached).await.unwrap();
        let mut reader = RawRecordingReader::new(bytes.as_slice(), limits);
        assert!(reader.next_observation().await.unwrap().is_some());
        assert!(reader.next_observation().await.unwrap().is_none());
        assert_eq!(reader.summary().unwrap().end, RecordingEnd::LimitReached);
        let mut bytes = Vec::new();
        let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
            .await
            .unwrap();
        writer.append(records[0].clone()).await.unwrap();
        assert!(writer.finish(RecordingEnd::Complete).await.is_err());
    }
    #[tokio::test]
    async fn raw_capture_gap_does_not_emit_a_successful_footer() {
        let (capture, input) = raw_capture_channel(1, 4096).unwrap();
        let feed = Okx::new()
            .trade()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT")
            .build();
        let session = capture.session(RawFeedInfo::from_feed(&feed)).unwrap();
        session.text("pong", None, true, 1.0);
        let (_tx, stop) = watch::channel(false);
        let mut bytes = Vec::new();
        assert!(
            record_raw_stream(&mut bytes, input, RawRecordingLimits::default(), stop)
                .await
                .is_err()
        );
        let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
        assert!(reader.next_observation().await.is_err());
        assert!(reader.summary().is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn partial_io_cancellation_and_read_deadlines_poison_state() {
        let records = observations().await;
        let (writer, _reader) = tokio::io::duplex(128);
        let mut writer = RawRecordingWriter::new(writer, RawRecordingLimits::default())
            .await
            .unwrap();
        let mut append = Box::pin(writer.append(records[0].clone()));
        assert!(futures::poll!(append.as_mut()).is_pending());
        drop(append);
        assert!(writer.finish(RecordingEnd::Stopped).await.is_err());
        let (_writer, reader) = tokio::io::duplex(128);
        let mut reader = RawRecordingReader::new(
            tokio::io::BufReader::new(reader),
            RawRecordingLimits::default(),
        );
        assert!(reader.next_observation().await.is_err());
        assert!(reader.next_observation().await.is_err());
        let (_writer, reader) = tokio::io::duplex(128);
        let mut reader = RawRecordingReader::new(
            tokio::io::BufReader::new(reader),
            RawRecordingLimits::default(),
        );
        let mut next = Box::pin(reader.next_observation());
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert!(reader.next_observation().await.is_err());
    }
    #[tokio::test]
    async fn reader_rejects_oversized_lines_and_session_limit_is_a_cutoff() {
        let records = observations().await;
        let mut bytes = Vec::new();
        let limits = RawRecordingLimits::new(RecordingLimits::default(), 1).unwrap();
        let mut writer = RawRecordingWriter::new(&mut bytes, limits).await.unwrap();
        for record in &records {
            writer.append(record.clone()).await.unwrap();
        }
        let mut new = records[0].clone();
        new.sequence = 4;
        new.elapsed_ns = records[2].elapsed_ns;
        let mut info = (*new.session).clone();
        info.session_id += 1;
        new.session = Arc::new(info);
        assert!(!writer.append(new).await.unwrap());
        writer.finish(RecordingEnd::LimitReached).await.unwrap();
        let limits =
            RawRecordingLimits::new(RecordingLimits::new(10, 4096, 256).unwrap(), 1).unwrap();
        let bytes = vec![b'x'; 257];
        let mut reader = RawRecordingReader::new(bytes.as_slice(), limits);
        assert!(reader.next_observation().await.is_err());
    }
    #[tokio::test]
    async fn inline_reference_validates_and_codecs_do_not_silently_mix() {
        const FIXTURE:&[u8]=br#"{"header":{"format":"cryptofeed-rs.raw-ws","version":1}}
{"observation":{"record":{"version":1,"sequence":1,"elapsed_ns":0,"observed_ts":0.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Okx","channels":["Trade"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTC-USDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":"connected"}}}
{"observation":{"record":{"version":1,"sequence":2,"elapsed_ns":0,"observed_ts":1.25,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Okx","channels":["Trade"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTC-USDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"received":{"json":{"value":{"data":{"px":100.1234567890123456789012345,"sz":"1","api_key":"[REDACTED]"},"error":{"code":7,"message":"[REDACTED]"},"headers":{"Authorization":"[REDACTED]"},"signature":"[REDACTED]"},"redacted":true}}}}}}
{"observation":{"record":{"version":1,"sequence":3,"elapsed_ns":0,"observed_ts":2.0,"session":{"session_id":1,"feed":{"sdk_version":"0.1.0","identity":null,"exchange":"Okx","channels":["Trade"],"symbols":[{"value":"BTC-USDT","kind":"Spot"}],"exchange_symbols":["BTC-USDT"],"channel_subscriptions":[],"candle_interval":"1m","candle_policy":"All","l2_book_depth":null,"l2_book_interval":null}},"kind":{"closed":{"clean":true}}}}}
{"end":{"events":3,"reason":"complete"}}
"#;
        let mut reader = RawRecordingReader::new(FIXTURE, RawRecordingLimits::default());
        let mut count = 0;
        while reader.next_observation().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        assert_eq!(reader.summary().unwrap().end, RecordingEnd::Complete);
        let mut reader =
            crate::recording::RecordingReader::new(FIXTURE, RecordingLimits::default());
        assert!(reader.next_event().await.is_err());
        let mut reader = RawRecordingReader::new(
            crate::recording::NORMALIZED_FIXTURE,
            RawRecordingLimits::default(),
        );
        assert!(reader.next_observation().await.is_err());
    }
}
