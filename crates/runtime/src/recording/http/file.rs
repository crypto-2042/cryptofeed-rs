use super::*;
use crate::recording::{
    FOOTER_RESERVE, IO_DEADLINE, RecordingEnd, RecordingLimits, RecordingSummary, bounded_line,
};
use std::collections::HashSet;
use tokio::{
    io::{AsyncBufRead, AsyncWrite, AsyncWriteExt},
    sync::watch,
};
const FORMAT: &str = "cryptofeed-rs.public-http";
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header { format: String, version: u16 },
    Observation { record: HttpObservation },
    End { events: u64, reason: RecordingEnd },
}
#[derive(Default)]
struct Trace {
    sequence: u64,
    elapsed: u64,
    requests: u64,
    pending: HashSet<u64>,
}
impl Trace {
    fn apply(&mut self, record: &HttpObservation) -> Result<()> {
        if record.sequence != self.sequence + 1
            || record.elapsed_ns < self.elapsed
            || !record.observed_ts.is_finite()
            || record.observed_ts < 0.0
        {
            return Err(invalid());
        }
        match &record.kind {
            HttpObservationKind::Request {
                request_id,
                exchange,
                url,
            } => {
                public_url(*exchange, url)?;
                if *request_id != self.requests + 1 {
                    return Err(invalid());
                }
                self.requests = *request_id;
                self.pending.insert(*request_id);
            }
            HttpObservationKind::Finished {
                request_id,
                outcome,
            } => {
                validate_outcome(outcome)?;
                if !self.pending.remove(request_id) {
                    return Err(invalid());
                }
            }
            HttpObservationKind::Cancelled { request_id } => {
                if !self.pending.remove(request_id) {
                    return Err(invalid());
                }
            }
        }
        self.sequence = record.sequence;
        self.elapsed = record.elapsed_ns;
        Ok(())
    }
    fn ended(&self, end: RecordingEnd) -> bool {
        end != RecordingEnd::Complete || self.pending.is_empty()
    }
}
fn io(error: std::io::Error) -> Error {
    Error::Transport(format!("HTTP recording I/O: {error}"))
}
pub struct HttpRecordingWriter<W> {
    writer: W,
    limits: RecordingLimits,
    trace: Trace,
    bytes: u64,
    poisoned: bool,
}
impl<W: AsyncWrite + Unpin> HttpRecordingWriter<W> {
    pub async fn new(writer: W, limits: RecordingLimits) -> Result<Self> {
        let mut this = Self {
            writer,
            limits,
            trace: Trace::default(),
            bytes: 0,
            poisoned: false,
        };
        let header = encode(
            &Record::Header {
                format: FORMAT.into(),
                version: 1,
            },
            limits.max_record_bytes,
        )?;
        this.write(&header).await?;
        Ok(this)
    }
    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.poisoned = true;
        tokio::time::timeout(IO_DEADLINE, self.writer.write_all(bytes))
            .await
            .map_err(|_| Error::Transport("HTTP recording write deadline".into()))?
            .map_err(io)?;
        self.bytes += bytes.len() as u64;
        self.poisoned = false;
        Ok(())
    }
    pub async fn append(&mut self, record: HttpObservation) -> Result<bool> {
        if self.poisoned {
            return Err(invalid());
        }
        if self.trace.sequence >= self.limits.max_events {
            return Ok(false);
        }
        let record = Record::Observation { record };
        let bytes = match encode(&record, self.limits.max_record_bytes) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(false),
        };
        if bytes.len() as u64 + FOOTER_RESERVE as u64
            > self.limits.max_bytes.saturating_sub(self.bytes)
        {
            return Ok(false);
        }
        let Record::Observation { record } = record else {
            unreachable!()
        };
        self.poisoned = true;
        self.trace.apply(&record)?;
        self.write(&bytes).await?;
        Ok(true)
    }
    pub async fn finish(mut self, end: RecordingEnd) -> Result<RecordingSummary> {
        if self.poisoned || !self.trace.ended(end) {
            return Err(invalid());
        }
        let footer = encode(
            &Record::End {
                events: self.trace.sequence,
                reason: end,
            },
            self.limits.max_record_bytes,
        )?;
        self.write(&footer).await?;
        tokio::time::timeout(IO_DEADLINE, self.writer.flush())
            .await
            .map_err(|_| Error::Transport("HTTP recording flush deadline".into()))?
            .map_err(io)?;
        Ok(RecordingSummary {
            events: self.trace.sequence,
            bytes: self.bytes,
            end,
        })
    }
}
pub async fn record_http_stream<W: AsyncWrite + Unpin>(
    writer: W,
    mut input: HttpCaptureReceiver,
    limits: RecordingLimits,
    mut shutdown: watch::Receiver<bool>,
) -> Result<RecordingSummary> {
    let mut writer = HttpRecordingWriter::new(writer, limits).await?;
    let end = loop {
        let record = tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>break RecordingEnd::Stopped,result=input.recv()=>match result?{Some(record)=>record,None=>break RecordingEnd::Complete}};
        let accepted = tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Err(Error::Transport("HTTP recording interrupted during append".into())),result=writer.append(record)=>result?};
        if !accepted || writer.trace.sequence >= limits.max_events {
            break RecordingEnd::LimitReached;
        }
    };
    writer.finish(end).await
}
pub struct HttpRecordingReader<R> {
    reader: R,
    limits: RecordingLimits,
    trace: Trace,
    bytes: u64,
    header: bool,
    poisoned: bool,
    summary: Option<RecordingSummary>,
}
impl<R: AsyncBufRead + Unpin> HttpRecordingReader<R> {
    pub fn new(reader: R, limits: RecordingLimits) -> Self {
        Self {
            reader,
            limits,
            trace: Trace::default(),
            bytes: 0,
            header: false,
            poisoned: false,
            summary: None,
        }
    }
    pub fn summary(&self) -> Option<RecordingSummary> {
        self.summary
    }
    async fn line(&mut self) -> Result<Option<Vec<u8>>> {
        bounded_line(&mut self.reader, &mut self.bytes, self.limits).await
    }
    async fn next_inner(&mut self) -> Result<Option<HttpObservation>> {
        if !self.header {
            let bytes = self.line().await?.ok_or_else(invalid)?;
            match serde_json::from_slice::<Record>(&bytes).map_err(|_| invalid())? {
                Record::Header { format, version: 1 } if format == FORMAT => self.header = true,
                _ => return Err(invalid()),
            }
        }
        let bytes = self.line().await?.ok_or_else(invalid)?;
        match serde_json::from_slice::<Record>(&bytes).map_err(|_| invalid())? {
            Record::Observation { record } => {
                if self.trace.sequence >= self.limits.max_events {
                    return Err(invalid());
                }
                self.trace.apply(&record)?;
                Ok(Some(record))
            }
            Record::End { events, reason }
                if events == self.trace.sequence && self.trace.ended(reason) =>
            {
                if self.line().await?.is_some() {
                    return Err(invalid());
                }
                self.summary = Some(RecordingSummary {
                    events,
                    bytes: self.bytes,
                    end: reason,
                });
                Ok(None)
            }
            _ => Err(invalid()),
        }
    }
    pub async fn next_observation(&mut self) -> Result<Option<HttpObservation>> {
        if self.poisoned {
            return Err(invalid());
        }
        if self.summary.is_some() {
            return Ok(None);
        }
        self.poisoned = true;
        let result = tokio::time::timeout(IO_DEADLINE, self.next_inner())
            .await
            .map_err(|_| Error::Transport("HTTP recording read deadline".into()))?;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
}
