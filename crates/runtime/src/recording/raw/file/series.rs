//! One logical raw v2 stream across ordered record-aligned physical files.
use super::super::{RawReplayItem, RawReplayOptions, RawReplaySummary};
use super::*;
use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, BufReader, ReadBuf},
};
const MANIFEST_LIMIT: usize = 256 * 1024;
const BASIS: u64 = 14_695_981_039_346_656_037;
fn hash(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state = (state ^ u64::from(*byte)).wrapping_mul(1_099_511_628_211);
    }
    state
}
#[derive(Clone, Copy, Debug)]
pub struct RawSeriesLimits {
    recording: RawRecordingLimits,
    part_bytes: u64,
    max_parts: usize,
}
impl Default for RawSeriesLimits {
    fn default() -> Self {
        Self {
            recording: RawRecordingLimits::default(),
            part_bytes: 16 * 1024 * 1024,
            max_parts: 1024,
        }
    }
}
impl RawSeriesLimits {
    pub fn new(recording: RawRecordingLimits, part_bytes: u64, max_parts: usize) -> Result<Self> {
        if part_bytes < recording.records.max_record_bytes as u64
            || !(1..=1024).contains(&max_parts)
        {
            return Err(Error::InvalidConfiguration(
                "raw series needs part bytes >= record limit and 1..=1024 parts".into(),
            ));
        }
        Ok(Self {
            recording,
            part_bytes,
            max_parts,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Part {
    bytes: u64,
    fnv1a64: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    version: u16,
    id: String,
    parts: Vec<Part>,
    events: u64,
    bytes: u64,
    end: RecordingEnd,
}
fn check_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::InvalidConfiguration(
            "raw series ID must be 1..=64 ASCII letters/digits/-/_".into(),
        ));
    }
    Ok(())
}
fn part_path(directory: &Path, id: &str, index: usize) -> PathBuf {
    directory.join(format!("{id}.part-{index:04}.jsonl"))
}
fn manifest_path(directory: &Path, id: &str) -> PathBuf {
    directory.join(format!("{id}.manifest.json"))
}
async fn timed<T>(work: impl Future<Output = std::io::Result<T>>) -> Result<T> {
    tokio::time::timeout(IO_DEADLINE, work)
        .await
        .map_err(|_| Error::Transport("raw series I/O deadline".into()))?
        .map_err(io)
}
async fn create(path: PathBuf) -> Result<File> {
    timed(OpenOptions::new().write(true).create_new(true).open(path)).await
}
struct Storage {
    directory: PathBuf,
    id: String,
    limits: RawSeriesLimits,
    manifest: File,
    current: File,
    parts: Vec<Part>,
}
impl Storage {
    async fn write_line(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() as u64 > self.limits.part_bytes {
            return Err(invalid());
        }
        if self.parts.last().expect("part").bytes + bytes.len() as u64 > self.limits.part_bytes {
            if self.parts.len() >= self.limits.max_parts {
                return Err(Error::InvalidConfiguration(
                    "raw series part limit reached".into(),
                ));
            }
            timed(self.current.flush()).await?;
            self.current = create(part_path(&self.directory, &self.id, self.parts.len())).await?;
            self.parts.push(Part {
                bytes: 0,
                fnv1a64: BASIS,
            });
        }
        timed(self.current.write_all(bytes)).await?;
        let part = self.parts.last_mut().expect("part");
        part.bytes += bytes.len() as u64;
        part.fnv1a64 = hash(part.fnv1a64, bytes);
        Ok(())
    }
    async fn finish(mut self, summary: RecordingSummary) -> Result<RecordingSummary> {
        timed(self.current.flush()).await?;
        let manifest = Manifest {
            format: "cryptofeed-rs.raw-series".into(),
            version: 1,
            id: self.id,
            parts: self.parts,
            events: summary.events,
            bytes: summary.bytes,
            end: summary.end,
        };
        let bytes = encode(&manifest, MANIFEST_LIMIT)?;
        timed(self.manifest.write_all(&bytes)).await?;
        timed(self.manifest.flush()).await?;
        Ok(summary)
    }
}
/// File rotation preserves one raw sequence/session/causal stream. Directory must
/// exist; ID names a new series. All files use create_new. Finalization writes
/// the reserved manifest after data flush; failure/cancellation forbids continuation.
pub struct RawSeriesWriter {
    inner: RawRecordingWriter<Vec<u8>>,
    storage: Storage,
    poisoned: bool,
}
impl RawSeriesWriter {
    pub async fn create(
        directory: impl AsRef<Path>,
        id: &str,
        limits: RawSeriesLimits,
    ) -> Result<Self> {
        check_id(id)?;
        let directory = directory.as_ref().to_owned();
        let manifest = create(manifest_path(&directory, id)).await?;
        let current = create(part_path(&directory, id, 0)).await?;
        let inner = RawRecordingWriter::new(Vec::new(), limits.recording).await?;
        let mut this = Self {
            inner,
            storage: Storage {
                directory,
                id: id.into(),
                limits,
                manifest,
                current,
                parts: vec![Part {
                    bytes: 0,
                    fnv1a64: BASIS,
                }],
            },
            poisoned: true,
        };
        this.drain().await?;
        this.poisoned = false;
        Ok(this)
    }
    async fn drain(&mut self) -> Result<()> {
        let mut bytes = std::mem::take(&mut self.inner.writer);
        self.storage.write_line(&bytes).await?;
        bytes.clear();
        self.inner.writer = bytes;
        Ok(())
    }
    pub async fn append(&mut self, observation: RawObservation) -> Result<bool> {
        if self.poisoned {
            return Err(invalid());
        }
        self.poisoned = true;
        let accepted = self.inner.append(observation).await?;
        self.drain().await?;
        self.poisoned = false;
        Ok(accepted)
    }
    pub async fn finish(self, end: RecordingEnd) -> Result<RecordingSummary> {
        if self.poisoned {
            return Err(invalid());
        }
        let (footer, summary) = self.inner.finish_with_writer(end).await?;
        let mut storage = self.storage;
        storage.write_line(&footer).await?;
        storage.finish(summary).await
    }
}
pub async fn record_raw_series(
    mut writer: RawSeriesWriter,
    mut input: RawCaptureReceiver,
    mut shutdown: watch::Receiver<bool>,
) -> Result<RecordingSummary> {
    let end = loop {
        let observation = tokio::select! {biased;
            _=super::super::super::stopped(&mut shutdown)=>break RecordingEnd::Stopped,
            result=input.recv()=>match result? {Some(value)=>value,None=>break RecordingEnd::Complete}
        };
        let accepted = tokio::select! {biased;
            _=super::super::super::stopped(&mut shutdown)=>return Err(Error::Transport("raw series interrupted during append".into())),
            result=writer.append(observation)=>result?
        };
        if !accepted
            || writer.inner.state.sequence >= writer.storage.limits.recording.records.max_events
        {
            break RecordingEnd::LimitReached;
        }
    };
    writer.finish(end).await
}

type Opening = Pin<Box<dyn Future<Output = std::io::Result<File>> + Send>>;
struct Input {
    directory: PathBuf,
    id: String,
    parts: Vec<Part>,
    index: usize,
    current: Option<File>,
    opening: Option<Opening>,
    bytes: u64,
    checksum: u64,
}
fn bad_bytes() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "raw series part size/checksum mismatch",
    )
}
impl AsyncRead for Input {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if this.index == this.parts.len() {
                return Poll::Ready(Ok(()));
            }
            if this.current.is_none() {
                let opening = this.opening.get_or_insert_with(|| {
                    Box::pin(File::open(part_path(&this.directory, &this.id, this.index)))
                });
                let file = match opening.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result?,
                };
                this.current = Some(file);
                this.opening = None;
            }
            let expected = &this.parts[this.index];
            if this.bytes == expected.bytes {
                let mut extra = [0_u8; 1];
                let mut probe = ReadBuf::new(&mut extra);
                match Pin::new(this.current.as_mut().expect("file")).poll_read(cx, &mut probe) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result?,
                };
                if !probe.filled().is_empty() || this.checksum != expected.fnv1a64 {
                    return Poll::Ready(Err(bad_bytes()));
                }
                this.index += 1;
                this.current = None;
                this.bytes = 0;
                this.checksum = BASIS;
                continue;
            }
            let count = out
                .remaining()
                .min((expected.bytes - this.bytes).min(usize::MAX as u64) as usize);
            let mut buffer = ReadBuf::new(out.initialize_unfilled_to(count));
            match Pin::new(this.current.as_mut().expect("file")).poll_read(cx, &mut buffer) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            };
            let count = buffer.filled().len();
            if count == 0 {
                return Poll::Ready(Err(bad_bytes()));
            }
            this.checksum = hash(this.checksum, buffer.filled());
            this.bytes += count as u64;
            out.advance(count);
            return Poll::Ready(Ok(()));
        }
    }
}
/// Streams manifest-ordered parts into the unchanged raw validator/parser replay.
/// Final certification requires every part and the logical raw footer to agree.
pub struct RawSeriesReader {
    inner: RawRecordingReader<BufReader<Input>>,
    expected: RecordingSummary,
    summary: Option<RecordingSummary>,
    poisoned: bool,
}
impl RawSeriesReader {
    pub async fn open(
        directory: impl AsRef<Path>,
        id: &str,
        limits: RawSeriesLimits,
    ) -> Result<Self> {
        check_id(id)?;
        let directory = directory.as_ref().to_owned();
        let file = timed(File::open(manifest_path(&directory, id))).await?;
        let mut bytes = Vec::new();
        timed(file.take(MANIFEST_LIMIT as u64 + 1).read_to_end(&mut bytes)).await?;
        if bytes.len() > MANIFEST_LIMIT {
            return Err(invalid());
        }
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        let total = manifest
            .parts
            .iter()
            .try_fold(0_u64, |total, part| total.checked_add(part.bytes));
        if manifest.format != "cryptofeed-rs.raw-series"
            || manifest.version != 1
            || manifest.id != id
            || manifest.parts.is_empty()
            || manifest.parts.len() > limits.max_parts
            || manifest
                .parts
                .iter()
                .any(|part| part.bytes == 0 || part.bytes > limits.part_bytes)
            || total != Some(manifest.bytes)
            || manifest.bytes > limits.recording.records.max_bytes
            || manifest.events > limits.recording.records.max_events
        {
            return Err(invalid());
        }
        let expected = RecordingSummary {
            events: manifest.events,
            bytes: manifest.bytes,
            end: manifest.end,
        };
        let input = Input {
            directory,
            id: id.into(),
            parts: manifest.parts,
            index: 0,
            current: None,
            opening: None,
            bytes: 0,
            checksum: BASIS,
        };
        Ok(Self {
            inner: RawRecordingReader::new(BufReader::new(input), limits.recording),
            expected,
            summary: None,
            poisoned: false,
        })
    }
    pub fn summary(&self) -> Option<RecordingSummary> {
        self.summary
    }
    pub(crate) fn read_bytes(&self) -> u64 {
        self.inner.read_bytes()
    }
    /// Number of parts declared by the bounded manifest, not EOF certification.
    pub fn part_count(&self) -> usize {
        self.inner.reader.get_ref().parts.len()
    }
    fn certify(&mut self, summary: RecordingSummary) -> Result<()> {
        if summary != self.expected {
            return Err(invalid());
        }
        self.summary = Some(summary);
        Ok(())
    }
    pub async fn next_observation(&mut self) -> Result<Option<RawObservation>> {
        if self.poisoned {
            return Err(invalid());
        }
        self.poisoned = true;
        let observation = self.inner.next_observation().await?;
        if observation.is_none() {
            self.certify(self.inner.summary().ok_or_else(invalid)?)?;
        }
        self.poisoned = false;
        Ok(observation)
    }
    pub async fn replay<F, Fut>(
        &mut self,
        options: RawReplayOptions,
        shutdown: watch::Receiver<bool>,
        callback: F,
    ) -> Result<Option<RawReplaySummary>>
    where
        F: FnMut(RawReplayItem) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if self.poisoned {
            return Err(invalid());
        }
        self.poisoned = true;
        let result = self.inner.replay(options, shutdown, callback).await?;
        if let Some(summary) = result {
            self.certify(summary.recording)?;
            self.poisoned = false;
        }
        Ok(result)
    }
}
#[cfg(test)]
mod tests;
