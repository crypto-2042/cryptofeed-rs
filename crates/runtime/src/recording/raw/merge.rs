//! Explicit common-axis composition of independently validated raw captures.
use super::replay::{RawReplayItem, RawReplayOptions, ReplayState};
use super::{
    RawObservation, RawRecordingLimits, RawRecordingReader, RawSeriesLimits, RawSeriesReader,
};
use crate::recording::{RecordingSummary, ReplayTiming};
use cryptofeed_core::error::{Error, Result};
use futures::{FutureExt, future::BoxFuture};
use std::{collections::HashSet, future::Future, path::Path, time::Duration};
use tokio::{io::AsyncBufRead, sync::watch};

trait Source: Send {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RawObservation>>>;
    fn summary(&self) -> Option<RecordingSummary>;
    fn bytes(&self) -> u64;
}
impl<R: AsyncBufRead + Unpin + Send> Source for RawRecordingReader<R> {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RawObservation>>> {
        self.next_observation().boxed()
    }
    fn summary(&self) -> Option<RecordingSummary> {
        self.summary()
    }
    fn bytes(&self) -> u64 {
        self.read_bytes()
    }
}
impl Source for RawSeriesReader {
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RawObservation>>> {
        self.next_observation().boxed()
    }
    fn summary(&self) -> Option<RecordingSummary> {
        self.summary()
    }
    fn bytes(&self) -> u64 {
        self.read_bytes()
    }
}
/// Owns a fresh independent input; session/sequence identities stay capture-local.
pub struct RawMergeInput {
    id: String,
    offset_ns: u64,
    source: Box<dyn Source>,
}
fn identity(id: String, offset: Duration) -> Result<(String, u64)> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(Error::InvalidConfiguration(
            "capture ID requires 1..=64 ASCII letters/digits/-/_".into(),
        ));
    }
    let offset_ns = u64::try_from(offset.as_nanos()).map_err(|_| {
        Error::InvalidConfiguration("capture offset exceeds nanosecond range".into())
    })?;
    Ok((id, offset_ns))
}
impl RawMergeInput {
    pub fn recording<R: AsyncBufRead + Unpin + Send + 'static>(
        id: impl Into<String>,
        offset: Duration,
        reader: R,
        limits: RawRecordingLimits,
    ) -> Result<Self> {
        let (id, offset_ns) = identity(id.into(), offset)?;
        Ok(Self {
            id,
            offset_ns,
            source: Box::new(RawRecordingReader::new(reader, limits)),
        })
    }
    pub async fn series(
        id: impl Into<String>,
        offset: Duration,
        directory: impl AsRef<Path>,
        series_id: &str,
        limits: RawSeriesLimits,
    ) -> Result<Self> {
        let (id, offset_ns) = identity(id.into(), offset)?;
        Ok(Self {
            id,
            offset_ns,
            source: Box::new(RawSeriesReader::open(directory, series_id, limits).await?),
        })
    }
}
#[derive(Clone, Copy, Debug)]
pub struct RawMergeOptions {
    replay: RawReplayOptions,
    maximum_observations: u64,
    maximum_bytes: u64,
}
impl Default for RawMergeOptions {
    fn default() -> Self {
        Self {
            replay: RawReplayOptions::default(),
            maximum_observations: 100_000,
            maximum_bytes: 256 * 1024 * 1024,
        }
    }
}
impl RawMergeOptions {
    pub fn replay(mut self, options: RawReplayOptions) -> Self {
        self.replay = options;
        self
    }
    pub fn input_limits(mut self, maximum_observations: u64, maximum_bytes: u64) -> Result<Self> {
        if maximum_observations == 0 || maximum_bytes == 0 {
            return Err(Error::InvalidConfiguration(
                "merge needs positive aggregate observation/byte limits".into(),
            ));
        }
        self.maximum_observations = maximum_observations;
        self.maximum_bytes = maximum_bytes;
        Ok(self)
    }
}
#[derive(Clone, Debug)]
pub struct RawMergeItem {
    pub capture_id: String,
    /// Caller-aligned elapsed time; normalized native receive/exchange clocks are unchanged.
    pub elapsed_ns: u64,
    pub item: RawReplayItem,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawMergeSummary {
    pub recordings: Vec<(String, RecordingSummary)>,
    pub observations: u64,
    pub bytes: u64,
    pub models: u64,
}
struct Input {
    capture: RawMergeInput,
    head: Option<(u64, RawObservation)>,
    parser: ReplayState,
}
impl Input {
    async fn advance(&mut self, bytes: &mut u64, maximum: u64) -> Result<()> {
        let before = self.capture.source.bytes();
        let next = self.capture.source.next().await?;
        *bytes = bytes
            .checked_add(self.capture.source.bytes() - before)
            .filter(|value| *value <= maximum)
            .ok_or_else(|| Error::Protocol("raw merge aggregate byte budget".into()))?;
        self.head = next
            .map(|record| {
                let at = self
                    .capture
                    .offset_ns
                    .checked_add(record.elapsed_ns)
                    .ok_or_else(|| {
                        Error::MalformedData("raw merge aligned time overflow".into())
                    })?;
                Ok::<_, Error>((at, record))
            })
            .transpose()?;
        Ok(())
    }
}
/// Consumes 1..=16 captures. A common axis is explicit; ties follow input order.
/// Errors/cancellation discard all parser state. Streaming effects are not rolled back.
pub async fn replay_raw_merge<F, Fut>(
    captures: Vec<RawMergeInput>,
    options: RawMergeOptions,
    mut shutdown: watch::Receiver<bool>,
    mut callback: F,
) -> Result<Option<RawMergeSummary>>
where
    F: FnMut(RawMergeItem) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut ids = HashSet::new();
    if captures.is_empty()
        || captures.len() > 16
        || captures.iter().any(|input| !ids.insert(input.id.clone()))
    {
        return Err(Error::InvalidConfiguration(
            "raw merge needs 1..=16 unique capture IDs".into(),
        ));
    }
    let mut inputs: Vec<_> = captures
        .into_iter()
        .map(|capture| Input {
            capture,
            head: None,
            parser: ReplayState::default(),
        })
        .collect();
    let started = tokio::time::Instant::now();
    let mut bytes = 0u64;
    let mut observations = 0u64;
    let mut models = 0u64;
    for input in &mut inputs {
        tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Ok(None),result=input.advance(&mut bytes,options.maximum_bytes)=>result?}
    }
    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        let selected = inputs
            .iter()
            .enumerate()
            .filter_map(|(index, input)| input.head.as_ref().map(|(at, _)| (*at, index)))
            .min();
        let Some((at, index)) = selected else {
            break;
        };
        if observations >= options.maximum_observations {
            return Err(Error::Protocol(
                "raw merge aggregate observation budget".into(),
            ));
        }
        if options.replay.delivery.timing == ReplayTiming::Recorded {
            let deadline = started
                .checked_add(Duration::from_nanos(at))
                .ok_or_else(|| {
                    Error::MalformedData("raw merge elapsed deadline overflow".into())
                })?;
            tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Ok(None),_=tokio::time::sleep_until(deadline)=>{}}
        }
        let input = &mut inputs[index];
        let (_, record) = input.head.take().expect("selected head");
        let mut replay = options.replay;
        replay.maximum_models = input.parser.models + (options.replay.maximum_models - models);
        let before = input.parser.models;
        let output = tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Ok(None),result=input.parser.process(record,replay)=>result?};
        models += input.parser.models - before;
        observations += 1;
        for item in output {
            let event = RawMergeItem {
                capture_id: input.capture.id.clone(),
                elapsed_ns: at,
                item,
            };
            let work = std::panic::AssertUnwindSafe(async { callback(event).await }).catch_unwind();
            tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Ok(None),result=tokio::time::timeout(options.replay.delivery.callback_deadline,work)=>result.map_err(|_|Error::Protocol("raw merge callback deadline".into()))?.map_err(|_|Error::Protocol("raw merge callback panicked".into()))??}
        }
        tokio::select! {biased;_=super::super::stopped(&mut shutdown)=>return Ok(None),result=input.advance(&mut bytes,options.maximum_bytes)=>result?}
    }
    let recordings = inputs
        .into_iter()
        .map(|input| {
            input
                .capture
                .source
                .summary()
                .map(|summary| (input.capture.id, summary))
                .ok_or_else(|| Error::MalformedData("raw merge input did not certify EOF".into()))
        })
        .collect::<Result<_>>()?;
    Ok(Some(RawMergeSummary {
        recordings,
        observations,
        bytes,
        models,
    }))
}

#[cfg(all(test, feature = "trade"))]
mod tests;

#[cfg(test)]
mod boundary_tests {
    use super::*;
    #[tokio::test]
    async fn independent_merge_empty_capture_certifies_without_any_model_feature() {
        let mut bytes = Vec::new();
        let writer =
            super::super::RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
                .await
                .unwrap();
        let expected = writer
            .finish(crate::recording::RecordingEnd::Complete)
            .await
            .unwrap();
        let input = RawMergeInput::recording(
            "empty",
            Duration::ZERO,
            std::io::Cursor::new(bytes),
            RawRecordingLimits::default(),
        )
        .unwrap();
        let (_tx, rx) = watch::channel(false);
        let summary = replay_raw_merge(vec![input], RawMergeOptions::default(), rx, |_| async {
            panic!("empty input has no callbacks")
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(summary.recordings, vec![("empty".into(), expected)]);
        assert_eq!(summary.models, 0);
        assert_eq!(summary.observations, 0);
        assert_eq!(summary.bytes, expected.bytes);
    }
}
