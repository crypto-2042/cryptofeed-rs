use super::*;
use crate::sink::{EventSink, SinkEnd, SinkWrite};

/// JSONL storage adapter for the generic sink runner. Uses the existing bounded,
/// versioned recording format, including exact decimals and strict EOF markers.
impl<W: AsyncWrite + Unpin + Send> EventSink for RecordingWriter<W> {
    type Summary = RecordingSummary;

    async fn write(&mut self, event: FeedEnvelope, elapsed: Duration) -> Result<SinkWrite> {
        if !self.append(event, elapsed).await? {
            return Ok(SinkWrite::Full);
        }
        if self.events >= self.limits.max_events {
            return Ok(SinkWrite::AcceptedAndFull);
        }
        Ok(SinkWrite::Accepted)
    }

    async fn finish(self, end: SinkEnd) -> Result<Self::Summary> {
        let end = match end {
            SinkEnd::Complete => RecordingEnd::Complete,
            SinkEnd::Stopped => RecordingEnd::Stopped,
            SinkEnd::LimitReached => RecordingEnd::LimitReached,
        };
        RecordingWriter::finish(self, end).await
    }
}
