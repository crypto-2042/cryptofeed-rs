use cryptofeed_rs::prelude::*;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: raw_recording_public NEW_PATH")?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    let (capture, input) = raw_capture_channel(64, 1024 * 1024)?;
    let mut handler = FeedHandler::new();
    handler.add_feed(
        Okx::new()
            .trade()
            .symbol("BTC-USDT")
            .raw_capture(capture)
            .build(),
    );
    let control = handler.control_handle();
    let runtime = tokio::spawn(async move { handler.run().await });
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let limits =
        RawRecordingLimits::new(RecordingLimits::new(24, 4 * 1024 * 1024, 1024 * 1024)?, 16)?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        record_raw_stream(&mut file, input, limits, stop),
    )
    .await;
    let shutdown = control.shutdown().await;
    let runtime_result = runtime.await?;
    shutdown?;
    runtime_result?;
    let capture = result??;
    file.sync_all().await?;
    drop(file);
    if capture.events != 24 || capture.end != RecordingEnd::LimitReached {
        return Err("unexpected raw segment cutoff".into());
    }
    let file = tokio::fs::File::open(&path).await?;
    let mut reader = RawRecordingReader::new(tokio::io::BufReader::new(file), limits);
    let mut records = 0;
    let mut packets = 0;
    while let Some(frame) = reader.next_observation().await? {
        if frame.session.feed.exchange != ExchangeId::Okx || frame.session.feed.identity.is_none() {
            return Err("missing recorded context".into());
        }
        records += 1;
        if let RawObservationKind::Received(RawPayload::Json { value, .. }) = frame.kind {
            if value["arg"]["channel"] == "trades" {
                packets += 1;
            }
        }
    }
    let loaded = reader.summary().ok_or("missing validated footer")?;
    if loaded != capture || records != 24 || packets == 0 {
        return Err("raw segment roundtrip mismatch".into());
    }
    println!(
        "raw segment observations={records}, market packets={packets}, bytes={}, end={:?}; offline validation complete",
        loaded.bytes, loaded.end
    );
    Ok(())
}
