use cryptofeed_rs::prelude::*;
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::args()
        .nth(1)
        .ok_or("usage: raw_series_public DIRECTORY NEW_SERIES_ID")?;
    let id = std::env::args().nth(2).ok_or("missing new series ID")?;
    tokio::fs::create_dir_all(&directory).await?;
    let limits = RawSeriesLimits::new(
        RawRecordingLimits::new(RecordingLimits::new(40, 4 * 1024 * 1024, 8192)?, 16)?,
        8192,
        64,
    )?;
    let writer = RawSeriesWriter::create(&directory, &id, limits).await?;
    let (capture, input) = raw_capture_channel(64, 4096)?;
    let mut handler = FeedHandler::new();
    handler.add_feed(
        Okx::new()
            .trade()
            .symbol("BTC-USDT")
            .raw_capture(capture)
            .build(),
    );
    let control = handler.control_handle();
    let runtime = tokio::spawn(handler.run());
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let capture = tokio::time::timeout(
        Duration::from_secs(60),
        record_raw_series(writer, input, shutdown),
    )
    .await;
    let stopped = control.shutdown().await;
    let finished = runtime.await?;
    stopped?;
    finished?;
    let captured = capture??;
    if captured.events != 40 || captured.end != RecordingEnd::LimitReached {
        return Err("unexpected capture prefix".into());
    }
    let mut reader = RawSeriesReader::open(&directory, &id, limits).await?;
    let parts = reader.part_count();
    if parts < 2 {
        return Err("capture did not rotate".into());
    }
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let mut models = 0;
    let summary = reader
        .replay(RawReplayOptions::default(), shutdown, |item| {
            if let RawReplayItem::Market(model) = item {
                let FeedEvent::Trade(trade) = model.event else {
                    return std::future::ready(Err(cryptofeed_core::error::Error::Protocol(
                        "unexpected replay category".into(),
                    )));
                };
                if trade.exchange != ExchangeId::Okx || trade.symbol != Symbol::spot("BTC", "USDT")
                {
                    return std::future::ready(Err(cryptofeed_core::error::Error::Protocol(
                        "unexpected replay scope".into(),
                    )));
                }
                models += 1;
            }
            std::future::ready(Ok(()))
        })
        .await?
        .ok_or("replay stopped")?;
    if summary.recording != captured || summary.models == 0 || summary.models != models {
        return Err("series replay mismatch".into());
    }
    println!(
        "raw series observations={} parts={parts} bytes={} offline_models={models} certified=true",
        captured.events, captured.bytes
    );
    Ok(())
}
