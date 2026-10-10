use cryptofeed_rs::{feed::FeedEvent, prelude::*};
use tokio::io::BufReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: recording_public NEW_OUTPUT_PATH")?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    let mut handler = FeedHandler::new();
    handler.add_feed(Okx::new().trade().symbol("BTC-USDT").build());
    let receiver = handler.subscribe_identified();
    let control = handler.control_handle();
    let runtime = tokio::spawn(async move { handler.run().await });
    let (_stop_tx, stop) = tokio::sync::watch::channel(false);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        record_stream(
            &mut file,
            receiver,
            RecordingLimits::new(20, 4 * 1024 * 1024, 1024 * 1024)?,
            stop,
        ),
    )
    .await;
    let shutdown = control.shutdown().await;
    let runtime_result = runtime.await?;
    shutdown?;
    runtime_result?;
    let capture = result??;
    file.sync_all().await?;
    drop(file);
    if capture.events != 20 || capture.end != RecordingEnd::LimitReached {
        return Err("unexpected capture prefix".into());
    }
    let file = tokio::fs::File::open(&path).await?;
    let mut reader = RecordingReader::new(BufReader::new(file), RecordingLimits::default());
    let (_stop_tx, stop) = tokio::sync::watch::channel(false);
    let mut count = 0;
    let summary = reader
        .replay(ReplayOptions::default(), stop, |record| {
            match record.event {
                FeedEvent::Trade(trade)
                    if trade.exchange == ExchangeId::Okx
                        && trade.symbol == Symbol::spot("BTC", "USDT") =>
                {
                    count += 1
                }
                _ => {
                    return std::future::ready(Err(cryptofeed_core::error::Error::Protocol(
                        "unexpected replay model".into(),
                    )));
                }
            };
            std::future::ready(Ok(()))
        })
        .await?
        .ok_or("replay stopped")?;
    if count != 20 || summary.events != capture.events {
        return Err("replay count mismatch".into());
    }
    println!(
        "capture={} bytes={} end={:?}; offline replay={} verified",
        capture.events, capture.bytes, capture.end, count
    );
    Ok(())
}
