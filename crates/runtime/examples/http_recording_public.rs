use cryptofeed_rs::prelude::*;
use std::time::Duration;
use tokio::io::BufReader;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: http_recording_public NEW_OUTPUT_PATH")?;
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    let (capture, input) = http_capture_channel(8, 64 * 1024 * 1024)?;
    let transport = TransportConfig::direct().with_http_capture(capture);
    let work = async move {
        let client =
            PublicRestClient::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, transport)
                .await?;
        let markets = client.catalog().symbols().len();
        let symbol = Symbol::spot("BTC", "USDT");
        let _ = client.ticker(&symbol).await?;
        let book = client.l2_book(&symbol, 5).await?;
        let trades = client.recent_trades(&symbol, 5).await?;
        if book.data.bids.is_empty() || book.data.asks.is_empty() || trades.is_empty() {
            return Err(cryptofeed_core::error::Error::Protocol(
                "empty public query result".into(),
            ));
        }
        Ok::<_, cryptofeed_core::error::Error>(markets)
    };
    let limits = RecordingLimits::new(32, 128 * 1024 * 1024, 64 * 1024 * 1024)?;
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let (markets, summary) = tokio::time::timeout(Duration::from_secs(90), async {
        tokio::try_join!(work, record_http_stream(file, input, limits, shutdown))
    })
    .await??;
    if summary.end != RecordingEnd::Complete {
        return Err("incomplete HTTP capture".into());
    }
    let file = tokio::fs::File::open(&path).await?;
    let mut reader = HttpRecordingReader::new(BufReader::new(file), limits);
    let (mut requests, mut json, mut failed, mut cancelled) = (0, 0, 0, 0);
    while let Some(row) = reader.next_observation().await? {
        match row.kind {
            HttpObservationKind::Request { .. } => requests += 1,
            HttpObservationKind::Finished {
                outcome: HttpOutcome::Json { .. },
                ..
            } => json += 1,
            HttpObservationKind::Finished { .. } => failed += 1,
            HttpObservationKind::Cancelled { .. } => cancelled += 1,
        }
    }
    if reader.summary() != Some(summary) || json < 4 || cancelled != 0 {
        return Err("HTTP codec verification mismatch".into());
    }
    println!(
        "public HTTP markets={markets} requests={requests} json={json} failures={failed} cancelled={cancelled} observations={} bytes={} validated=true",
        summary.events, summary.bytes
    );
    Ok(())
}
