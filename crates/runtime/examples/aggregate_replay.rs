//! Offline arrival-clock OHLCV over a normalized trade recording.
use cryptofeed_rs::prelude::*;
use rust_decimal::Decimal;
use std::time::Duration;
use tokio::io::BufReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: aggregate_replay RECORDING_PATH [WINDOW_SECONDS]")?;
    let seconds: u64 = std::env::args().nth(2).unwrap_or("60".into()).parse()?;
    let file = tokio::fs::File::open(path).await?;
    let mut reader = RecordingReader::new(BufReader::new(file), RecordingLimits::default());
    let mut aggregator = Ohlcv::new(Duration::from_secs(seconds), 256)?;
    let mut bars = Vec::new();
    let mut trades = 0_u64;
    let mut volume = Decimal::ZERO;
    while let Some(record) = reader.next_event().await? {
        let FeedEvent::Trade(trade) = record.event else {
            return Err("this example requires a trade-only recording".into());
        };
        trades += 1;
        volume = volume.checked_add(trade.amount).ok_or("volume overflow")?;
        bars.extend(aggregator.push(
            record.identity,
            &trade,
            Duration::from_nanos(record.elapsed_ns),
        )?);
    }
    bars.extend(aggregator.finish());
    let output_trades: u64 = bars.iter().map(|bar| bar.trades).sum();
    let output_volume = bars.iter().try_fold(Decimal::ZERO, |volume, bar| {
        volume.checked_add(bar.volume).ok_or("volume overflow")
    })?;
    if output_trades != trades || output_volume != volume {
        return Err("aggregate input/output count or volume mismatch".into());
    }
    println!(
        "offline trades={trades} bars={} closed={} partial={} count/volume verified",
        bars.len(),
        bars.iter().filter(|bar| bar.closed).count(),
        bars.iter().filter(|bar| !bar.closed).count(),
    );
    Ok(())
}
