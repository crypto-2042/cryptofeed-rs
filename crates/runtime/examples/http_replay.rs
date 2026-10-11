//! Offline SDK queries over a validated public HTTP recording.
use cryptofeed_rs::prelude::*;
use tokio::io::BufReader;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: http_replay OKX_SPOT_RECORDING")?;
    let file = tokio::fs::File::open(path).await?;
    let limits = RecordingLimits::new(100, 256 * 1024 * 1024, 64 * 1024 * 1024)?;
    let replay = HttpRecordingReader::new(BufReader::new(file), limits)
        .into_replay()
        .await?;
    let transport = TransportConfig::direct().with_http_replay(replay.clone());
    let client = PublicRestClient::load_with_transport(
        ExchangeId::Okx,
        InstrumentKind::Spot,
        transport.clone(),
    )
    .await?;
    let cached =
        MarketCatalog::load_with_transport(ExchangeId::Okx, InstrumentKind::Spot, &transport)
            .await?;
    if cached.symbols() != client.catalog().symbols() || replay.report().consumed != 1 {
        return Err("cached catalog mismatch or unexpected archive consumption".into());
    }
    let markets = client.catalog().symbols().len();
    let symbol = Symbol::spot("BTC", "USDT");
    let ticker = client.ticker(&symbol).await?;
    let book = client.l2_book(&symbol, 5).await?;
    let trades = client.recent_trades(&symbol, 5).await?;
    if ticker.data.bid > ticker.data.ask
        || book.data.bids.is_empty()
        || book.data.asks.is_empty()
        || trades.is_empty()
    {
        return Err("unusable archived query results".into());
    }
    drop(client);
    let report = replay.verify_complete()?;
    println!(
        "offline HTTP markets={markets} requests={} consumed={} unfinished={} cancelled={} mismatches={} book_bids={} book_asks={} trades={}",
        report.requests,
        report.consumed,
        report.unfinished,
        report.cancelled,
        report.mismatches,
        book.data.bids.len(),
        book.data.asks.len(),
        trades.len()
    );
    Ok(())
}
