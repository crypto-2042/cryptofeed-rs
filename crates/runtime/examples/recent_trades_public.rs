use cryptofeed_rs::prelude::*;

async fn observe(
    exchange: ExchangeId,
    product: InstrumentKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = PublicRestClient::load(exchange, product).await?;
    let symbol = if product == InstrumentKind::Spot {
        Symbol::spot("BTC", "USDT")
    } else {
        Symbol::perpetual("BTC", "USDT")
    };
    let records = client.recent_trades(&symbol, 5).await?;
    if records.len() != 5 {
        return Err("unexpected recent trade count".into());
    }
    let mut ids = std::collections::HashSet::new();
    let mut previous = 0.0;
    for trade in &records {
        if trade.exchange != exchange
            || trade.symbol != symbol
            || !trade.exchange_ts.is_finite()
            || trade.exchange_ts <= 0.0
            || trade.exchange_ts < previous
            || trade.exchange_ts > trade.received_ts + 60.0
            || !ids.insert(trade.id.as_deref().ok_or("trade ID missing")?)
        {
            return Err("recent trade identity/time/duplicate mismatch".into());
        }
        previous = trade.exchange_ts;
    }
    println!(
        "{exchange:?}/{product:?}: records={} unique_ids={} ascending=true",
        records.len(),
        ids.len()
    );
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut failures = Vec::new();
    for exchange in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ] {
        for product in [InstrumentKind::Spot, InstrumentKind::Perpetual] {
            if let result @ (Err(_) | Ok(Err(_))) = tokio::time::timeout(
                std::time::Duration::from_secs(90),
                observe(exchange, product),
            )
            .await
            {
                println!("{exchange:?}/{product:?}: failed={result:?}");
                failures.push(format!("{exchange:?}/{product:?}"));
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("recent trade failures: {}", failures.join(", ")).into())
    }
}
