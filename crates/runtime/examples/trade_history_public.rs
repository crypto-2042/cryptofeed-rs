use cryptofeed_rs::prelude::*;
async fn observe(
    exchange: ExchangeId,
    product: InstrumentKind,
    start_ms: u64,
    end_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = PublicRestClient::load(exchange, product).await?;
    let symbol = if product == InstrumentKind::Spot {
        Symbol::spot("BTC", "USDT")
    } else {
        Symbol::perpetual("BTC", "USDT")
    };
    let kind = if exchange == ExchangeId::Binance {
        TradeHistoryKind::Aggregate
    } else {
        TradeHistoryKind::Individual
    };
    let size = 5;
    let query = TradeHistoryQuery::new(start_ms, end_ms, kind)?.limits(size, 2)?;
    let mut batch = client.trade_history(&symbol, query.clone()).await?;
    let mut ids = std::collections::HashSet::new();
    let mut total = 0;
    for label in ["first", "resume"] {
        println!(
            "{exchange:?}/{product:?} {label}: records={} scanned={} pages={} kind={:?} stop={:?} next={}",
            batch.records.len(),
            batch.scanned_rows,
            batch.pages,
            batch.kind,
            batch.stop,
            batch.next.is_some()
        );
        for record in &batch.records {
            if record.exchange != exchange
                || record.symbol != symbol
                || record.exchange_ts * 1000.0 < (start_ms as f64)
                || record.exchange_ts * 1000.0 >= (end_ms as f64)
                || !ids.insert(record.id.clone())
            {
                return Err("history identity/range/duplicate mismatch".into());
            }
        }
        total += batch.records.len();
        if label == "first" {
            let cursor = batch
                .next
                .take()
                .ok_or("expected bounded history continuation")?;
            let cursor: TradeHistoryCursor = serde_json::from_slice(&serde_json::to_vec(&cursor)?)?;
            batch = client
                .trade_history(&symbol, query.clone().resume(cursor))
                .await?;
        }
    }
    if total == 0 {
        return Err("no requested historical executions observed".into());
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now_ms: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let end_ms = now_ms - 120_000;
    let start_ms = end_ms - 300_000;
    let mut failures = Vec::new();
    for exchange in [ExchangeId::Binance, ExchangeId::Okx, ExchangeId::Gateio] {
        for product in [InstrumentKind::Spot, InstrumentKind::Perpetual] {
            if let result @ (Err(_) | Ok(Err(_))) = tokio::time::timeout(
                std::time::Duration::from_secs(90),
                observe(exchange, product, start_ms, end_ms),
            )
            .await
            {
                println!("{exchange:?}/{product:?} failed={result:?}");
                failures.push(format!("{exchange:?}/{product:?}"));
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("trade history failures: {}", failures.join(", ")).into())
    }
}
