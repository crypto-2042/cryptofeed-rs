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
    let query = CandleHistoryQuery::new(start_ms, end_ms, "1m")?.limits(5, 2)?;
    let first = client.candle_history(&symbol, query.clone()).await?;
    println!(
        "{exchange:?}/{product:?} first: records={} scanned={} pages={} stop={:?} next={}",
        first.records.len(),
        first.scanned_rows,
        first.pages,
        first.stop,
        first.next.is_some()
    );
    if first.records.len() != 10 || first.pages != 2 {
        return Err("unexpected first candle batch".into());
    }
    let mut seen = std::collections::HashSet::new();
    for record in &first.records {
        if record.closed.is_some()
            || record.start * 1000.0 < start_ms as f64
            || record.start * 1000.0 >= end_ms as f64
            || record.symbol != symbol
            || !seen.insert(record.start.to_bits())
        {
            return Err("candle identity/duplicate mismatch".into());
        }
    }
    if let Some(cursor) = first.next {
        let cursor: CandleHistoryCursor = serde_json::from_slice(&serde_json::to_vec(&cursor)?)?;
        let next = client.candle_history(&symbol, query.resume(cursor)).await?;
        println!(
            "{exchange:?}/{product:?} resume: records={} scanned={} pages={} stop={:?} next={}",
            next.records.len(),
            next.scanned_rows,
            next.pages,
            next.stop,
            next.next.is_some()
        );
        for record in &next.records {
            if record.closed.is_some()
                || record.start * 1000.0 < start_ms as f64
                || record.start * 1000.0 >= end_ms as f64
                || record.symbol != symbol
                || !seen.insert(record.start.to_bits())
            {
                return Err("candle continuation repeated a record".into());
            }
        }
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let end_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?;
    let start_ms = end_ms - 60 * 60 * 1000;
    let mut failures = Vec::new();
    for exchange in [ExchangeId::Binance, ExchangeId::Bybit] {
        for product in [InstrumentKind::Spot, InstrumentKind::Perpetual] {
            match tokio::time::timeout(
                std::time::Duration::from_secs(90),
                observe(exchange, product, start_ms, end_ms),
            )
            .await
            {
                Ok(Ok(())) => {}
                result => {
                    println!("{exchange:?} failed={result:?}");
                    failures.push(format!("{exchange:?}"));
                }
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "candle history observations failed: {}",
            failures.join(", ")
        )
        .into())
    }
}
