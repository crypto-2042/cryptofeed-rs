use cryptofeed_rs::prelude::*;

async fn observe(
    exchange: ExchangeId,
    start_ms: u64,
    end_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = PublicRestClient::load(exchange, InstrumentKind::Perpetual).await?;
    let symbol = Symbol::perpetual("BTC", "USDT");
    let query = FundingHistoryQuery::new(start_ms, end_ms)?.limits(5, 2)?;
    let first = client.funding_history(&symbol, query.clone()).await?;
    println!(
        "{exchange:?} first: records={} scanned={} pages={} stop={:?} next={}",
        first.records.len(),
        first.scanned_rows,
        first.pages,
        first.stop,
        first.next.is_some()
    );
    let mut seen = std::collections::HashSet::new();
    for record in &first.records {
        if record.symbol != symbol || !seen.insert(record.exchange_ts.to_bits()) {
            return Err("funding identity/duplicate mismatch".into());
        }
    }
    if let Some(cursor) = first.next {
        let cursor: FundingHistoryCursor = serde_json::from_slice(&serde_json::to_vec(&cursor)?)?;
        let next = client
            .funding_history(&symbol, query.resume(cursor))
            .await?;
        println!(
            "{exchange:?} resume: records={} scanned={} pages={} stop={:?} next={}",
            next.records.len(),
            next.scanned_rows,
            next.pages,
            next.stop,
            next.next.is_some()
        );
        for record in &next.records {
            if record.symbol != symbol || !seen.insert(record.exchange_ts.to_bits()) {
                return Err("funding continuation repeated a record".into());
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
    let start_ms = end_ms - 7 * 24 * 60 * 60 * 1000;
    let mut failures = Vec::new();
    for exchange in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ] {
        match tokio::time::timeout(
            std::time::Duration::from_secs(90),
            observe(exchange, start_ms, end_ms),
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
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "funding history observations failed: {}",
            failures.join(", ")
        )
        .into())
    }
}
