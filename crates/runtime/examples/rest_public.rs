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
    let ticker = client.ticker(&symbol).await?;
    println!(
        "{exchange:?}/{product:?} ticker: bid={} ask={} native_time={:?}",
        ticker.data.bid, ticker.data.ask, ticker.exchange_ts
    );
    let book = client.l2_book(&symbol, 20).await?;
    // This BTC-only manual observation checks gross unit/clock mistakes; it is
    // not a generic quote freshness rule for quiet instruments.
    for (native, received) in [
        (ticker.exchange_ts, ticker.received_ts),
        (book.exchange_ts, book.received_ts),
    ] {
        if native.is_some_and(|time| (time - received).abs() > 300.0) {
            return Err("native timestamp units or local clock skew failed BTC observation".into());
        }
    }
    if book.data.symbol != symbol || ticker.data.symbol != symbol {
        return Err("normalized identity mismatch".into());
    }
    println!(
        "{exchange:?}/{product:?} book: bids={} asks={} native_time={:?} sequence={:?}",
        book.data.bids.len(),
        book.data.asks.len(),
        book.exchange_ts,
        book.sequence
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
            match tokio::time::timeout(
                std::time::Duration::from_secs(60),
                observe(exchange, product),
            )
            .await
            {
                Ok(Ok(())) => {}
                result => {
                    println!("{exchange:?}/{product:?} failed={result:?}");
                    failures.push(format!("{exchange:?}/{product:?}"));
                }
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("REST observations failed: {}", failures.join(", ")).into())
    }
}
