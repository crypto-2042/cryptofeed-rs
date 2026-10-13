use cryptofeed_rs::prelude::*;

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
            let catalog = match MarketCatalog::refresh(exchange, product).await {
                Ok(catalog) => catalog,
                Err(error) => {
                    println!("{exchange:?}/{product:?}: failed={error}");
                    failures.push(format!("{exchange:?}/{product:?}"));
                    continue;
                }
            };
            let symbol = if product == InstrumentKind::Spot {
                Symbol::spot("BTC", "USDT")
            } else {
                Symbol::perpetual("BTC", "USDT")
            };
            let market = catalog.market(&symbol)?;
            println!(
                "{:?}/{:?}: markets={} channels={:?} sample={market:?}",
                catalog.exchange(),
                catalog.product(),
                catalog.markets().len(),
                catalog.supported_channels()
            );
            if market.exchange != exchange
                || catalog.exchange_symbol(&symbol)? != market.exchange_symbol
            {
                return Err("catalog identity mismatch".into());
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("catalog observations failed: {}", failures.join(", ")).into())
    }
}
