use cryptofeed_rs::prelude::*;

#[tokio::test]
async fn unsupported_catalog_fails_without_network() {
    assert!(
        MarketCatalog::load(ExchangeId::Binance, InstrumentKind::Option)
            .await
            .is_err()
    );
    assert!(
        MarketCatalog::load(ExchangeId::Coinbase, InstrumentKind::Spot)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unsupported_catalog_refresh_fails_without_network() {
    assert!(
        MarketCatalog::refresh(ExchangeId::Binance, InstrumentKind::Option)
            .await
            .is_err()
    );
    assert!(
        MarketCatalog::refresh(ExchangeId::Coinbase, InstrumentKind::Spot)
            .await
            .is_err()
    );
}

#[test]
fn bulk_symbols_append_to_existing_symbols() {
    let feed = Binance::new()
        .symbol("BTC-USDT")
        .symbols(["eth-usdt", "SOL-USDT"])
        .build();
    assert_eq!(
        feed.symbols.iter().map(Symbol::as_str).collect::<Vec<_>>(),
        ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
    );
}

#[test]
fn bulk_instruments_preserve_product_identity() {
    let feed = Binance::new()
        .instruments([
            Symbol::perpetual("BTC", "USDT"),
            Symbol::perpetual("ETH", "USDT"),
        ])
        .build();
    assert_eq!(feed.symbols.len(), 2);
    assert!(
        feed.symbols
            .iter()
            .all(|s| s.kind() == InstrumentKind::Perpetual)
    );
}

#[tokio::test]
async fn pre_signalled_shutdown_skips_invalid_feed_hydration() {
    let mut handler = FeedHandler::new();
    handler.add_feed(Binance::new().symbol("invalid").build());
    let (_tx, rx) = tokio::sync::watch::channel(true);
    handler.run_with_shutdown(rx).await.unwrap();
}
