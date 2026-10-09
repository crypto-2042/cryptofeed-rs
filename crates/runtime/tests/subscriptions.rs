#![cfg(all(feature = "trade", feature = "orderbook", feature = "ticker"))]

use cryptofeed_rs::prelude::*;

#[test]
fn channel_sets_group_by_membership_and_preserve_native_mapping() {
    let feed = Binance::new()
        .subscription(Channel::Trade, ["ETH-USDT", "BTC-USDT", "ETH-USDT"])
        .subscription(Channel::Ticker, ["BTC-USDT", "ETH-USDT"])
        .subscription(Channel::L2Book, ["BTC-USDT"])
        .exchange_symbol("ETHUSDT")
        .exchange_symbol("BTCUSDT")
        .build();
    let groups = feed.connection_feeds().unwrap();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].channels, [Channel::Trade, Channel::Ticker]);
    assert_eq!(
        groups[0].symbols,
        [Symbol::spot("BTC", "USDT"), Symbol::spot("ETH", "USDT")]
    );
    assert_eq!(groups[0].exchange_symbols, ["BTCUSDT", "ETHUSDT"]);
    assert_eq!(groups[1].channels, [Channel::L2Book]);
    assert_eq!(groups[1].symbols, [Symbol::spot("BTC", "USDT")]);
    assert!(feed.subscribes(Channel::Trade, &Symbol::spot("ETH", "USDT")));
    assert!(!feed.subscribes(Channel::L2Book, &Symbol::spot("ETH", "USDT")));
}

#[test]
fn mapped_and_legacy_modes_cannot_mix_in_either_order() {
    for feed in [
        Binance::new()
            .trade()
            .subscription(Channel::Trade, ["BTC-USDT"])
            .build(),
        Binance::new()
            .subscription(Channel::Trade, ["BTC-USDT"])
            .trade()
            .build(),
        Binance::new()
            .symbol("BTC-USDT")
            .subscription(Channel::Trade, ["BTC-USDT"])
            .build(),
    ] {
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_err());
        assert!(feed.connection_feeds().is_err());
    }
}

#[test]
fn every_channel_must_have_symbols_and_products_must_remain_homogeneous() {
    for feed in [
        Binance::new()
            .subscription(Channel::Trade, ["BTC-USDT"])
            .subscription(Channel::L2Book, [] as [&str; 0])
            .build(),
        Binance::new()
            .subscription(Channel::Trade, ["BTC-USDT"])
            .subscription(Channel::L2Book, ["BTC-USDT-PERP"])
            .build(),
        Binance::new()
            .subscription(Channel::Trade, ["BTC-USDT"])
            .subscription(Channel::Funding, ["BTC-USDT"])
            .build(),
    ] {
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_err());
    }
}

#[test]
fn repeated_channel_entries_merge_and_legacy_feeds_are_unchanged() {
    let feed = Binance::new()
        .subscription(Channel::Trade, ["BTC-USDT"])
        .subscription(Channel::Trade, ["ETH-USDT", "BTC-USDT"])
        .build();
    assert_eq!(feed.connection_feeds().unwrap()[0].symbols.len(), 2);
    let legacy = Binance::new()
        .trade()
        .symbols(["ETH-USDT", "BTC-USDT"])
        .build();
    let groups = legacy.connection_feeds().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].symbols, legacy.symbols);
    assert_eq!(groups[0].channels, legacy.channels);
}

#[test]
fn all_exchange_plans_omit_unrequested_symbol_channel_pairs() {
    use cryptofeed_rs::{
        binance::adapter::BinanceAdapter, bitget::adapter::BitgetAdapter,
        bybit::adapter::BybitAdapter, gateio::adapter::GateioAdapter, okx::adapter::OkxAdapter,
    };
    for exchange in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ] {
        let feed = ExchangeFeedBuilder::new(exchange)
            .subscription(Channel::Trade, ["BTC-USDT", "ETH-USDT"])
            .subscription(Channel::L2Book, ["BTC-USDT"])
            .build();
        let groups = feed.connection_feeds().unwrap();
        cryptofeed_rs::markets::validate_feed(&feed).unwrap();
        let books = &groups[1];
        let planned = match exchange {
            ExchangeId::Binance => BinanceAdapter::connection_plans(books)
                .unwrap()
                .iter()
                .map(|p| p.websocket_url.clone())
                .collect::<Vec<_>>()
                .join(" "),
            ExchangeId::Bitget => BitgetAdapter::subscription_message(books),
            ExchangeId::Bybit => BybitAdapter::subscription_message(books),
            ExchangeId::Okx => OkxAdapter::subscription_message(books),
            ExchangeId::Gateio => GateioAdapter::subscription_messages(books).join(" "),
            _ => unreachable!(),
        };
        assert!(
            planned.to_ascii_uppercase().contains("BTC"),
            "{exchange:?}: {planned}"
        );
        assert!(
            !planned.to_ascii_uppercase().contains("ETH"),
            "{exchange:?}: {planned}"
        );
    }
}

#[tokio::test]
async fn explicit_mappings_remain_globally_unambiguous_before_partitioning() {
    let feed = Binance::new()
        .subscription(Channel::Trade, ["BTC-USDT"])
        .subscription(Channel::L2Book, ["ETH-USDT"])
        .exchange_symbol("BTCUSDT")
        .exchange_symbol("BTCUSDT")
        .build();
    assert!(matches!(
        cryptofeed_rs::markets::resolve_feed_symbols(&feed).await,
        Err(cryptofeed_core::error::Error::AmbiguousSymbol(_))
    ));
}

#[test]
fn typed_channel_symbols_keep_their_product_and_reject_partial_native_maps() {
    let feed = Binance::new()
        .subscription_instruments(
            Channel::Trade,
            [
                Symbol::perpetual("BTC", "USDT"),
                Symbol::perpetual("ETH", "USDT"),
            ],
        )
        .build();
    assert_eq!(
        feed.connection_feeds().unwrap()[0].product_kind().unwrap(),
        InstrumentKind::Perpetual
    );
    let partial = Binance::new()
        .subscription(Channel::Trade, ["BTC-USDT", "ETH-USDT"])
        .exchange_symbol("BTCUSDT")
        .build();
    assert!(partial.connection_feeds().is_err());
}
