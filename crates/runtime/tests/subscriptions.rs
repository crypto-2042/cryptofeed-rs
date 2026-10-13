#![cfg(all(feature = "trade", feature = "orderbook", feature = "ticker"))]

use cryptofeed_rs::prelude::*;

#[test]
fn unequal_channel_sets_share_connections_and_preserve_native_mapping() {
    let feed = Binance::new()
        .subscription(Channel::Trade, ["ETH-USDT", "BTC-USDT", "ETH-USDT"])
        .subscription(Channel::Ticker, ["BTC-USDT", "ETH-USDT"])
        .subscription(Channel::L2Book, ["BTC-USDT"])
        .exchange_symbol("ETHUSDT")
        .exchange_symbol("BTCUSDT")
        .build();
    let groups = feed.connection_feeds().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0].channels,
        [Channel::Trade, Channel::Ticker, Channel::L2Book]
    );
    assert_eq!(
        groups[0].symbols,
        [Symbol::spot("ETH", "USDT"), Symbol::spot("BTC", "USDT")]
    );
    assert_eq!(groups[0].exchange_symbols, ["ETHUSDT", "BTCUSDT"]);
    assert!(!groups[0].subscribes(Channel::L2Book, &Symbol::spot("ETH", "USDT")));
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
        assert_eq!(groups.len(), 1);
        let books = &groups[0];
        let planned = match exchange {
            ExchangeId::Binance => BinanceAdapter::connection_plans(books)
                .unwrap()
                .into_iter()
                .flat_map(|p| p.streams)
                .filter(|stream| stream.contains("@depth"))
                .collect::<Vec<_>>()
                .join(" "),
            ExchangeId::Bitget => {
                let value: serde_json::Value =
                    serde_json::from_str(&BitgetAdapter::subscription_message(books)).unwrap();
                value["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|arg| arg["topic"].as_str().unwrap().starts_with("books"))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            }
            ExchangeId::Bybit => {
                let value: serde_json::Value =
                    serde_json::from_str(&BybitAdapter::subscription_message(books)).unwrap();
                value["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|arg| arg.as_str().unwrap().starts_with("orderbook."))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            }
            ExchangeId::Okx => {
                let value: serde_json::Value =
                    serde_json::from_str(&OkxAdapter::subscription_message(books)).unwrap();
                value["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|arg| arg["channel"].as_str().unwrap().starts_with("books"))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            }
            ExchangeId::Gateio => GateioAdapter::subscription_messages(books)
                .into_iter()
                .filter(|message| message.contains("order_book"))
                .collect::<Vec<_>>()
                .join(" "),
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

#[test]
fn sparse_capacity_shards_preserve_every_pair_once_and_omit_empty_channels() {
    let mut builder = Bitget::new()
        .subscription(
            Channel::Trade,
            (0..60).map(|index| format!("S{index}-USDT")),
        )
        .subscription(Channel::L2Book, ["S0-USDT"]);
    for index in 0..60 {
        builder = builder.exchange_symbol(&format!("S{index}USDT"));
    }
    let feed = builder.build();
    let groups = feed.connection_feeds().unwrap();
    assert_eq!(groups.len(), 2);
    let mut pairs = std::collections::HashSet::new();
    for group in &groups {
        cryptofeed_rs::markets::validate_feed(group).unwrap();
        let payload: serde_json::Value = serde_json::from_str(
            &cryptofeed_rs::bitget::adapter::BitgetAdapter::subscription_message(group),
        )
        .unwrap();
        let args = payload["args"].as_array().unwrap();
        assert!(args.len() < 50);
        for arg in args {
            assert!(pairs.insert((
                arg["topic"].as_str().unwrap().to_owned(),
                arg["symbol"].as_str().unwrap().to_owned()
            )));
        }
    }
    assert_eq!(pairs.len(), 61);
    assert!(pairs.contains(&("books".into(), "S0USDT".into())));
    assert!(!groups[1].channels.contains(&Channel::L2Book));
    assert_eq!(
        groups
            .iter()
            .map(|group| group.symbols.len())
            .sum::<usize>(),
        60
    );
}

#[test]
fn manually_incomplete_union_is_rejected_instead_of_dropping_a_requested_channel() {
    let mut feed = Binance::new()
        .subscription(Channel::Trade, ["ETH-USDT"])
        .subscription(Channel::L2Book, ["BTC-USDT"])
        .build();
    feed.symbols
        .retain(|symbol| symbol == &Symbol::spot("ETH", "USDT"));
    assert!(cryptofeed_rs::markets::validate_feed(&feed).is_err());
    assert!(feed.connection_feeds().is_err());
}
