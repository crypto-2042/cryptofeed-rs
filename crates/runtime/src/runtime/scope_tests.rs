use super::*;
use crate::exchange::{bybit::Bybit, okx::Okx};
#[test]
fn bybit_product_scope_retains_only_its_sparse_pairs_and_aligned_native_names() {
    let feed = Bybit::new()
        .subscription(Channel::Trade, ["BTC-USDT-PERP"])
        .subscription(Channel::Ticker, ["BTC-USD-PERP"])
        .exchange_symbol("BTCUSDT")
        .exchange_symbol("BTCUSD")
        .build();
    let linear = bybit_feed_for_url(
        &feed,
        BybitAdapter::websocket_url_for_product(BybitProduct::Linear),
    );
    let inverse = bybit_feed_for_url(
        &feed,
        BybitAdapter::websocket_url_for_product(BybitProduct::Inverse),
    );
    assert!(linear.validate_subscription_configuration().is_ok());
    assert!(inverse.validate_subscription_configuration().is_ok());
    assert_eq!(linear.channels, vec![Channel::Trade]);
    assert_eq!(inverse.channels, vec![Channel::Ticker]);
    assert_eq!(linear.exchange_symbols, vec!["BTCUSDT"]);
    assert_eq!(inverse.exchange_symbols, vec!["BTCUSD"]);
}

#[test]
fn okx_public_business_scope_contains_only_its_sparse_rules_and_symbol_union() {
    let feed = Okx::new()
        .subscription(Channel::L2Book, ["BTC-USDT"])
        .subscription(Channel::Candles, ["ETH-USDT"])
        .exchange_symbol("BTC-USDT")
        .exchange_symbol("ETH-USDT")
        .build();
    let public = okx_feed_for_url(&feed, OkxAdapter::websocket_url());
    let business = okx_feed_for_url(&feed, "wss://ws.okx.com/ws/v5/business");
    assert_eq!(
        public
            .channel_subscriptions
            .iter()
            .map(|(channel, _)| *channel)
            .collect::<Vec<_>>(),
        vec![Channel::L2Book]
    );
    assert_eq!(
        business
            .channel_subscriptions
            .iter()
            .map(|(channel, _)| *channel)
            .collect::<Vec<_>>(),
        vec![Channel::Candles]
    );
    assert_eq!(
        public
            .symbols
            .iter()
            .map(|symbol| symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["BTC-USDT"]
    );
    assert_eq!(
        business
            .symbols
            .iter()
            .map(|symbol| symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["ETH-USDT"]
    );
    assert_eq!(public.exchange_symbols, vec!["BTC-USDT"]);
    assert_eq!(business.exchange_symbols, vec!["ETH-USDT"]);
}

#[cfg(feature = "orderbook")]
fn seed(feed: &ExchangeFeed, symbol: &cryptofeed_core::symbol::Symbol) {
    feed.orderbook_states.lock().unwrap().insert(
        symbol.as_str().into(),
        cryptofeed_orderbook::L2BookState::new(symbol.clone()),
    );
    feed.bybit_book_syncs
        .lock()
        .unwrap()
        .insert(symbol.as_str().into(), BybitBookSync::new(symbol.clone()));
}
#[cfg(feature = "orderbook")]
#[test]
fn bybit_reset_is_limited_to_owned_l2_pairs_not_other_product_or_price_only_symbols() {
    use cryptofeed_core::symbol::Symbol;
    let linear = Symbol::perpetual("BTC", "USDT");
    let inverse = Symbol::perpetual("BTC", "USD");
    let price = Symbol::perpetual("ETH", "USDT");
    let feed = Bybit::new()
        .subscription(Channel::L2Book, [linear.as_str(), inverse.as_str()])
        .subscription(Channel::Ticker, [price.as_str()])
        .exchange_symbol("BTCUSDT")
        .exchange_symbol("BTCUSD")
        .exchange_symbol("ETHUSDT")
        .build();
    for symbol in [&linear, &inverse, &price] {
        seed(&feed, symbol);
    }
    {
        use crate::exchange::bybit::book_sync::{BybitBookAction, BybitDepthUpdate};
        use cryptofeed_orderbook::{L2Book, L2BookSnapshot, PriceLevel};
        let level = PriceLevel {
            price: 100.into(),
            amount: 2.into(),
        };
        let snapshot = L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bybit,
            symbol: inverse.clone(),
            bids: vec![level],
            asks: vec![],
            exchange_ts: 1.0,
            received_ts: 1.0,
        });
        feed.bybit_book_syncs
            .lock()
            .unwrap()
            .get_mut(inverse.as_str())
            .unwrap()
            .apply(BybitDepthUpdate {
                action: BybitBookAction::Snapshot,
                update_id: 10,
                seq: Some(20),
                book: snapshot,
            })
            .unwrap();
    }
    let planned = bybit_feed_for_url(
        &feed,
        BybitAdapter::websocket_url_for_product(BybitProduct::Linear),
    );
    assert!(std::sync::Arc::ptr_eq(
        &planned.orderbook_states,
        &feed.orderbook_states
    ));
    reset_connection_books(&planned, true, &planned.symbols);
    let state = feed.orderbook_states.lock().unwrap();
    assert!(!state.contains_key(linear.as_str()));
    assert!(state.contains_key(inverse.as_str()));
    assert!(state.contains_key(price.as_str()));
    drop(state);
    let syncs = feed.bybit_book_syncs.lock().unwrap();
    assert!(!syncs.contains_key(linear.as_str()));
    assert!(syncs.contains_key(inverse.as_str()));
    drop(syncs);
    {
        use crate::exchange::bybit::book_sync::{BybitBookAction, BybitDepthUpdate};
        use cryptofeed_orderbook::{L2Book, L2BookDelta, PriceLevel};
        let delta = L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Bybit,
            symbol: inverse.clone(),
            bids: vec![PriceLevel {
                price: 100.into(),
                amount: 3.into(),
            }],
            asks: vec![],
            exchange_ts: 2.0,
            received_ts: 2.0,
        });
        let applied = feed
            .bybit_book_syncs
            .lock()
            .unwrap()
            .get_mut(inverse.as_str())
            .unwrap()
            .apply(BybitDepthUpdate {
                action: BybitBookAction::Delta,
                update_id: 11,
                seq: Some(21),
                book: delta,
            })
            .unwrap()
            .unwrap();
        assert_eq!(applied.bids()[0].amount.to_string(), "3");
    }
}
#[cfg(all(feature = "orderbook", feature = "candles"))]
#[test]
fn okx_business_reconnect_does_not_clear_public_book_cache() {
    use cryptofeed_core::symbol::Symbol;
    let symbol = Symbol::spot("BTC", "USDT");
    let feed = Okx::new()
        .l2_book()
        .candles()
        .symbol(symbol.as_str())
        .exchange_symbol("BTC-USDT")
        .build();
    seed(&feed, &symbol);
    let business = okx_feed_for_url(&feed, "wss://ws.okx.com/ws/v5/business");
    reset_connection_books(&business, true, &business.symbols);
    assert!(
        feed.orderbook_states
            .lock()
            .unwrap()
            .contains_key(symbol.as_str())
    );
    let public = okx_feed_for_url(&feed, OkxAdapter::websocket_url());
    reset_connection_books(&public, true, &public.symbols);
    assert!(
        !feed
            .orderbook_states
            .lock()
            .unwrap()
            .contains_key(symbol.as_str())
    );
}
#[cfg(all(feature = "orderbook", feature = "trade"))]
#[test]
fn binance_market_connection_does_not_clear_public_depth_state() {
    use cryptofeed_core::symbol::Symbol;
    let symbol = Symbol::perpetual("BTC", "USDT");
    let feed = crate::exchange::binance::Binance::new()
        .trade()
        .l2_book()
        .instrument(symbol.clone())
        .exchange_symbol("BTCUSDT")
        .build();
    seed(&feed, &symbol);
    let plans = BinanceAdapter::connection_plans(&feed).unwrap();
    let market = plans
        .iter()
        .find(|plan| plan.snapshot_urls.is_empty())
        .expect("market plan");
    reset_connection_books(
        &feed,
        !market.snapshot_urls.is_empty(),
        market
            .instruments
            .iter()
            .map(|instrument| &instrument.symbol),
    );
    assert!(
        feed.orderbook_states
            .lock()
            .unwrap()
            .contains_key(symbol.as_str())
    );
    let public = plans
        .iter()
        .find(|plan| !plan.snapshot_urls.is_empty())
        .expect("depth plan");
    reset_connection_books(
        &feed,
        true,
        public
            .instruments
            .iter()
            .map(|instrument| &instrument.symbol),
    );
    assert!(
        !feed
            .orderbook_states
            .lock()
            .unwrap()
            .contains_key(symbol.as_str())
    );
}
#[cfg(feature = "orderbook")]
#[test]
fn gate_settlement_plan_reset_preserves_other_settlement() {
    use cryptofeed_core::symbol::Symbol;
    let usdt = Symbol::perpetual("BTC", "USDT");
    let btc = Symbol::perpetual("BTC", "USD");
    let feed = crate::exchange::gateio::Gateio::new()
        .l2_book()
        .instruments([usdt.clone(), btc.clone()])
        .exchange_symbol("BTC_USDT")
        .exchange_symbol("BTC_USD")
        .build();
    seed(&feed, &usdt);
    seed(&feed, &btc);
    let plans = GateioAdapter::connection_plans(&feed).unwrap();
    let plan = plans
        .iter()
        .find(|plan| plan.product == crate::exchange::gateio::adapter::GateioProduct::UsdtPerpetual)
        .unwrap();
    reset_connection_books(
        &feed,
        !plan.snapshot_urls.is_empty(),
        plan.instruments.iter().map(|instrument| &instrument.symbol),
    );
    assert!(
        !feed
            .orderbook_states
            .lock()
            .unwrap()
            .contains_key(usdt.as_str())
    );
    assert!(
        feed.orderbook_states
            .lock()
            .unwrap()
            .contains_key(btc.as_str())
    );
}

#[cfg(feature = "recording")]
#[tokio::test]
async fn physical_sparse_contexts_roundtrip_the_raw_state_validator() {
    use crate::recording::RecordingEnd;
    use crate::recording::raw::{
        RawFeedInfo, RawRecordingLimits, RawRecordingReader, RawRecordingWriter,
        raw_capture_channel,
    };
    let bybit = Bybit::new()
        .subscription(Channel::Trade, ["BTC-USDT-PERP"])
        .subscription(Channel::Ticker, ["BTC-USD-PERP"])
        .exchange_symbol("BTCUSDT")
        .exchange_symbol("BTCUSD")
        .build();
    let okx = Okx::new()
        .subscription(Channel::L2Book, ["BTC-USDT"])
        .subscription(Channel::Candles, ["ETH-USDT"])
        .exchange_symbol("BTC-USDT")
        .exchange_symbol("ETH-USDT")
        .build();
    let scopes = vec![
        bybit_feed_for_url(
            &bybit,
            BybitAdapter::websocket_url_for_product(BybitProduct::Linear),
        ),
        bybit_feed_for_url(
            &bybit,
            BybitAdapter::websocket_url_for_product(BybitProduct::Inverse),
        ),
        okx_feed_for_url(&okx, OkxAdapter::websocket_url()),
        okx_feed_for_url(&okx, "wss://ws.okx.com/ws/v5/business"),
    ];
    let (capture, mut input) = raw_capture_channel(16, 4096).unwrap();
    for scope in scopes {
        let mut session = capture.session(RawFeedInfo::from_feed(&scope)).unwrap();
        session.close(true);
    }
    drop(capture);
    let mut bytes = Vec::new();
    let mut writer = RawRecordingWriter::new(&mut bytes, RawRecordingLimits::default())
        .await
        .unwrap();
    while let Some(record) = input.recv().await.unwrap() {
        writer.append(record).await.unwrap();
    }
    writer.finish(RecordingEnd::Complete).await.unwrap();
    let mut reader = RawRecordingReader::new(bytes.as_slice(), RawRecordingLimits::default());
    let mut count = 0;
    while reader.next_observation().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 8);
}
