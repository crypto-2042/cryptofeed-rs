#[cfg(any(
    feature = "candles",
    feature = "funding",
    feature = "index",
    feature = "openinterest",
    feature = "markprice",
    feature = "orderbook",
    feature = "liquidations",
    feature = "ticker",
    feature = "trade"
))]
use serde_json::json;

#[cfg(feature = "markprice")]
#[test]
fn markprice_only_parses_binance_mark_price_updates() {
    use cryptofeed_rs::binance::adapter::{BinanceAdapter, BinanceEvent};

    let message = json!({
        "e": "markPriceUpdate",
        "E": 1562305380000i64,
        "s": "BTCUSDT",
        "p": "11185.87786614",
        "i": "11154.00000000",
        "r": "0.00030000",
        "T": 1562306400000i64
    });

    let events = BinanceAdapter::parse_messages(&message, 1562305381.0);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, BinanceEvent::MarkPrice(_)))
    );
}

#[cfg(all(feature = "orderbook", not(feature = "ticker")))]
#[test]
fn orderbook_only_parses_gateio_book_ticker_as_l1() {
    use cryptofeed_rs::gateio::adapter::{GateioAdapter, GateioEvent};

    let message = json!({
        "channel": "spot.book_ticker",
        "event": "update",
        "result": {
            "s": "BTC_USDT",
            "b": "64999.10",
            "B": "1.25",
            "a": "65000.20",
            "A": "0.75",
            "t": 1710000000
        }
    });

    let events = GateioAdapter::parse_messages(&message, 1710000001.5);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, GateioEvent::L1Book(_)))
    );
}

#[cfg(feature = "funding")]
#[test]
fn funding_parses_bitget_without_ticker_feature() {
    use cryptofeed_rs::bitget::adapter::{BitgetAdapter, BitgetEvent};
    let message = json!({"arg": {"instType": "usdt-futures", "topic": "ticker", "symbol": "BTCUSDT"},
        "data": [{"fundingRate": "0.0001"}], "ts": 1736371332162i64});
    assert!(
        BitgetAdapter::parse_messages(&message, 1736371332.2)
            .iter()
            .any(|event| matches!(event, BitgetEvent::Funding(_)))
    );
}

#[cfg(feature = "openinterest")]
#[test]
fn openinterest_parses_bitget_without_ticker_feature() {
    use cryptofeed_rs::bitget::adapter::{BitgetAdapter, BitgetEvent};
    let message = json!({"arg": {"instType": "usdt-futures", "topic": "ticker", "symbol": "BTCUSDT"},
        "data": [{"openInterest": "123"}], "ts": 1736371332162i64});
    assert!(
        BitgetAdapter::parse_messages(&message, 1736371332.2)
            .iter()
            .any(|event| matches!(event, BitgetEvent::OpenInterest(_)))
    );
}

#[cfg(feature = "index")]
#[test]
fn index_parses_bitget_without_ticker_feature() {
    use cryptofeed_rs::bitget::adapter::{BitgetAdapter, BitgetEvent};
    let message = json!({"arg": {"instType": "usdt-futures", "topic": "ticker", "symbol": "BTCUSDT"},
        "data": [{"indexPrice": "64999"}], "ts": 1736371332162i64});
    assert!(
        BitgetAdapter::parse_messages(&message, 1736371332.2)
            .iter()
            .any(|event| matches!(event, BitgetEvent::IndexPrice(_)))
    );
}

#[cfg(feature = "markprice")]
#[test]
fn markprice_parses_bitget_without_ticker_feature() {
    use cryptofeed_rs::bitget::adapter::{BitgetAdapter, BitgetEvent};
    let message = json!({"arg": {"instType": "usdt-futures", "topic": "ticker", "symbol": "BTCUSDT"},
        "data": [{"markPrice": "65000"}], "ts": 1736371332162i64});
    assert!(
        BitgetAdapter::parse_messages(&message, 1736371332.2)
            .iter()
            .any(|event| matches!(event, BitgetEvent::MarkPrice(_)))
    );
}

#[cfg(feature = "orderbook")]
#[test]
fn orderbook_parses_bitget_l1_without_ticker_feature() {
    use cryptofeed_rs::bitget::adapter::{BitgetAdapter, BitgetEvent};
    let message = json!({"arg":{"instType":"spot","topic":"books1","symbol":"BTCUSDT"},"action":"snapshot",
        "data":[{"b":[["64999","1"]],"a":[["65000","2"]],"ts":"1736371332162"}]});
    assert!(
        BitgetAdapter::parse_messages(&message, 1736371332.2)
            .iter()
            .any(|event| matches!(event, BitgetEvent::L1Book(_)))
    );
}

#[cfg(feature = "liquidations")]
#[test]
fn liquidations_parses_gateio_public_orders_without_trade_feature() {
    use cryptofeed_rs::gateio::adapter::{GateioAdapter, GateioEvent};
    let message = json!({"channel":"futures.public_liquidates","event":"update","result":[
        {"contract":"BTC_USDT","size":"-1","price":"65000","time_ms":1736371332162i64}]});
    assert!(matches!(
        GateioAdapter::parse_messages(&message, 1736371332.2).first(),
        Some(GateioEvent::Liquidation(_))
    ));
}

#[cfg(feature = "index")]
#[test]
fn index_parses_okx_contract_without_other_features() {
    use cryptofeed_rs::okx::{
        Okx,
        adapter::{OkxAdapter, OkxEvent},
    };
    let feed = Okx::new()
        .index()
        .symbol("BTC-USDT-PERP")
        .exchange_symbol("BTC-USDT-SWAP")
        .build();
    let message = json!({"arg":{"channel":"index-tickers","instId":"BTC-USDT"},
        "data":[{"instId":"BTC-USDT","idxPx":"65000","ts":"1736371332162"}]});
    let events = OkxAdapter::parse_messages_for_feed(&feed, &message, 1736371332.2);
    let Some(OkxEvent::IndexPrice(index)) = events.first() else {
        panic!("index");
    };
    assert_eq!(index.symbol.as_str(), "BTC-USDT-PERP");
}

#[cfg(feature = "index")]
#[test]
fn index_only_parses_binance_mark_price_index() {
    use cryptofeed_rs::binance::adapter::{BinanceAdapter, BinanceEvent};
    let message = json!({"e":"markPriceUpdate","E":1562305380000i64,"s":"BTCUSDT","i":"11784.62659091","T":1562306400000i64});
    assert!(
        BinanceAdapter::parse_messages(&message, 1562305381.0)
            .iter()
            .any(|event| matches!(event, BinanceEvent::IndexPrice(_)))
    );
}

#[test]
fn channel_subscription_preflight_honors_each_feature_boundary() {
    use cryptofeed_rs::prelude::*;
    for (channel, enabled) in [
        (Channel::Trade, cfg!(feature = "trade")),
        (Channel::Ticker, cfg!(feature = "ticker")),
        (Channel::L2Book, cfg!(feature = "orderbook")),
        (Channel::L1Book, cfg!(feature = "orderbook")),
        (Channel::Candles, cfg!(feature = "candles")),
        (Channel::Funding, cfg!(feature = "funding")),
        (Channel::Liquidations, cfg!(feature = "liquidations")),
        (Channel::OpenInterest, cfg!(feature = "openinterest")),
        (Channel::Index, cfg!(feature = "index")),
        (Channel::MarkPrice, cfg!(feature = "markprice")),
    ] {
        let feed = Bybit::new()
            .subscription(channel, ["BTC-USDT-PERP"])
            .build();
        assert_eq!(
            cryptofeed_rs::markets::validate_feed(&feed).is_ok(),
            enabled,
            "{channel:?}"
        );
    }
}

#[cfg(feature = "trade")]
#[test]
fn hyperliquid_trade_parses_without_ticker_or_orderbook_features() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .trade()
        .symbol("BTC-USD-PERP")
        .exchange_symbol("BTC")
        .build();
    let message = json!({"channel": "trades", "data": [{
        "coin": "BTC", "side": "B", "px": "78468.0", "sz": "0.1",
        "time": 1_793_782_868_033_i64, "hash": "0x00", "tid": 1_i64,
        "users": ["a", "b"]
    }]});
    let events =
        HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0).expect("trade events");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::Trade(_)))
    );
}

#[cfg(feature = "orderbook")]
#[test]
fn hyperliquid_snapshot_book_parses_without_ticker_feature() {
    use cryptofeed_rs::hyperliquid::{
        book_sync::{HyperliquidBookOutcome, HyperliquidBookSync},
        parser as hyperliquid_parser,
    };
    use cryptofeed_rs::prelude::{L2Book, Symbol};
    let symbol = Symbol::perpetual("BTC", "USD");
    let data = json!({"coin": "BTC", "time": 1_793_782_877_454_i64, "levels": [
        [{"px": "78454.0", "sz": "0.09296", "n": 5}],
        [{"px": "78455.0", "sz": "20.3117", "n": 62}]
    ]});
    let update = hyperliquid_parser::parse_book(&data, 2.0, &symbol).expect("book");
    let mut sync = HyperliquidBookSync::new(symbol);
    let HyperliquidBookOutcome::Publish(L2Book::Snapshot(book)) = sync.apply(update).unwrap()
    else {
        panic!("expected snapshot");
    };
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks[0].amount.to_string(), "20.3117");
}

#[cfg(feature = "orderbook")]
#[test]
fn hyperliquid_bbo_emits_l1_only_without_ticker_feature() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .l1_book()
        .symbol("PURR-USDC")
        .exchange_symbol("PURR/USDC")
        .build();
    let message = json!({"channel": "bbo", "data": {
        "coin": "PURR/USDC", "time": 1_i64,
        "bbo": [{"px": "0.11525", "sz": "1202.0", "n": 1},
                {"px": "0.11552", "sz": "728.0", "n": 1}]
    }});
    let events =
        HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0).expect("bbo events");
    assert_eq!(events.len(), 1);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::L1Book(_)))
    );
}

#[cfg(feature = "ticker")]
#[test]
fn hyperliquid_bbo_emits_ticker_only_without_orderbook_feature() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .ticker()
        .symbol("BTC-USD-PERP")
        .exchange_symbol("BTC")
        .build();
    let message = json!({"channel": "bbo", "data": {
        "coin": "BTC", "time": 1_i64,
        "bbo": [{"px": "78451.0", "sz": "0.22297", "n": 1},
                {"px": "78452.0", "sz": "13.21482", "n": 74}]
    }});
    let events =
        HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0).expect("bbo events");
    assert_eq!(events.len(), 1);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::Ticker(_)))
    );
    // A documented null side produces no quote at all.
    let one_sided = json!({"channel": "bbo", "data": {
        "coin": "BTC", "time": 1_i64,
        "bbo": [{"px": "78451.0", "sz": "0.22297", "n": 1}, null]
    }});
    assert!(
        HyperliquidAdapter::parse_messages_for_feed(&feed, &one_sided, 1.0)
            .expect("one-sided bbo")
            .is_empty()
    );
}

#[cfg(feature = "candles")]
#[test]
fn hyperliquid_candle_parses_without_ticker_feature() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .candles()
        .candles_interval("1m")
        .symbol("BTC-USD-PERP")
        .exchange_symbol("BTC")
        .build();
    let message = json!({"channel": "candle", "data": {
        "t": 1_793_782_860_000_i64, "T": 1_793_782_919_999_i64, "s": "BTC", "i": "1m",
        "o": "78469.0", "c": "78451.0", "h": "78469.0", "l": "78451.0",
        "v": "11.58778", "n": 86
    }});
    let events =
        HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0).expect("candle events");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::Candle(_)))
    );
}

#[cfg(feature = "funding")]
#[test]
fn hyperliquid_funding_splits_without_ticker_feature() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .funding()
        .symbol("BTC-USD-PERP")
        .exchange_symbol("BTC")
        .build();
    let message = json!({"channel": "activeAssetCtx", "data": {"coin": "BTC", "ctx": {
        "funding": "0.0000125", "openInterest": "39370.73328",
        "oraclePx": "78490.4", "markPx": "78464.8"
    }}});
    let events =
        HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0).expect("funding events");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::Funding(_)))
    );
}

#[cfg(feature = "openinterest")]
#[test]
fn hyperliquid_open_interest_splits_without_ticker_feature() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .open_interest()
        .symbol("BTC-USD-PERP")
        .exchange_symbol("BTC")
        .build();
    let message = json!({"channel": "activeAssetCtx", "data": {"coin": "BTC", "ctx": {
        "funding": "0.0000125", "openInterest": "39370.73328",
        "oraclePx": "78490.4", "markPx": "78464.8"
    }}});
    let events = HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0)
        .expect("open interest events");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::OpenInterest(_)))
    );
}

#[cfg(feature = "markprice")]
#[test]
fn hyperliquid_mark_price_splits_without_ticker_feature() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .mark_price()
        .symbol("PURR-USDC")
        .exchange_symbol("PURR/USDC")
        .build();
    let message = json!({"channel": "activeSpotAssetCtx", "data": {
        "coin": "PURR/USDC", "ctx": {"markPx": "0.11552", "midPx": "0.115385"}
    }});
    let events =
        HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0).expect("mark events");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::MarkPrice(_)))
    );
}

#[cfg(feature = "index")]
#[test]
fn hyperliquid_index_price_splits_without_ticker_feature() {
    use cryptofeed_rs::hyperliquid::adapter::{HyperliquidAdapter, HyperliquidEvent};
    let feed = cryptofeed_rs::prelude::Hyperliquid::new()
        .index()
        .symbol("BTC-USD-PERP")
        .exchange_symbol("BTC")
        .build();
    let message = json!({"channel": "activeAssetCtx", "data": {"coin": "BTC", "ctx": {
        "funding": "0.0000125", "openInterest": "39370.73328",
        "oraclePx": "78490.4", "markPx": "78464.8"
    }}});
    let events =
        HyperliquidAdapter::parse_messages_for_feed(&feed, &message, 1.0).expect("index events");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, HyperliquidEvent::IndexPrice(_)))
    );
}
