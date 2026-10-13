#[cfg(any(
    feature = "funding",
    feature = "index",
    feature = "openinterest",
    feature = "markprice",
    feature = "orderbook",
    feature = "liquidations"
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
