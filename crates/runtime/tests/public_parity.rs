use cryptofeed_orderbook::L2Book;
use cryptofeed_rs::binance::{adapter::BinanceAdapter, parser as binance_parser};
use cryptofeed_rs::bitget::{adapter::BitgetAdapter, parser as bitget_parser};
use rust_decimal::Decimal;
use serde_json::json;

#[test]
fn binance_ticker_matches_python_public_baseline() {
    let message = json!({
        "e": "bookTicker",
        "s": "BTCUSDT",
        "b": "64999.10",
        "a": "65000.20",
        "E": 1710000000456u64
    });

    let ticker = binance_parser::parse_ticker(&message, 1710000001.5).expect("ticker");

    assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    assert_eq!(ticker.bid, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("65000.20").unwrap());
    assert_eq!(ticker.exchange_ts, 1710000000.456);
    assert_eq!(ticker.received_ts, 1710000001.5);
}

#[test]
fn binance_trade_matches_python_public_baseline() {
    let message = json!({
        "s": "BTCUSDT",
        "a": 12345,
        "p": "65000.50",
        "q": "0.01000000",
        "T": 1710000000123u64,
        "m": false
    });

    let trade = binance_parser::parse_trade(&message, 1710000001.5).expect("trade");

    assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    assert_eq!(trade.price, Decimal::from_str_exact("65000.50").unwrap());
    assert_eq!(trade.amount, Decimal::from_str_exact("0.01000000").unwrap());
    assert_eq!(trade.exchange_ts, 1710000000.123);
    assert_eq!(trade.received_ts, 1710000001.5);
    assert_eq!(trade.id.as_deref(), Some("12345"));
}

#[test]
fn binance_l2_book_matches_python_public_baseline() {
    let message = json!({
        "e": "depthUpdate",
        "E": 1710000000456u64,
        "s": "BTCUSDT",
        "U": 100u64,
        "u": 101u64,
        "b": [["64999.10", "1.25"]],
        "a": [["65000.20", "0.75"]]
    });

    let update = binance_parser::parse_l2_book_update(&message, 1710000001.5).expect("book");

    assert_eq!(update.first_update_id, 100);
    assert_eq!(update.last_update_id, 101);
    assert_eq!(update.book.symbol.as_str(), "BTC-USDT");
    assert_eq!(
        update.book.bids[0].price,
        Decimal::from_str_exact("64999.10").unwrap()
    );
    assert_eq!(
        update.book.bids[0].amount,
        Decimal::from_str_exact("1.25").unwrap()
    );
    assert_eq!(
        update.book.asks[0].price,
        Decimal::from_str_exact("65000.20").unwrap()
    );
    assert_eq!(
        update.book.asks[0].amount,
        Decimal::from_str_exact("0.75").unwrap()
    );
}

#[test]
fn binance_candle_matches_python_public_baseline() {
    let message = json!({
        "e": "kline",
        "E": 1615927655524u64,
        "s": "BTCUSDT",
        "k": {
            "t": 1615927620000u64,
            "T": 1615927679999u64,
            "i": "1m",
            "o": "56215.99000000",
            "c": "56232.07000000",
            "h": "56238.59000000",
            "l": "56181.99000000",
            "v": "13.80522200",
            "n": 505u64,
            "x": true
        }
    });

    let candle = binance_parser::parse_candle(&message, 1615927656.0).expect("candle");

    assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    assert_eq!(candle.interval, "1m");
    assert_eq!(candle.start, 1615927620.0);
    assert_eq!(candle.end, 1615927679.999);
    assert_eq!(
        candle.open,
        Decimal::from_str_exact("56215.99000000").unwrap()
    );
    assert_eq!(
        candle.close,
        Decimal::from_str_exact("56232.07000000").unwrap()
    );
    assert_eq!(candle.closed, Some(true));
}

#[test]
fn binance_combined_stream_unwrap_matches_public_baseline() {
    let message = json!({
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        }
    });

    let event = BinanceAdapter::parse_message(&message, 1710000001.5);

    assert!(event.is_some());
}

#[test]
fn bitget_ticker_matches_python_public_baseline() {
    let message = json!({
        "arg": {"instType": "spot", "topic": "ticker", "symbol": "BTCUSDT"},
        "data": [{ "bid1Price": "64999.10", "ask1Price": "65000.20" }],
        "ts": "1710000000456"
    });

    let ticker = bitget_parser::parse_ticker(&message, 1710000001.5).expect("ticker");

    assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    assert_eq!(ticker.bid, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("65000.20").unwrap());
    assert_eq!(ticker.exchange_ts, 1710000000.456);
    assert_eq!(ticker.received_ts, 1710000001.5);
}

#[test]
fn bitget_trade_matches_python_public_baseline() {
    let message = json!({
        "arg": {"instType": "spot", "topic": "publicTrade", "symbol": "BTCUSDT"},
        "data": [{
            "T": "1710000000123",
            "p": "65000.50",
            "v": "0.0100",
            "S": "buy",
            "i": "123456"
        }]
    });

    let trade = bitget_parser::parse_trade(&message, 1710000001.5).expect("trade");

    assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    assert_eq!(trade.price, Decimal::from_str_exact("65000.50").unwrap());
    assert_eq!(trade.amount, Decimal::from_str_exact("0.0100").unwrap());
    assert_eq!(trade.exchange_ts, 1710000000.123);
    assert_eq!(trade.received_ts, 1710000001.5);
    assert_eq!(trade.id.as_deref(), Some("123456"));
}

#[test]
fn bitget_l2_book_matches_python_public_baseline() {
    let message = json!({
        "arg": {"instType": "spot", "topic": "books", "symbol": "BTCUSDT"},
        "action": "snapshot",
        "data": [{
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]],
            "seq": 100u64,
            "pseq": 0u64,
            "ts": "1710000000456"
        }]
    });

    let book = bitget_parser::parse_l2_book(&message, 1710000001.5).expect("book");

    match book {
        L2Book::Delta(delta) => {
            assert_eq!(delta.symbol.as_str(), "BTC-USDT");
            assert_eq!(
                delta.bids[0].price,
                Decimal::from_str_exact("64999.10").unwrap()
            );
            assert_eq!(
                delta.bids[0].amount,
                Decimal::from_str_exact("1.25").unwrap()
            );
            assert_eq!(
                delta.asks[0].price,
                Decimal::from_str_exact("65000.20").unwrap()
            );
            assert_eq!(
                delta.asks[0].amount,
                Decimal::from_str_exact("0.75").unwrap()
            );
        }
        L2Book::Snapshot(_) => panic!("expected delta event model"),
    }
}

#[test]
fn bitget_candle_matches_python_public_baseline() {
    let message = json!({
        "arg": {"instType": "spot", "topic": "candle1m", "symbol": "BTCUSDT"},
        "data": [[
            "1710000000000",
            "65000.00",
            "65100.00",
            "64900.00",
            "65050.00",
            "12.50"
        ]]
    });

    let candle = bitget_parser::parse_candle(&message, 1710000061.0).expect("candle");

    assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    assert_eq!(candle.interval, "1m");
    assert_eq!(candle.start, 1710000000.0);
    assert_eq!(candle.end, 1710000060.0);
    assert_eq!(candle.open, Decimal::from_str_exact("65000.00").unwrap());
    assert_eq!(candle.close, Decimal::from_str_exact("65050.00").unwrap());
    assert_eq!(candle.volume, Decimal::from_str_exact("12.50").unwrap());
}

#[test]
fn bitget_subscribe_ack_is_not_market_data() {
    let message = json!({
        "event": "subscribe",
        "arg": {"instType": "spot", "topic": "publicTrade", "symbol": "BTCUSDT"}
    });

    let event = BitgetAdapter::parse_message(&message, 1710000001.5);

    assert!(event.is_none());
}
