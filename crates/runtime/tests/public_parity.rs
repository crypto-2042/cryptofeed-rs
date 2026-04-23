use cryptofeed_orderbook::L2Book;
use cryptofeed_rs::binance::{adapter::BinanceAdapter, parser as binance_parser};
use cryptofeed_rs::bitget::{adapter::BitgetAdapter, parser as bitget_parser};
use cryptofeed_rs::bybit::parser as bybit_parser;
use cryptofeed_rs::gateio::parser as gateio_parser;
use cryptofeed_rs::okx::parser as okx_parser;
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
fn binance_funding_matches_python_public_baseline() {
    let message = json!({
        "e": "markPriceUpdate",
        "E": 1562305380000i64,
        "s": "BTCUSDT",
        "p": "11185.87786614",
        "r": "0.00030000",
        "T": 1562306400000i64
    });

    let funding = binance_parser::parse_funding(&message, 1562305381.0).expect("funding");

    assert_eq!(funding.symbol.as_str(), "BTC-USDT");
    assert_eq!(
        funding.mark_price,
        Some(Decimal::from_str_exact("11185.87786614").unwrap())
    );
    assert_eq!(
        funding.rate,
        Some(Decimal::from_str_exact("0.00030000").unwrap())
    );
    assert_eq!(funding.next_funding_time, Some(1562306400.0));
    assert_eq!(funding.exchange_ts, 1562305380.0);
}

#[test]
fn binance_liquidation_matches_python_public_baseline() {
    let message = json!({
        "e": "forceOrder",
        "E": 1568014460893i64,
        "o": {
            "s": "BTCUSDT",
            "S": "SELL",
            "q": "0.014",
            "p": "9910",
            "X": "FILLED"
        }
    });

    let liquidation =
        binance_parser::parse_liquidation(&message, 1568014461.0).expect("liquidation");

    assert_eq!(liquidation.symbol.as_str(), "BTC-USDT");
    assert_eq!(liquidation.side, "sell");
    assert_eq!(
        liquidation.quantity,
        Decimal::from_str_exact("0.014").unwrap()
    );
    assert_eq!(liquidation.price, Decimal::from_str_exact("9910").unwrap());
    assert_eq!(liquidation.exchange_ts, 1568014460.893);
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

#[test]
fn bybit_ticker_matches_public_baseline() {
    let message = json!({
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486868i64,
        "data": {
            "symbol": "BTCUSDT",
            "bid1Price": "16578.50",
            "ask1Price": "16579.00"
        }
    });

    let ticker = bybit_parser::parse_ticker(&message, 1672304487.0).expect("ticker");

    assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    assert_eq!(ticker.bid, Decimal::from_str_exact("16578.50").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("16579.00").unwrap());
}

#[test]
fn bybit_trade_matches_public_baseline() {
    let message = json!({
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486868i64,
        "data": [{
            "T": 1672304486865i64,
            "s": "BTCUSDT",
            "S": "Buy",
            "v": "0.001",
            "p": "16578.50",
            "i": "20f43950"
        }]
    });

    let trade = bybit_parser::parse_trade(&message, 1672304487.0).expect("trade");

    assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    assert_eq!(trade.price, Decimal::from_str_exact("16578.50").unwrap());
    assert_eq!(trade.amount, Decimal::from_str_exact("0.001").unwrap());
    assert_eq!(trade.id.as_deref(), Some("20f43950"));
}

#[test]
fn bybit_l2_book_matches_public_baseline() {
    let message = json!({
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304484978i64,
        "data": {
            "s": "BTCUSDT",
            "b": [["16493.50", "0.006"]],
            "a": [["16493.60", "0.100"]]
        }
    });

    let book = bybit_parser::parse_l2_book(&message, 1672304485.0).expect("book");

    match book {
        L2Book::Delta(delta) => {
            assert_eq!(delta.symbol.as_str(), "BTC-USDT");
            assert_eq!(
                delta.bids[0].price,
                Decimal::from_str_exact("16493.50").unwrap()
            );
            assert_eq!(
                delta.asks[0].amount,
                Decimal::from_str_exact("0.100").unwrap()
            );
        }
        L2Book::Snapshot(_) => panic!("expected delta event model"),
    }
}

#[test]
fn bybit_candle_matches_public_baseline() {
    let message = json!({
        "topic": "kline.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1672324988882i64,
        "data": [{
            "start": 1672324800000i64,
            "end": 1672324859999i64,
            "interval": "1",
            "open": "16649.5",
            "close": "16677",
            "high": "16677",
            "low": "16608",
            "volume": "2.081",
            "confirm": false
        }]
    });

    let candle = bybit_parser::parse_candle(&message, 1672324989.0).expect("candle");

    assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    assert_eq!(candle.interval, "1");
    assert_eq!(candle.open, Decimal::from_str_exact("16649.5").unwrap());
    assert_eq!(candle.close, Decimal::from_str_exact("16677").unwrap());
}

#[test]
fn okx_ticker_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "tickers", "instId": "BTC-USDT"},
        "data": [{ "bidPx": "64999.10", "askPx": "65000.20", "ts": "1710000000456" }]
    });

    let ticker = okx_parser::parse_ticker(&message, 1710000001.5).expect("ticker");

    assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    assert_eq!(ticker.bid, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("65000.20").unwrap());
}

#[test]
fn okx_trade_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "trades", "instId": "BTC-USDT"},
        "data": [{ "tradeId": "1", "px": "65000.50", "sz": "0.0100", "side": "buy", "ts": "1710000000123" }]
    });

    let trade = okx_parser::parse_trade(&message, 1710000001.5).expect("trade");

    assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    assert_eq!(trade.price, Decimal::from_str_exact("65000.50").unwrap());
    assert_eq!(trade.amount, Decimal::from_str_exact("0.0100").unwrap());
    assert_eq!(trade.id.as_deref(), Some("1"));
}

#[test]
fn okx_l2_book_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "books", "instId": "BTC-USDT"},
        "data": [{
            "bids": [["64999.10", "1.25", "0", "1"]],
            "asks": [["65000.20", "0.75", "0", "1"]],
            "ts": "1710000000456"
        }]
    });

    let book = okx_parser::parse_l2_book(&message, 1710000001.5).expect("book");

    match book {
        L2Book::Delta(delta) => {
            assert_eq!(delta.symbol.as_str(), "BTC-USDT");
            assert_eq!(
                delta.bids[0].price,
                Decimal::from_str_exact("64999.10").unwrap()
            );
        }
        L2Book::Snapshot(_) => panic!("expected delta event model"),
    }
}

#[test]
fn okx_candle_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
        "data": [["1710000000000", "65000.00", "65100.00", "64900.00", "65050.00", "12.50", "0", "0", "1"]]
    });

    let candle = okx_parser::parse_candle(&message, 1710000061.0).expect("candle");

    assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    assert_eq!(candle.interval, "1m");
    assert_eq!(candle.open, Decimal::from_str_exact("65000.00").unwrap());
    assert_eq!(candle.close, Decimal::from_str_exact("65050.00").unwrap());
}

#[test]
fn gateio_ticker_matches_public_baseline() {
    let message = json!({
        "channel": "spot.book_ticker",
        "event": "update",
        "result": {"s": "BTC_USDT", "b": "64999.10", "a": "65000.20", "t": 1710000000}
    });

    let ticker = gateio_parser::parse_ticker(&message, 1710000001.5).expect("ticker");

    assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    assert_eq!(ticker.bid, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("65000.20").unwrap());
}

#[test]
fn gateio_trade_matches_public_baseline() {
    let message = json!({
        "channel": "spot.trades",
        "event": "update",
        "result": [{
            "id": "1",
            "currency_pair": "BTC_USDT",
            "price": "65000.50",
            "amount": "0.0100",
            "side": "buy",
            "create_time_ms": "1710000000123"
        }]
    });

    let trade = gateio_parser::parse_trade(&message, 1710000001.5).expect("trade");

    assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    assert_eq!(trade.price, Decimal::from_str_exact("65000.50").unwrap());
    assert_eq!(trade.amount, Decimal::from_str_exact("0.0100").unwrap());
    assert_eq!(trade.id.as_deref(), Some("1"));
}

#[test]
fn gateio_l2_book_matches_public_baseline() {
    let message = json!({
        "channel": "spot.order_book_update",
        "event": "update",
        "result": {
            "s": "BTC_USDT",
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]],
            "t": 1710000000
        }
    });

    let book = gateio_parser::parse_l2_book(&message, 1710000001.5).expect("book");

    match book {
        L2Book::Delta(delta) => {
            assert_eq!(delta.symbol.as_str(), "BTC-USDT");
            assert_eq!(
                delta.bids[0].price,
                Decimal::from_str_exact("64999.10").unwrap()
            );
        }
        L2Book::Snapshot(_) => panic!("expected delta event model"),
    }
}

#[test]
fn gateio_candle_matches_public_baseline() {
    let message = json!({
        "channel": "spot.candlesticks",
        "event": "update",
        "result": {
            "t": "1710000000",
            "v": "12.50",
            "c": "65050.00",
            "h": "65100.00",
            "l": "64900.00",
            "o": "65000.00",
            "n": "1m_BTC_USDT",
            "w": true
        }
    });

    let candle = gateio_parser::parse_candle(&message, 1710000061.0).expect("candle");

    assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    assert_eq!(candle.interval, "1m");
    assert_eq!(candle.open, Decimal::from_str_exact("65000.00").unwrap());
    assert_eq!(candle.close, Decimal::from_str_exact("65050.00").unwrap());
}
