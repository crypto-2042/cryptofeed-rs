use cryptofeed_core::symbol::Symbol;
use cryptofeed_orderbook::L2Book;
use cryptofeed_rs::binance::{
    adapter::{BinanceAdapter, BinanceInstrument, BinanceProduct},
    parser as binance_parser,
};
use cryptofeed_rs::bitget::{adapter::BitgetAdapter, parser as bitget_parser};
use cryptofeed_rs::bybit::parser as bybit_parser;
use cryptofeed_rs::gateio::{
    adapter::{GateioInstrument, GateioProduct},
    parser as gateio_parser,
};
use cryptofeed_rs::okx::parser as okx_parser;
use rust_decimal::Decimal;
use serde_json::json;

#[test]
fn binance_ticker_matches_python_public_baseline() {
    let message = json!({
        "u": 400900217u64,
        "s": "BTCUSDT",
        "b": "64999.10",
        "B": "1.25",
        "a": "65000.20",
        "A": "0.75"
    });

    let ticker = binance_parser::parse_ticker(&message, 1710000001.5).expect("ticker");

    assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    assert_eq!(ticker.bid, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("65000.20").unwrap());
    assert_eq!(ticker.exchange_ts, 1710000001.5);
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
fn binance_non_usdt_spot_symbol_is_not_guessed_as_usdt() {
    let message = json!({
        "s": "ETHBTC",
        "a": 12346,
        "p": "0.03410",
        "q": "1.25000000",
        "T": 1710000000124u64,
        "m": false
    });

    let instrument =
        BinanceInstrument::new(Symbol::spot("ETH", "BTC"), "ETHBTC", BinanceProduct::Spot);
    let trade = binance_parser::parse_trade_for_instrument(&message, 1710000001.5, &instrument)
        .expect("resolved ETH-BTC trade");

    assert_eq!(trade.symbol.as_str(), "ETH-BTC");
}

#[test]
fn binance_dated_coin_future_preserves_resolved_product_identity() {
    let message = json!({
        "s": "BTCUSD_240628",
        "a": 12347,
        "p": "65000.00",
        "q": "2",
        "T": 1710000000125u64,
        "m": true
    });
    let instrument = BinanceInstrument::new(
        Symbol::futures("BTC", "USD", "240628"),
        "BTCUSD_240628",
        BinanceProduct::CoinM,
    );

    let trade = binance_parser::parse_trade_for_instrument(&message, 1710000001.5, &instrument)
        .expect("resolved dated future trade");

    assert_eq!(trade.symbol.as_str(), "BTC-USD-240628");
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

/// Spot partial-depth streams (`@depth5@100ms` etc.) push the complete top-N
/// book as `{lastUpdateId, bids, asks}` with no `e`/`s`/`U`/`u`; the symbol
/// lives only in the combined-stream `stream` name. Shape verified against
/// the official Binance spot stream documentation (2026-08-08; no live
/// capture is available offline, so this inline payload is the reference).
#[test]
fn binance_spot_partial_depth_push_normalizes_last_update_id() {
    let message = json!({
        "stream": "btcusdt@depth5@100ms",
        "data": {
            "lastUpdateId": 160,
            "bids": [["64999.10", "1.25"]],
            "asks": [["65000.20", "0.75"]]
        }
    });
    let instrument =
        BinanceInstrument::new(Symbol::spot("btc", "usdt"), "BTCUSDT", BinanceProduct::Spot);

    let update = binance_parser::parse_partial_depth_for_instrument(
        message.get("data").expect("data"),
        1710000001.5,
        &instrument,
    )
    .expect("partial depth push");

    assert_eq!(update.delta.first_update_id, 160);
    assert_eq!(update.delta.last_update_id, 160);
    assert_eq!(update.delta.book.symbol.as_str(), "BTC-USDT");
    assert_eq!(
        update.delta.book.bids[0].price,
        Decimal::from_str_exact("64999.10").unwrap()
    );
    assert_eq!(
        update.delta.book.asks[0].amount,
        Decimal::from_str_exact("0.75").unwrap()
    );
    assert_eq!(update.previous_update_id, None);
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
    assert_eq!(liquidation.side, cryptofeed_trade::model::Side::Sell);
    assert_eq!(
        liquidation.quantity,
        Decimal::from_str_exact("0.014").unwrap()
    );
    assert_eq!(liquidation.price, Decimal::from_str_exact("9910").unwrap());
    assert_eq!(liquidation.exchange_ts, 1568014460.893);
}

#[test]
fn binance_index_price_matches_public_baseline() {
    // The live USD-M stream emits `e: "IndexUpdate"` (verified 2026-08-07);
    // the docs example uses `indexPriceUpdate`, and both names are accepted.
    for event in ["IndexUpdate", "indexPriceUpdate"] {
        let message = json!({
            "e": event,
            "E": 1591264326123i64,
            "s": "BTCUSDT",
            "i": "BTCUSDT",
            "p": "9501.98000000",
            "T": 1591264326123i64
        });

        let index = binance_parser::parse_index_price(&message, 1591264326.2).expect("index price");

        assert_eq!(index.symbol.as_str(), "BTC-USDT");
        assert_eq!(
            index.price,
            Decimal::from_str_exact("9501.98000000").unwrap()
        );
        assert_eq!(index.open_24h, None);
        assert_eq!(index.exchange_ts, 1591264326.123);
        assert_eq!(index.received_ts, 1591264326.2);
    }
}

#[test]
fn binance_open_interest_matches_public_baseline() {
    let message = json!({
        "e": "openInterest",
        "E": 1589434280000i64,
        "s": "BTCUSDT",
        "o": "19995.08900000",
        "T": 1589434200000i64
    });

    let oi = binance_parser::parse_open_interest(&message, 1589434281.0).expect("open interest");

    assert_eq!(oi.symbol.as_str(), "BTC-USDT");
    assert_eq!(
        oi.open_interest,
        Decimal::from_str_exact("19995.08900000").unwrap()
    );
    assert_eq!(oi.coin_quantity, None);
    assert_eq!(oi.value_usd, None);
    assert_eq!(oi.exchange_ts, 1589434200.0);
}

#[test]
fn binance_mark_price_matches_public_baseline() {
    let message = json!({
        "e": "markPriceUpdate",
        "E": 1562305380000i64,
        "s": "BTCUSDT",
        "p": "11185.87786614",
        "r": "0.00030000",
        "P": "11784.25641265",
        "T": 1562306400000i64
    });

    let mark = binance_parser::parse_mark_price(&message, 1562305381.0).expect("mark price");

    assert_eq!(mark.symbol.as_str(), "BTC-USDT");
    assert_eq!(
        mark.price,
        Decimal::from_str_exact("11185.87786614").unwrap()
    );
    assert_eq!(mark.next_funding_time, Some(1562306400.0));
    assert_eq!(mark.predicted_rate, None);
    assert_eq!(mark.exchange_ts, 1562305380.0);
    assert_eq!(mark.received_ts, 1562305381.0);
}

#[test]
fn binance_book_ticker_emits_both_ticker_and_l1_book() {
    let message = json!({
        "u": 400900217u64,
        "s": "BTCUSDT",
        "b": "64999.10",
        "B": "1.25",
        "a": "65000.20",
        "A": "0.75"
    });

    let events =
        cryptofeed_rs::binance::adapter::BinanceAdapter::parse_messages(&message, 1710000001.5);
    let ticker = events.iter().find_map(|event| match event {
        cryptofeed_rs::binance::adapter::BinanceEvent::Ticker(ticker) => Some(ticker),
        _ => None,
    });
    let l1 = events.iter().find_map(|event| match event {
        cryptofeed_rs::binance::adapter::BinanceEvent::L1Book(book) => Some(book),
        _ => None,
    });

    assert_eq!(ticker.unwrap().symbol.as_str(), "BTC-USDT");
    let book = l1.expect("l1 book");
    assert_eq!(book.symbol.as_str(), "BTC-USDT");
    assert_eq!(book.bid.price, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(book.bid.amount, Decimal::from_str_exact("1.25").unwrap());
    assert_eq!(book.ask.price, Decimal::from_str_exact("65000.20").unwrap());
    assert_eq!(book.ask.amount, Decimal::from_str_exact("0.75").unwrap());
    assert_eq!(book.exchange_ts, 1710000001.5);
    assert_eq!(book.received_ts, 1710000001.5);
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

    let event = BinanceAdapter::parse_message(&message, 1710000001.5).expect("trade event");

    match event {
        cryptofeed_rs::binance::adapter::BinanceEvent::Trade(trade) => {
            assert_eq!(trade.symbol.as_str(), "BTC-USDT");
            assert_eq!(trade.price, Decimal::from_str_exact("65000.50").unwrap());
            assert_eq!(trade.amount, Decimal::from_str_exact("0.01000000").unwrap());
            assert_eq!(trade.exchange_ts, 1710000000.123);
        }
        _ => panic!("expected aggTrade event"),
    }
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
fn bitget_v3_trade_batch_preserves_every_entry_in_exchange_order() {
    let message = json!({
        "arg": {"instType": "usdt-futures", "topic": "publicTrade", "symbol": "BTCUSDT"},
        "action": "snapshot",
        "data": [
            {"p": "100000", "S": "buy", "T": "1736348770627", "v": "0.00118", "i": "1260903622036942849", "L": "1234568787787878787", "isRPI": "no"},
            {"p": "100001", "S": "sell", "T": "1736348770628", "v": "0.00200", "i": "1260903622036942850", "L": "1234568787787878788", "isRPI": "no"}
        ],
        "ts": 1736371104297i64
    });

    let trades = bitget_parser::parse_trades(&message, 1736371105.0);

    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].id.as_deref(), Some("1260903622036942849"));
    assert_eq!(trades[1].id.as_deref(), Some("1260903622036942850"));
    assert_eq!(trades[0].symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(trades[1].exchange_ts, 1736348770.628);
}

#[test]
fn bitget_v3_usdc_future_preserves_product_identity() {
    let message = json!({
        "arg": {"instType": "usdc-futures", "topic": "publicTrade", "symbol": "BTCUSDC"},
        "action": "snapshot",
        "data": [{"p": "65000.00", "S": "buy", "T": "1736348770627", "v": "0.001", "i": "1"}],
        "ts": 1736371104297i64
    });

    let trade = bitget_parser::parse_trade(&message, 1736371105.0).expect("USDC future");

    assert_eq!(trade.symbol.as_str(), "BTC-USDC-PERP");
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
        L2Book::Snapshot(snapshot) => {
            assert_eq!(snapshot.symbol.as_str(), "BTC-USDT");
            assert_eq!(
                snapshot.bids[0].price,
                Decimal::from_str_exact("64999.10").unwrap()
            );
            assert_eq!(
                snapshot.bids[0].amount,
                Decimal::from_str_exact("1.25").unwrap()
            );
            assert_eq!(
                snapshot.asks[0].price,
                Decimal::from_str_exact("65000.20").unwrap()
            );
            assert_eq!(
                snapshot.asks[0].amount,
                Decimal::from_str_exact("0.75").unwrap()
            );
        }
        L2Book::Delta(_) => panic!("v3 action=snapshot must remain a snapshot event"),
    }
}

#[test]
fn bitget_v3_full_depth_update_remains_a_delta() {
    let message = json!({
        "arg": {"instType": "usdt-futures", "topic": "books", "symbol": "BTCUSDT"},
        "action": "update",
        "data": [{
            "b": [["99756.5", "0.0200"]],
            "a": [["99756.7", "0"]],
            "seq": 1304314508780744706u64,
            "pseq": 1304314508780744705u64,
            "maxDepth": "50",
            "ts": "1746698732563"
        }]
    });

    let book = bitget_parser::parse_l2_book(&message, 1746698733.0).expect("book update");

    match book {
        L2Book::Delta(delta) => assert_eq!(delta.symbol.as_str(), "BTC-USDT-PERP"),
        L2Book::Snapshot(_) => panic!("v3 action=update must remain a delta event"),
    }
}

#[test]
fn bitget_candle_matches_python_public_baseline() {
    let message = json!({
        "arg": {"instType": "spot", "topic": "kline", "symbol": "BTCUSDT", "interval": "1m"},
        "action": "snapshot",
        "data": [{
            "start": "1736370720000",
            "open": "65000.00",
            "high": "65100.00",
            "low": "64900.00",
            "close": "65050.00",
            "volume": "12.50",
            "turnover": "813125.00"
        }],
        "ts": 1736370735556i64
    });

    let candle = bitget_parser::parse_candle(&message, 1736370736.0).expect("v3 kline");

    assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    assert_eq!(candle.interval, "1m");
    assert_eq!(candle.start, 1736370720.0);
    assert_eq!(candle.end, 1736370780.0);
    assert_eq!(candle.open, Decimal::from_str_exact("65000.00").unwrap());
    assert_eq!(candle.close, Decimal::from_str_exact("65050.00").unwrap());
    assert_eq!(candle.volume, Decimal::from_str_exact("12.50").unwrap());
    assert_eq!(candle.exchange_ts, 1736370735.556);
}

/// Hourly bars used to silently parse to zero candles (the interval unit was
/// matched uppercase while the v3 wire uses lowercase `1h`); a `1h` bar must
/// produce a real candle with a 3600s duration.
#[test]
fn bitget_hourly_candle_parses_with_wire_interval() {
    let message = json!({
        "arg": {"instType": "spot", "topic": "kline", "symbol": "BTCUSDT", "interval": "1H"},
        "action": "snapshot",
        "data": [{
            "start": "1736373600000",
            "open": "65000.00",
            "high": "65100.00",
            "low": "64900.00",
            "close": "65050.00",
            "volume": "12.50",
            "turnover": "813125.00"
        }],
        "ts": 1736373660000i64
    });

    let candle = bitget_parser::parse_candle(&message, 1736373660.0).expect("1h kline");

    assert_eq!(candle.interval, "1h");
    assert_eq!(candle.start, 1736373600.0);
    assert_eq!(candle.end, 1736377200.0);
    assert_eq!(candle.open, Decimal::from_str_exact("65000.00").unwrap());
}

#[test]
fn bitget_liquidation_matches_public_baseline() {
    let message = json!({
        "arg": {"instType": "usdt-futures", "topic": "liquidation"},
        "data": [{
            "symbol": "BTCUSDT",
            "side": "buy",
            "price": "64000",
            "amount": "37.722858",
            "ts": "1736371332162"
        }],
        "action": "update",
        "ts": 1736371332162i64
    });

    let liquidation =
        bitget_parser::parse_liquidation(&message, 1736371332.2).expect("liquidation");

    assert_eq!(liquidation.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(liquidation.side, cryptofeed_trade::model::Side::Sell);
    assert_eq!(
        liquidation.quantity,
        Decimal::from_str_exact("37.722858").unwrap() / Decimal::from_str_exact("64000").unwrap()
    );
    assert_eq!(liquidation.price, Decimal::from_str_exact("64000").unwrap());
    assert_eq!(liquidation.id, None);
    assert_eq!(
        liquidation.status,
        cryptofeed_liquidations::LiquidationStatus::Filled
    );
    assert_eq!(liquidation.exchange_ts, 1736371332.162);
    assert_eq!(liquidation.received_ts, 1736371332.2);
}

#[test]
fn bitget_subscribe_ack_is_not_market_data() {
    let message = json!({
        "event": "subscribe",
        "arg": {"instType": "spot", "topic": "books", "symbol": "BTCUSDT"},
        "connId": "4a87f8f5"
    });

    let event = BitgetAdapter::parse_message(&message, 1710000001.5);

    assert!(event.is_none());
}

#[test]
fn binance_subscribe_ack_is_a_control_frame() {
    let message = json!({"id": 1, "result": null});

    let events = BinanceAdapter::parse_messages(&message, 1710000001.5);

    assert!(events.is_empty());
}

#[test]
fn okx_error_frame_surfaces_subscription_rejection() {
    use cryptofeed_rs::okx::adapter::{OkxAdapter, OkxControl};
    let message = json!({
        "event": "error",
        "code": "60018",
        "msg": "instId doesn't exist",
        "connId": "a1b2c3"
    });

    let control = OkxAdapter::parse_control_message(&message).expect("control frame");
    assert!(control.is_err());
    assert!(matches!(
        OkxAdapter::parse_control_message(&message),
        Some(Err(_))
    ));
    assert!(matches!(
        OkxAdapter::parse_control_message(&json!({"event": "subscribe"})),
        Some(Ok(OkxControl::Subscribed))
    ));
}

#[test]
fn bybit_spot_24h_statistics_are_not_bbo() {
    let message = json!({
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486868i64,
        "data": {
            "symbol": "BTCUSDT",
            "lastPrice": "16578.50",
            "highPrice24h": "17000.00",
            "lowPrice24h": "16000.00",
            "volume24h": "1234.5"
        }
    });

    assert!(bybit_parser::parse_ticker(&message, 1672304487.0).is_none());
}

#[test]
fn bybit_spot_level_one_book_normalizes_to_bbo() {
    let message = json!({
        "topic": "orderbook.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486868i64,
        "data": {
            "s": "BTCUSDT",
            "b": [["16578.50", "0.001"]],
            "a": [["16579.00", "0.002"]],
            "u": 123u64,
            "seq": 456u64
        }
    });

    let ticker = bybit_parser::parse_bbo_ticker(&message, 1672304487.0).expect("ticker");
    assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    assert_eq!(ticker.bid, Decimal::from_str_exact("16578.50").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("16579.00").unwrap());
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
fn bybit_v5_trade_batch_preserves_every_entry_in_exchange_order() {
    let message = json!({
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486868i64,
        "data": [
            {"T": 1672304486865i64, "s": "BTCUSDT", "S": "Buy", "v": "0.001", "p": "16578.50", "i": "20f43950-a", "seq": 1783284617},
            {"T": 1672304486866i64, "s": "BTCUSDT", "S": "Sell", "v": "0.002", "p": "16578.40", "i": "20f43950-b", "seq": 1783284617}
        ]
    });

    let trades = bybit_parser::parse_trades(&message, 1672304487.0);

    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].id.as_deref(), Some("20f43950-a"));
    assert_eq!(trades[1].id.as_deref(), Some("20f43950-b"));
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
            "a": [["16493.60", "0.100"]],
            "u": 18521288u64,
            "seq": 7961638724u64
        }
    });

    let book = bybit_parser::parse_l2_book(&message, 1672304485.0).expect("book");

    match book {
        L2Book::Snapshot(snapshot) => {
            assert_eq!(snapshot.symbol.as_str(), "BTC-USDT");
            assert_eq!(
                snapshot.bids[0].price,
                Decimal::from_str_exact("16493.50").unwrap()
            );
            assert_eq!(
                snapshot.asks[0].amount,
                Decimal::from_str_exact("0.100").unwrap()
            );
        }
        L2Book::Delta(_) => panic!("replacement snapshot must clear prior book state"),
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
fn bybit_funding_from_tickers_matches_public_baseline() {
    // Bybit no longer serves `funding.{symbol}` (verified live 2026-08-06):
    // funding rides the derivative tickers stream with `fundingRate`,
    // `nextFundingTime`, and `markPrice`.
    let message = json!({
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304487030i64,
        "data": [{
            "symbol": "BTCUSDT",
            "fundingRate": "0.0001",
            "nextFundingTime": "1672304487000",
            "markPrice": "50000",
            "indexPrice": "49950"
        }]
    });

    let funding = bybit_parser::parse_funding(&message, 1672304487.1).expect("funding");

    assert_eq!(funding.symbol.as_str(), "BTC-USDT");
    assert_eq!(
        funding.rate,
        Some(Decimal::from_str_exact("0.0001").unwrap())
    );
    assert_eq!(
        funding.mark_price,
        Some(Decimal::from_str_exact("50000").unwrap())
    );
    assert_eq!(funding.exchange_ts, 1672304487.03);
    assert_eq!(funding.received_ts, 1672304487.1);
    assert_eq!(funding.next_funding_time, Some(1672304487.0));
    assert_eq!(funding.predicted_rate, None);
}

#[test]
fn bybit_open_interest_from_tickers_matches_public_baseline() {
    let message = json!({
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486870i64,
        "data": {
            "symbol": "BTCUSDT",
            "bid1Price": "16578.50",
            "ask1Price": "16579.00",
            "openInterest": "1234.5",
            "openInterestValue": "20000000",
            "indexPrice": "16577.00"
        }
    });

    let oi = bybit_parser::parse_open_interest(&message, 1672304487.1).expect("open interest");

    assert_eq!(oi.symbol.as_str(), "BTC-USDT");
    assert_eq!(oi.open_interest, Decimal::from_str_exact("1234.5").unwrap());
    assert_eq!(
        oi.value_usd,
        Some(Decimal::from_str_exact("20000000").unwrap())
    );
    assert_eq!(oi.coin_quantity, None);
    assert_eq!(oi.exchange_ts, 1672304486.87);
    assert_eq!(oi.received_ts, 1672304487.1);
}

#[test]
fn bybit_index_price_from_tickers_matches_public_baseline() {
    let message = json!({
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486870i64,
        "data": {
            "symbol": "BTCUSDT",
            "lastPrice": "16578.50",
            "indexPrice": "16577.00",
            "markPrice": "16578.00",
            "openInterest": "1234.5"
        }
    });

    let index = bybit_parser::parse_index_price(&message, 1672304487.1).expect("index price");

    assert_eq!(index.symbol.as_str(), "BTC-USDT");
    assert_eq!(index.price, Decimal::from_str_exact("16577.00").unwrap());
    assert_eq!(index.open_24h, None);
    assert_eq!(index.high_24h, None);
    assert_eq!(index.low_24h, None);
    assert_eq!(index.exchange_ts, 1672304486.87);
    assert_eq!(index.received_ts, 1672304487.1);
}

#[test]
fn bybit_mark_price_from_tickers_matches_public_baseline() {
    let message = json!({
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486870i64,
        "data": {
            "symbol": "BTCUSDT",
            "lastPrice": "16578.50",
            "indexPrice": "16577.00",
            "markPrice": "16578.00"
        }
    });

    let mark = bybit_parser::parse_mark_price(&message, 1672304487.1).expect("mark price");

    assert_eq!(mark.symbol.as_str(), "BTC-USDT");
    assert_eq!(mark.price, Decimal::from_str_exact("16578.00").unwrap());
    assert_eq!(mark.next_funding_time, None);
    assert_eq!(mark.predicted_rate, None);
    assert_eq!(mark.exchange_ts, 1672304486.87);
    assert_eq!(mark.received_ts, 1672304487.1);
}

#[test]
fn okx_mark_price_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
        "data": [
            {"instType": "SWAP", "instId": "BTC-USDT-SWAP", "markPx": "97093.7", "ts": "1710000000123"}
        ]
    });

    let mark = okx_parser::parse_mark_price(&message, 1710000001.5).expect("mark price");

    assert_eq!(mark.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(mark.price, Decimal::from_str_exact("97093.7").unwrap());
    assert_eq!(mark.next_funding_time, None);
    assert_eq!(mark.exchange_ts, 1710000000.123);
    assert_eq!(mark.received_ts, 1710000001.5);
}

#[test]
fn bybit_option_ticker_preserves_option_identity() {
    let feed = cryptofeed_rs::bybit::Bybit::new()
        .ticker()
        .instrument(Symbol::option("BTC", "USDC", "30DEC22", "18000", "C"))
        .exchange_symbol("BTC-30DEC22-18000-C")
        .build();
    let message = json!({
        "topic": "tickers.BTC-30DEC22-18000-C",
        "type": "snapshot",
        "ts": 1672304487050i64,
        "data": {
            "symbol": "BTC-30DEC22-18000-C",
            "bidPrice": "0.001",
            "bidSize": "2.5",
            "bidIv": "0.48",
            "askPrice": "0.002",
            "askSize": "3.0",
            "askIv": "0.51",
            "markPriceIv": "0.495"
        }
    });

    let events = cryptofeed_rs::bybit::adapter::BybitAdapter::parse_messages_for_feed(
        &feed,
        &message,
        1672304487.1,
    );
    assert_eq!(events.len(), 1);
    match &events[0] {
        cryptofeed_rs::bybit::adapter::BybitEvent::Ticker(ticker) => {
            assert_eq!(ticker.symbol.as_str(), "BTC-USDC-30DEC22-18000-C");
            assert_eq!(ticker.bid, Decimal::from_str_exact("0.001").unwrap());
            assert_eq!(ticker.ask, Decimal::from_str_exact("0.002").unwrap());
            assert_eq!(
                ticker.implied_volatility,
                Some(Decimal::from_str_exact("0.495").unwrap())
            );
        }
        _ => panic!("expected option ticker"),
    }
}

#[test]
fn binance_option_ticker_and_trade_match_public_baseline() {
    let instrument = BinanceInstrument::new(
        Symbol::option("BTC", "USDT", "250627", "100000", "C"),
        "BTC-250627-100000-C",
        BinanceProduct::Option,
    );
    let ticker = json!({
        "e": "24hrTicker",
        "E": 1710000000456u64,
        "s": "BTC-250627-100000-C",
        "bidOpenPrice": "0.001",
        "askOpenPrice": "0.002",
        "volatility": "0.5"
    });
    let trade = json!({
        "e": "trade",
        "E": 1710000000456u64,
        "s": "BTC-250627-100000-C",
        "tradeId": "100",
        "price": "0.0015",
        "quantity": "1",
        "side": -1i64,
        "tradeTime": 1710000000123u64
    });

    let parsed =
        binance_parser::parse_option_ticker_for_instrument(&ticker, 1710000001.5, &instrument)
            .expect("option ticker");
    assert_eq!(parsed.symbol.as_str(), "BTC-USDT-250627-100000-C");
    assert_eq!(parsed.bid, Decimal::from_str_exact("0.001").unwrap());
    assert_eq!(parsed.ask, Decimal::from_str_exact("0.002").unwrap());
    assert_eq!(
        parsed.implied_volatility,
        Some(Decimal::from_str_exact("0.5").unwrap())
    );

    let parsed =
        binance_parser::parse_option_trade_for_instrument(&trade, 1710000001.5, &instrument)
            .expect("option trade");
    assert_eq!(parsed.symbol.as_str(), "BTC-USDT-250627-100000-C");
    assert!(matches!(parsed.side, cryptofeed_trade::Side::Sell));
    assert_eq!(parsed.price, Decimal::from_str_exact("0.0015").unwrap());
    assert_eq!(parsed.amount, Decimal::from_str_exact("1").unwrap());
}

#[test]
fn bybit_option_trade_resolves_from_base_stream() {
    let feed = cryptofeed_rs::bybit::Bybit::new()
        .trade()
        .instrument(Symbol::option("BTC", "USDC", "30JUN26", "65000", "C"))
        .exchange_symbol("BTC-30JUN26-65000-C")
        .build();
    let message = json!({
        "topic": "publicTrade.BTC",
        "type": "snapshot",
        "ts": 1672304486868i64,
        "data": [{
            "T": 1672304486865i64,
            "s": "BTC-30JUN26-65000-C",
            "S": "Buy",
            "v": "0.5",
            "p": "0.001",
            "i": "opt-trade-1"
        }]
    });

    let events = cryptofeed_rs::bybit::adapter::BybitAdapter::parse_messages_for_feed(
        &feed,
        &message,
        1672304487.1,
    );
    match &events[0] {
        cryptofeed_rs::bybit::adapter::BybitEvent::Trade(trade) => {
            assert_eq!(trade.symbol.as_str(), "BTC-USDC-30JUN26-65000-C");
            assert_eq!(trade.price, Decimal::from_str_exact("0.001").unwrap());
            assert_eq!(trade.amount, Decimal::from_str_exact("0.5").unwrap());
        }
        _ => panic!("expected option trade"),
    }
}

/// A base-coin option batch can span several series of the base coin; each
/// row must resolve its own instrument instead of inheriting the first
/// row's symbol.
#[test]
fn bybit_option_base_coin_batch_resolves_each_series_per_row() {
    let feed = cryptofeed_rs::bybit::Bybit::new()
        .trade()
        .instrument(Symbol::option("BTC", "USDC", "30JUN26", "65000", "C"))
        .exchange_symbol("BTC-30JUN26-65000-C")
        .instrument(Symbol::option("BTC", "USDC", "30JUN26", "70000", "C"))
        .exchange_symbol("BTC-30JUN26-70000-C")
        .build();
    let message = json!({
        "topic": "publicTrade.BTC",
        "type": "snapshot",
        "ts": 1672304486868i64,
        "data": [
            {
                "T": 1672304486865i64,
                "s": "BTC-30JUN26-65000-C",
                "S": "Buy",
                "v": "0.5",
                "p": "0.001",
                "i": "opt-trade-1"
            },
            {
                "T": 1672304486866i64,
                "s": "BTC-30JUN26-70000-C",
                "S": "Sell",
                "v": "1.0",
                "p": "0.002",
                "i": "opt-trade-2"
            }
        ]
    });

    let events = cryptofeed_rs::bybit::adapter::BybitAdapter::parse_messages_for_feed(
        &feed,
        &message,
        1672304487.1,
    );
    assert_eq!(events.len(), 2);
    let symbols: Vec<&str> = events
        .iter()
        .map(|event| match event {
            cryptofeed_rs::bybit::adapter::BybitEvent::Trade(trade) => trade.symbol.as_str(),
            _ => panic!("expected trade"),
        })
        .collect();
    assert_eq!(
        symbols,
        ["BTC-USDC-30JUN26-65000-C", "BTC-USDC-30JUN26-70000-C"]
    );
}

#[test]
fn okx_margin_events_rebind_to_margin_symbol_identity() {
    let feed = cryptofeed_rs::okx::Okx::new()
        .liquidations()
        .candles()
        .instrument(Symbol::margin("BTC", "USDT"))
        .exchange_symbol("BTC-USDT")
        .build();
    let message = json!({
        "arg": {"channel": "liquidation-orders", "instId": "BTC-USDT"},
        "data": [
            {"instId": "BTC-USDT", "px": "64000", "sz": "1.5", "side": "sell", "ts": "1710000000123"}
        ]
    });

    let events = cryptofeed_rs::okx::adapter::OkxAdapter::parse_messages_for_feed(
        &feed,
        &message,
        1710000001.5,
    );
    assert_eq!(events.len(), 1);
    match &events[0] {
        cryptofeed_rs::okx::adapter::OkxEvent::Liquidation(liquidation) => {
            assert_eq!(liquidation.symbol.as_str(), "BTC-USDT");
            assert_eq!(
                liquidation.symbol.kind(),
                cryptofeed_core::symbol::InstrumentKind::Margin
            );
        }
        _ => panic!("expected liquidation"),
    }
}

#[test]
fn okx_option_ticker_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "tickers", "instId": "BTC-USD-250627-100000-C"},
        "data": [{ "bidPx": "0.001", "askPx": "0.002", "ts": "1710000000456" }]
    });

    let ticker = okx_parser::parse_ticker(&message, 1710000001.5).expect("option ticker");

    assert_eq!(ticker.symbol.as_str(), "BTC-USD-250627-100000-C");
    assert_eq!(ticker.bid, Decimal::from_str_exact("0.001").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("0.002").unwrap());
}

#[test]
fn bybit_l1_book_matches_public_baseline() {
    // Inline payload mirrors the `orderbook.1` capture in
    // `sample_data/bybit.ws.v5` (same `ts`, prices, and sizes).
    let message = json!({
        "topic": "orderbook.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304484951i64,
        "data": {
            "s": "BTCUSDT",
            "b": [["16493.50", "0.006"]],
            "a": [["16493.60", "0.100"]],
            "u": 18521287u64,
            "seq": 7961638723u64
        }
    });

    let l1 = bybit_parser::parse_l1_book(&message, 1672304487.0).expect("l1 book");

    assert_eq!(l1.symbol.as_str(), "BTC-USDT");
    assert_eq!(l1.bid.price, Decimal::from_str_exact("16493.50").unwrap());
    assert_eq!(l1.bid.amount, Decimal::from_str_exact("0.006").unwrap());
    assert_eq!(l1.ask.price, Decimal::from_str_exact("16493.60").unwrap());
    assert_eq!(l1.ask.amount, Decimal::from_str_exact("0.100").unwrap());
    assert_eq!(l1.exchange_ts, 1672304484.951);
    assert_eq!(l1.received_ts, 1672304487.0);
}

#[test]
fn bybit_liquidation_matches_public_baseline() {
    let message = json!({
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304487040i64,
        "data": [{
            "T": 1672304487035i64,
            "s": "BTCUSDT",
            "S": "Sell",
            "v": "0.001",
            "p": "49500"
        }]
    });

    let liquidation = bybit_parser::parse_liquidation(&message, 1672304487.1).expect("liquidation");

    assert_eq!(liquidation.symbol.as_str(), "BTC-USDT");
    assert_eq!(liquidation.side, cryptofeed_trade::model::Side::Buy);
    assert_eq!(
        liquidation.quantity,
        Decimal::from_str_exact("0.001").unwrap()
    );
    assert_eq!(liquidation.price, Decimal::from_str_exact("49500").unwrap());
    assert_eq!(liquidation.id, None);
    assert_eq!(
        liquidation.status,
        cryptofeed_liquidations::LiquidationStatus::Filled
    );
    assert_eq!(liquidation.exchange_ts, 1672304487.035);
    assert_eq!(liquidation.received_ts, 1672304487.1);
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
fn okx_v5_trade_batch_requires_every_entry_in_exchange_order() {
    let message = json!({
        "arg": {"channel": "trades", "instId": "ETH-BTC"},
        "data": [
            {"tradeId": "1", "px": "0.03410", "sz": "1.2500", "side": "buy", "ts": "1710000000123"},
            {"tradeId": "2", "px": "0.03420", "sz": "0.7500", "side": "sell", "ts": "1710000000124"}
        ]
    });

    let trades = okx_parser::parse_trades(&message, 1710000001.5);

    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].id.as_deref(), Some("1"));
    assert_eq!(trades[1].id.as_deref(), Some("2"));
}

#[test]
fn okx_swap_product_identity_is_preserved() {
    let message = json!({
        "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
        "data": [{"tradeId": "1", "px": "65000.50", "sz": "1", "side": "buy", "ts": "1710000000123"}]
    });

    let trade = okx_parser::parse_trade(&message, 1710000001.5).expect("swap trade");

    assert_eq!(trade.symbol.as_str(), "BTC-USDT-PERP");
}

#[test]
fn okx_funding_rate_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "instId": "BTC-USDT-SWAP",
            "fundingRate": "0.0001",
            "fundingTime": "1710000000123",
            "nextFundingRate": "0.0002",
            "nextFundingTime": "1710003600123"
        }]
    });

    let funding = okx_parser::parse_funding(&message, 1710000001.5).expect("funding");

    assert_eq!(funding.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(
        funding.rate,
        Some(Decimal::from_str_exact("0.0001").unwrap())
    );
    assert_eq!(funding.mark_price, None);
    assert_eq!(funding.next_funding_time, Some(1710003600.123));
    assert_eq!(
        funding.predicted_rate,
        Some(Decimal::from_str_exact("0.0002").unwrap())
    );
    assert_eq!(funding.exchange_ts, 1710000000.123);
    assert_eq!(funding.received_ts, 1710000001.5);
}

#[test]
fn okx_open_interest_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "instType": "SWAP",
            "instId": "BTC-USDT-SWAP",
            "oi": "12345.6",
            "oiCcy": "123.4567890123456789",
            "oiUsd": "800000000",
            "ts": "1710000000123"
        }]
    });

    let oi = okx_parser::parse_open_interest(&message, 1710000001.5).expect("open interest");

    assert_eq!(oi.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(
        oi.open_interest,
        Decimal::from_str_exact("12345.6").unwrap()
    );
    assert_eq!(
        oi.coin_quantity,
        Some(Decimal::from_str_exact("123.4567890123456789").unwrap())
    );
    assert_eq!(
        oi.value_usd,
        Some(Decimal::from_str_exact("800000000").unwrap())
    );
    assert_eq!(oi.exchange_ts, 1710000000.123);
    assert_eq!(oi.received_ts, 1710000001.5);
}

#[test]
fn okx_index_ticker_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "index-tickers", "instId": "BTC-USD"},
        "data": [{
            "instId": "BTC-USD",
            "idxPx": "65000.5",
            "open24h": "64000",
            "high24h": "65500",
            "low24h": "63500",
            "ts": "1710000000123"
        }]
    });

    let index = okx_parser::parse_index_price(&message, 1710000001.5).expect("index price");

    assert_eq!(index.symbol.as_str(), "BTC-USD");
    assert_eq!(index.price, Decimal::from_str_exact("65000.5").unwrap());
    assert_eq!(
        index.open_24h,
        Some(Decimal::from_str_exact("64000").unwrap())
    );
    assert_eq!(
        index.high_24h,
        Some(Decimal::from_str_exact("65500").unwrap())
    );
    assert_eq!(
        index.low_24h,
        Some(Decimal::from_str_exact("63500").unwrap())
    );
    assert_eq!(index.exchange_ts, 1710000000.123);
    assert_eq!(index.received_ts, 1710000001.5);
}

#[test]
fn okx_l1_book_matches_public_baseline() {
    let message = json!({
        "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"},
        "data": [{
            "bids": [["64999.10", "1.25", "0", "1"]],
            "asks": [["65000.20", "0.75", "0", "1"]],
            "ts": "1710000000456"
        }]
    });

    let l1 = okx_parser::parse_l1_book(&message, 1710000001.5).expect("l1 book");

    assert_eq!(l1.symbol.as_str(), "BTC-USDT");
    assert_eq!(l1.bid.price, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(l1.bid.amount, Decimal::from_str_exact("1.25").unwrap());
    assert_eq!(l1.ask.price, Decimal::from_str_exact("65000.20").unwrap());
    assert_eq!(l1.ask.amount, Decimal::from_str_exact("0.75").unwrap());
    assert_eq!(l1.exchange_ts, 1710000000.456);
    assert_eq!(l1.received_ts, 1710000001.5);
}

#[test]
fn okx_liquidation_orders_match_public_baseline() {
    let message = json!({
        "arg": {"channel": "liquidation-orders", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "instId": "BTC-USDT-SWAP",
            "px": "64000",
            "sz": "1.5",
            "side": "sell",
            "ts": "1710000000123",
            "ordId": "123456"
        }]
    });

    let liquidation = okx_parser::parse_liquidation(&message, 1710000001.5).expect("liquidation");

    assert_eq!(liquidation.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(liquidation.side, cryptofeed_trade::model::Side::Sell);
    assert_eq!(
        liquidation.quantity,
        Decimal::from_str_exact("1.5").unwrap()
    );
    assert_eq!(liquidation.price, Decimal::from_str_exact("64000").unwrap());
    assert_eq!(liquidation.id.as_deref(), Some("123456"));
    assert_eq!(
        liquidation.status,
        cryptofeed_liquidations::LiquidationStatus::Filled
    );
    assert_eq!(liquidation.exchange_ts, 1710000000.123);
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

/// An hourly OKX bar must carry the normalized `1h` interval and an `end`
/// one hour after `start` (previously hard-coded to start + 60s).
#[test]
fn okx_hourly_candle_derives_end_and_normalizes_interval() {
    let message = json!({
        "arg": {"channel": "candle1H", "instId": "BTC-USDT"},
        "data": [["1710003600000", "65000.00", "65100.00", "64900.00", "65050.00", "12.50", "0", "0", "1"]]
    });

    let candle = okx_parser::parse_candle(&message, 1710003661.0).expect("candle");

    assert_eq!(candle.interval, "1h");
    assert_eq!(candle.start, 1710003600.0);
    assert_eq!(candle.end, 1710007200.0);
}

#[test]
fn gateio_ticker_matches_public_baseline() {
    let message = json!({
        "channel": "spot.book_ticker",
        "event": "update",
        "result": {"s": "ETH_BTC", "b": "0.03400", "a": "0.03410", "t": 1606293275123i64}
    });

    let ticker = gateio_parser::parse_ticker(&message, 1606293276.0).expect("ticker");

    assert_eq!(ticker.symbol.as_str(), "ETH-BTC");
    assert_eq!(ticker.bid, Decimal::from_str_exact("0.03400").unwrap());
    assert_eq!(ticker.ask, Decimal::from_str_exact("0.03410").unwrap());
    assert_eq!(ticker.exchange_ts, 1606293275.123);
}

#[test]
fn gateio_trade_matches_public_baseline() {
    let message = json!({
        "channel": "spot.trades",
        "event": "update",
        "result": {
            "id": 309143071,
            "id_market": 2390902,
            "create_time": 1606292218,
            "create_time_ms": "1606292218213.4578",
            "side": "sell",
            "currency_pair": "GT_USDT",
            "amount": "16.4700000000",
            "price": "0.4705000000",
            "range": "2390902-2390902"
        }
    });

    let trade = gateio_parser::parse_trade(&message, 1606292219.0).expect("official v4 trade");

    assert_eq!(trade.symbol.as_str(), "GT-USDT");
    assert_eq!(
        trade.price,
        Decimal::from_str_exact("0.4705000000").unwrap()
    );
    assert_eq!(
        trade.amount,
        Decimal::from_str_exact("16.4700000000").unwrap()
    );
    assert_eq!(trade.exchange_ts, 1606292218.2134578);
    assert_eq!(trade.id.as_deref(), Some("309143071"));
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
            "t": 1606294781123i64,
            "U": 48776301u64,
            "u": 48776306u64
        }
    });

    let update = gateio_parser::parse_l2_book_update(&message, 1606294782.0).expect("book");

    let delta = match update {
        cryptofeed_rs::gateio::book_sync::GateioBookUpdate::Delta(delta) => delta,
        _ => panic!("expected delta update"),
    };
    assert_eq!(delta.first_update_id, 48776301);
    assert_eq!(delta.last_update_id, 48776306);
    let book = L2Book::Delta(delta.book);

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
fn gateio_rest_book_bootstrap_is_a_snapshot() {
    let message = json!({
        "id": 100u64,
        "current": 1710000001.5,
        "bids": [["64999.10", "1.25"]],
        "asks": [["65000.20", "0.75"]]
    });

    let snapshot = gateio_parser::parse_l2_book_snapshot(&message, "BTC_USDT", 1710000001.6)
        .expect("REST snapshot");

    assert_eq!(snapshot.last_update_id, Some(100));
    assert_eq!(snapshot.book.symbol.as_str(), "BTC-USDT");
    assert_eq!(
        snapshot.book.bids[0].amount,
        Decimal::from_str_exact("1.25").unwrap()
    );
}

#[test]
fn gateio_usdt_and_btc_perpetual_l2_preserve_resolved_product_identity() {
    let usdt_snapshot = json!({
        "id": 200u64,
        "current": 1710000001.6,
        "update": 1710000001.55,
        "bids": [{"p": "64999.10", "s": "2"}],
        "asks": [{"p": "65000.20", "s": "3"}]
    });
    let usdt_instrument = GateioInstrument::new(
        Symbol::perpetual("BTC", "USDT"),
        "BTC_USDT",
        GateioProduct::UsdtPerpetual,
    );
    let snapshot = gateio_parser::parse_l2_book_snapshot_for_instrument(
        &usdt_snapshot,
        &usdt_instrument,
        1710000001.65,
    )
    .expect("USDT perpetual snapshot");

    assert_eq!(snapshot.last_update_id, Some(200));
    assert_eq!(snapshot.book.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(snapshot.book.bids[0].amount, Decimal::from(2));

    let btc_update = json!({
        "channel": "futures.order_book_update",
        "event": "update",
        "result": {
            "t": 1615366381517i64,
            "s": "BTC_USD",
            "U": 251u64,
            "u": 252u64,
            "b": [{"p": "64997.50", "s": 6}],
            "a": [{"p": "65001.50", "s": 7}]
        }
    });
    let btc_instrument = GateioInstrument::new(
        Symbol::perpetual("BTC", "USD"),
        "BTC_USD",
        GateioProduct::BtcPerpetual,
    );
    let update = gateio_parser::parse_l2_book_update_for_instrument(
        &btc_update,
        1615366382.0,
        &btc_instrument,
    )
    .expect("BTC perpetual update");

    let delta = match update {
        cryptofeed_rs::gateio::book_sync::GateioBookUpdate::Delta(delta) => delta,
        _ => panic!("expected delta update"),
    };
    assert_eq!(delta.first_update_id, 251);
    assert_eq!(delta.last_update_id, 252);
    assert_eq!(delta.book.symbol.as_str(), "BTC-USD-PERP");
}

#[test]
fn gateio_usdt_delivery_snapshot_and_update_preserve_expiry() {
    let instrument = GateioInstrument::new(
        Symbol::futures("BTC", "USDT", "20260925"),
        "BTC_USDT_20260925",
        GateioProduct::UsdtDelivery,
    );
    let snapshot_message = json!({
        "id": 300u64,
        "current": 1710000001.8,
        "update": 1710000001.75,
        "bids": [{"p": "65010.00", "s": "6"}],
        "asks": [{"p": "65020.00", "s": "7"}]
    });
    let snapshot = gateio_parser::parse_l2_book_snapshot_for_instrument(
        &snapshot_message,
        &instrument,
        1710000001.85,
    )
    .expect("delivery snapshot");

    assert_eq!(snapshot.last_update_id, Some(300));
    assert_eq!(snapshot.book.symbol.as_str(), "BTC-USDT-20260925");

    let update_message = json!({
        "channel": "futures.order_book_update",
        "event": "update",
        "result": {
            "t": 1615366381617i64,
            "s": "BTC_USDT_20260925",
            "U": 301u64,
            "u": 302u64,
            "b": [{"p": "65009.50", "s": 8}],
            "a": [{"p": "65020.50", "s": 9}]
        }
    });
    let update = gateio_parser::parse_l2_book_update_for_instrument(
        &update_message,
        1615366382.0,
        &instrument,
    )
    .expect("delivery update");

    let delta = match update {
        cryptofeed_rs::gateio::book_sync::GateioBookUpdate::Delta(delta) => delta,
        _ => panic!("expected delta update"),
    };
    assert_eq!(delta.first_update_id, 301);
    assert_eq!(delta.last_update_id, 302);
    assert_eq!(delta.book.symbol.as_str(), "BTC-USDT-20260925");
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

#[test]
fn gateio_derivative_tickers_carry_funding_oi_and_index() {
    // Sanitized from the official Gate.io v4 futures WebSocket tickers
    // contract (`futures.tickers` pushes `funding_rate`, `mark_price`,
    // `index_price`, `total_size`, `time`).
    let message = json!({
        "channel": "futures.tickers",
        "event": "update",
        "result": [{
            "contract": "BTC_USDT",
            "last": "65102.1",
            "change_percentage": "0.21",
            "total_size": "2334747.0",
            "low_24h": "63321.3",
            "high_24h": "65557.9",
            "mark_price": "65103.4",
            "funding_rate": "0.000061",
            "funding_rate_indicative": "0.000061",
            "index_price": "65103.1",
            "last_size": "1977",
            "volume_24h": "45478740629.5",
            "time": 1710000000000u64
        }]
    });

    let funding = gateio_parser::parse_funding(&message, 1710000001.0).expect("funding");
    assert_eq!(funding.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(
        funding.rate,
        Some(Decimal::from_str_exact("0.000061").unwrap())
    );
    assert_eq!(
        funding.mark_price,
        Some(Decimal::from_str_exact("65103.4").unwrap())
    );
    assert_eq!(funding.next_funding_time, None);
    assert_eq!(funding.exchange_ts, 1710000000.0);

    let oi = gateio_parser::parse_open_interest(&message, 1710000001.0).expect("open interest");
    assert_eq!(oi.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(
        oi.open_interest,
        Decimal::from_str_exact("2334747.0").unwrap()
    );
    assert_eq!(oi.value_usd, None);

    let index = gateio_parser::parse_index_price(&message, 1710000001.0).expect("index");
    assert_eq!(index.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(index.price, Decimal::from_str_exact("65103.1").unwrap());
}

#[test]
fn gateio_book_ticker_doubles_as_l1_book() {
    // Sanitized from the official Gate.io v4 book ticker contract: `b`/`a`
    // are the best bid/ask prices and `B`/`A` the sizes (spot and futures
    // share the shape).
    let message = json!({
        "channel": "futures.book_ticker",
        "event": "update",
        "result": {
            "t": 1710000000123u64,
            "u": 48733182u64,
            "s": "BTC_USDT",
            "b": "64999.10",
            "B": "1.25",
            "a": "65000.20",
            "A": "0.75"
        }
    });

    let book = gateio_parser::parse_l1_book(&message, 1710000001.5).expect("l1 book");

    assert_eq!(book.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(book.bid.price, Decimal::from_str_exact("64999.10").unwrap());
    assert_eq!(book.bid.amount, Decimal::from_str_exact("1.25").unwrap());
    assert_eq!(book.ask.price, Decimal::from_str_exact("65000.20").unwrap());
    assert_eq!(book.ask.amount, Decimal::from_str_exact("0.75").unwrap());
    assert_eq!(book.exchange_ts, 1710000000.123);
}

#[test]
fn gateio_derivative_ticker_carries_mark_price() {
    let message = json!({
        "channel": "futures.tickers",
        "event": "update",
        "result": [{
            "contract": "BTC_USDT",
            "last": "65102.1",
            "mark_price": "65103.4",
            "funding_rate": "0.000061",
            "time": 1710000000000u64
        }]
    });

    let mark = gateio_parser::parse_mark_price(&message, 1710000001.0).expect("mark price");

    assert_eq!(mark.symbol.as_str(), "BTC-USDT-PERP");
    assert_eq!(mark.price, Decimal::from_str_exact("65103.4").unwrap());
    assert_eq!(mark.next_funding_time, None);
    assert_eq!(mark.predicted_rate, None);
    assert_eq!(mark.exchange_ts, 1710000000.0);
}

#[test]
fn gateio_derivative_tickers_resolve_delivery_instrument() {
    let message = json!({
        "channel": "futures.tickers",
        "event": "update",
        "result": [{
            "contract": "BTC_USDT_20260925",
            "last": "65000",
            "funding_rate": "0",
            "total_size": "100",
            "index_price": "65001.2",
            "time": 1710000000000u64
        }]
    });
    let instrument = GateioInstrument::new(
        Symbol::futures("BTC", "USDT", "20260925"),
        "BTC_USDT_20260925",
        GateioProduct::UsdtDelivery,
    );

    let oi =
        gateio_parser::parse_open_interests_for_instrument(&message, 1710000001.0, &instrument);
    assert_eq!(oi.len(), 1);
    assert_eq!(oi[0].symbol.as_str(), "BTC-USDT-20260925");

    let index =
        gateio_parser::parse_index_prices_for_instrument(&message, 1710000001.0, &instrument);
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].symbol.as_str(), "BTC-USDT-20260925");

    let fundings =
        gateio_parser::parse_fundings_for_instrument(&message, 1710000001.0, &instrument);
    assert_eq!(fundings.len(), 1);
    assert_eq!(fundings[0].symbol.as_str(), "BTC-USDT-20260925");
}

#[test]
fn bitget_l1_spot_and_derivatives_preserve_top_of_book() {
    use cryptofeed_rs::bitget::adapter::BitgetEvent;
    for (inst_type, native, symbol) in [
        ("spot", "BTCUSDT", Symbol::spot("BTC", "USDT")),
        ("usdt-futures", "BTCUSDT", Symbol::perpetual("BTC", "USDT")),
        ("usdc-futures", "BTCUSDC", Symbol::perpetual("BTC", "USDC")),
        ("coin-futures", "BTCUSD", Symbol::perpetual("BTC", "USD")),
        (
            "usdt-futures",
            "BTCUSDT261225",
            Symbol::futures("BTC", "USDT", "261225"),
        ),
    ] {
        let feed = cryptofeed_rs::bitget::Bitget::new()
            .l1_book()
            .instrument(symbol.clone())
            .exchange_symbol(native)
            .build();
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_ok());
        let subscribe: serde_json::Value =
            serde_json::from_str(&BitgetAdapter::subscription_message(&feed)).unwrap();
        assert_eq!(subscribe["args"][0]["topic"], "books1");
        let message = json!({"arg":{"instType":inst_type,"topic":"books1","symbol":native},"action":"snapshot",
            "data":[{"b":[["99756.60000001","0.01280001"]],"a":[["99756.70000001","23.97740001"]],"seq":7,"ts":"1746698732562"}],"ts":1746698732563i64});
        let events = BitgetAdapter::parse_messages_for_feed(&feed, &message, 1746698732.6);
        let book = events
            .into_iter()
            .find_map(|event| match event {
                BitgetEvent::L1Book(book) => Some(book),
                _ => None,
            })
            .unwrap();
        assert_eq!(book.symbol, symbol);
        assert_eq!(
            book.bid.price,
            Decimal::from_str_exact("99756.60000001").unwrap()
        );
        assert_eq!(
            book.bid.amount,
            Decimal::from_str_exact("0.01280001").unwrap()
        );
        assert_eq!(
            book.ask.amount,
            Decimal::from_str_exact("23.97740001").unwrap()
        );
        assert_eq!(book.exchange_ts, 1746698732.562);
        assert_eq!(book.received_ts, 1746698732.6);
    }
}

#[test]
fn bitget_derivative_ticker_normalizes_all_fields_and_deduplicates_subscription() {
    use cryptofeed_rs::bitget::adapter::BitgetEvent;
    for (inst_type, native, symbol) in [
        ("usdt-futures", "BTCUSDT", Symbol::perpetual("BTC", "USDT")),
        ("usdc-futures", "BTCUSDC", Symbol::perpetual("BTC", "USDC")),
        ("coin-futures", "BTCUSD", Symbol::perpetual("BTC", "USD")),
        (
            "usdt-futures",
            "BTCUSDT261225",
            Symbol::futures("BTC", "USDT", "261225"),
        ),
    ] {
        let feed = cryptofeed_rs::bitget::Bitget::new()
            .ticker()
            .open_interest()
            .index()
            .mark_price()
            .instrument(symbol.clone())
            .exchange_symbol(native)
            .build();
        let mut feed = feed;
        if symbol.kind() == cryptofeed_core::symbol::InstrumentKind::Perpetual {
            feed.channels
                .push(cryptofeed_core::exchange::Channel::Funding);
        }
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_ok());
        let subscribe: serde_json::Value =
            serde_json::from_str(&BitgetAdapter::subscription_message(&feed)).unwrap();
        assert_eq!(subscribe["args"].as_array().unwrap().len(), 1);
        assert_eq!(subscribe["args"][0]["topic"], "ticker");
        let mut row = json!({"bid1Price":"99999","ask1Price":"100000","markPrice":"99999.12345678","indexPrice":"99998.12345678",
            "openInterest":"123.45678901","fundingRate":"-0.00012345","nextFundingTime":"1736373600000"});
        if symbol.kind() == cryptofeed_core::symbol::InstrumentKind::Futures {
            row.as_object_mut().unwrap().remove("fundingRate");
            row.as_object_mut().unwrap().remove("nextFundingTime");
        }
        let message = json!({"arg":{"instType":inst_type,"topic":"ticker","symbol":native},"action":"snapshot","data":[row.clone(),row],"ts":1736371332162i64});
        let events = BitgetAdapter::parse_messages_for_feed(&feed, &message, 1736371332.2);
        assert_eq!(
            events.len(),
            if symbol.kind() == cryptofeed_core::symbol::InstrumentKind::Futures {
                8
            } else {
                10
            }
        );
        for event in events {
            match event {
                BitgetEvent::Funding(value) => {
                    assert_eq!(value.symbol, symbol);
                    assert_eq!(
                        value.rate,
                        Some(Decimal::from_str_exact("-0.00012345").unwrap())
                    );
                    assert_eq!(
                        value.mark_price,
                        Some(Decimal::from_str_exact("99999.12345678").unwrap())
                    );
                    assert_eq!(value.next_funding_time, Some(1736373600.0));
                    assert_eq!(value.exchange_ts, 1736371332.162);
                    assert_eq!(value.received_ts, 1736371332.2);
                }
                BitgetEvent::OpenInterest(value) => {
                    assert_eq!(value.symbol, symbol);
                    assert_eq!(
                        value.open_interest,
                        Decimal::from_str_exact("123.45678901").unwrap()
                    );
                    assert_eq!(value.coin_quantity, None);
                    assert_eq!(value.value_usd, None);
                }
                BitgetEvent::IndexPrice(value) => {
                    assert_eq!(value.symbol, symbol);
                    assert_eq!(
                        value.price,
                        Decimal::from_str_exact("99998.12345678").unwrap()
                    );
                }
                BitgetEvent::MarkPrice(value) => {
                    assert_eq!(value.symbol, symbol);
                    assert_eq!(
                        value.price,
                        Decimal::from_str_exact("99999.12345678").unwrap()
                    );
                    assert_eq!(
                        value.next_funding_time,
                        if symbol.kind() == cryptofeed_core::symbol::InstrumentKind::Futures {
                            None
                        } else {
                            Some(1736373600.0)
                        }
                    );
                }
                BitgetEvent::Ticker(value) => assert_eq!(value.symbol, symbol),
                _ => panic!("unexpected event"),
            }
        }
    }
}

#[test]
fn bitget_derivative_fields_reject_spot_and_do_not_invent_missing_values() {
    for inst_type in ["spot", "usdt-futures"] {
        let message = json!({"arg":{"instType":inst_type,"topic":"ticker","symbol":"BTCUSDT"},"data":[{}],"ts":1736371332162i64});
        assert!(bitget_parser::parse_fundings(&message, 1.0).is_empty());
        assert!(bitget_parser::parse_open_interests(&message, 1.0).is_empty());
        assert!(bitget_parser::parse_index_prices(&message, 1.0).is_empty());
        assert!(bitget_parser::parse_mark_prices(&message, 1.0).is_empty());
    }
    let message = json!({"arg":{"instType":"spot","topic":"ticker","symbol":"BTCUSDT"},"data":[{"fundingRate":"0","openInterest":"1","indexPrice":"2","markPrice":"3"}]});
    assert!(bitget_parser::parse_fundings(&message, 1.0).is_empty());
    assert!(bitget_parser::parse_open_interests(&message, 1.0).is_empty());
    assert!(bitget_parser::parse_index_prices(&message, 1.0).is_empty());
    assert!(bitget_parser::parse_mark_prices(&message, 1.0).is_empty());
    for channel in [
        cryptofeed_core::exchange::Channel::Funding,
        cryptofeed_core::exchange::Channel::OpenInterest,
        cryptofeed_core::exchange::Channel::Index,
        cryptofeed_core::exchange::Channel::MarkPrice,
    ] {
        let mut feed = cryptofeed_rs::bitget::Bitget::new()
            .ticker()
            .symbol("BTC-USDT")
            .build();
        feed.channels = vec![channel];
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_err());
    }
}

#[test]
fn gateio_public_liquidations_preserve_sign_precision_and_product_scope() {
    use cryptofeed_core::model::Side;
    use cryptofeed_rs::gateio::adapter::{GateioAdapter, GateioEvent};
    let message = json!({"channel":"futures.public_liquidates","event":"update","time_ms":1541505434123i64,
        "result":[{"price":215.1,"size":"-124.5","time_ms":1541486601123i64,"contract":"BTC_USD"},
                  {"price":"3000.12345678","size":"0.01234567","time_ms":1541486601124i64,"contract":"ETH_USDT"}]});
    let events = GateioAdapter::parse_messages(&message, 1541486601.2);
    assert_eq!(events.len(), 2);
    let GateioEvent::Liquidation(first) = &events[0] else {
        panic!("liquidation");
    };
    assert_eq!(first.symbol.as_str(), "BTC-USD-PERP");
    assert_eq!(first.side, Side::Sell);
    assert_eq!(first.quantity, Decimal::from_str_exact("124.5").unwrap());
    assert_eq!(first.price, Decimal::from_str_exact("215.1").unwrap());
    assert_eq!(first.exchange_ts, 1541486601.123);
    assert_eq!(first.received_ts, 1541486601.2);
    assert_eq!(first.id, None);
    let GateioEvent::Liquidation(second) = &events[1] else {
        panic!("liquidation");
    };
    assert_eq!(second.side, Side::Buy);
    assert_eq!(
        second.quantity,
        Decimal::from_str_exact("0.01234567").unwrap()
    );
    assert_eq!(
        second.price,
        Decimal::from_str_exact("3000.12345678").unwrap()
    );
    for symbol in [
        Symbol::perpetual("BTC", "USDT"),
        Symbol::perpetual("BTC", "USD"),
    ] {
        let feed = cryptofeed_rs::gateio::Gateio::new()
            .liquidations()
            .instrument(symbol)
            .build();
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_ok());
        let plans = GateioAdapter::connection_plans(&feed).unwrap();
        let subscribe: serde_json::Value =
            serde_json::from_str(&plans[0].subscription_messages[0]).unwrap();
        assert_eq!(subscribe["channel"], "futures.public_liquidates");
        assert!(subscribe.get("auth").is_none());
    }
    for symbol in [
        Symbol::spot("BTC", "USDT"),
        Symbol::futures("BTC", "USDT", "261225"),
    ] {
        let feed = cryptofeed_rs::gateio::Gateio::new()
            .liquidations()
            .instrument(symbol)
            .build();
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_err());
        assert!(GateioAdapter::connection_plans(&feed).is_err());
    }
    let ack = json!({"time":1791444122i64,"time_ms":1791444122760i64,"channel":"futures.public_liquidates","event":"subscribe","payload":["BTC_USDT"],"result":{"status":"success"}});
    assert!(GateioAdapter::parse_messages(&ack, 1.0).is_empty());
}

#[test]
fn okx_contract_indexes_use_index_ids_and_fan_out_to_matching_contracts() {
    use cryptofeed_rs::okx::adapter::{OkxAdapter, OkxEvent};
    for symbols in [
        vec![
            Symbol::perpetual("BTC", "USDT"),
            Symbol::perpetual("ETH", "USDT"),
        ],
        vec![
            Symbol::futures("BTC", "USD", "261225"),
            Symbol::futures("BTC", "USD", "270326"),
        ],
    ] {
        let mut builder = cryptofeed_rs::okx::Okx::new().index().mark_price();
        for symbol in &symbols {
            builder = builder.instrument(symbol.clone());
        }
        let feed = builder.build();
        assert!(cryptofeed_rs::markets::validate_feed(&feed).is_ok());
        let subscribe: serde_json::Value =
            serde_json::from_str(&OkxAdapter::subscription_message(&feed)).unwrap();
        let args = subscribe["args"].as_array().unwrap();
        let index_id = if symbols[0].kind() == cryptofeed_core::symbol::InstrumentKind::Perpetual {
            "BTC-USDT"
        } else {
            "BTC-USD"
        };
        assert_eq!(
            args.iter()
                .filter(|arg| arg["channel"] == "index-tickers" && arg["instId"] == index_id)
                .count(),
            1
        );
        assert!(
            args.iter()
                .filter(|arg| arg["channel"] == "index-tickers")
                .all(|arg| arg["instId"].as_str().unwrap().split('-').count() == 2)
        );
        let message = json!({"arg":{"channel":"index-tickers","instId":index_id},"data":[{"instId":index_id,"idxPx":"65000.12345678","high24h":"66000","low24h":"64000","open24h":"64500","ts":"1710000000123"}]});
        let events = OkxAdapter::parse_messages_for_feed(&feed, &message, 1710000000.2);
        let expected: Vec<_> = symbols
            .iter()
            .filter(|symbol| symbol.as_str().starts_with("BTC-"))
            .collect();
        assert_eq!(events.len(), expected.len());
        for (event, symbol) in events.iter().zip(expected) {
            let OkxEvent::IndexPrice(index) = event else {
                panic!("index");
            };
            assert_eq!(&index.symbol, symbol);
            assert_eq!(
                index.price,
                Decimal::from_str_exact("65000.12345678").unwrap()
            );
            assert_eq!(index.high_24h, Some(Decimal::from(66000)));
            assert_eq!(index.exchange_ts, 1710000000.123);
            assert_eq!(index.received_ts, 1710000000.2);
        }
        let unrelated = json!({"arg":{"channel":"index-tickers","instId":"SOL-USDT"},"data":[{"instId":"SOL-USDT","idxPx":"150","ts":"1710000000123"}]});
        assert!(OkxAdapter::parse_messages_for_feed(&feed, &unrelated, 1.0).is_empty());
    }
}

#[test]
fn binance_settlement_price_is_not_a_predicted_funding_rate() {
    let message = json!({"e":"markPriceUpdate","E":1562305380000i64,"s":"BTCUSDT",
        "p":"11794.15000000","i":"11784.62659091","P":"11784.25641265","r":"0.00038167","T":1562306400000i64});
    let funding = binance_parser::parse_funding(&message, 1562305381.0).unwrap();
    let mark = binance_parser::parse_mark_price(&message, 1562305381.0).unwrap();
    assert_eq!(funding.predicted_rate, None);
    assert_eq!(mark.predicted_rate, None);
    assert_eq!(
        funding.rate,
        Some(Decimal::from_str_exact("0.00038167").unwrap())
    );
    assert_eq!(
        mark.price,
        Decimal::from_str_exact("11794.15000000").unwrap()
    );
}

#[test]
fn okx_coin_quantity_is_not_a_currency_code() {
    let message = json!({"arg":{"channel":"open-interest","instId":"BTC-USDT-SWAP"},
        "data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","oi":"2216113.01000000309",
        "oiCcy":"22161.1301000000309","oiUsd":"1939251795.54769270396321","ts":"1743041250440"}]});
    let oi = okx_parser::parse_open_interest(&message, 1743041250.5).unwrap();
    assert_eq!(
        oi.coin_quantity,
        Some(Decimal::from_str_exact("22161.1301000000309").unwrap())
    );
}

#[test]
fn gateio_numeric_prices_preserve_wire_decimal_precision() {
    let message: serde_json::Value = serde_json::from_str(r#"{"channel":"futures.public_liquidates","event":"update","result":[{"contract":"BTC_USDT","price":65000.12345678901234567890,"size":-0.12345678901234567890,"time_ms":1736371332162}]}"#).unwrap();
    let values = gateio_parser::parse_liquidations_for_instrument(&message, 1736371332.2, None);
    assert_eq!(values.len(), 1);
    assert_eq!(
        values[0].price,
        Decimal::from_str_exact("65000.12345678901234567890").unwrap()
    );
    assert_eq!(
        values[0].quantity,
        Decimal::from_str_exact("0.12345678901234567890").unwrap()
    );
}

#[test]
fn bitget_feed_binding_is_product_qualified() {
    let feed = cryptofeed_rs::bitget::Bitget::new()
        .ticker()
        .symbol("BTC-USDT-PERP")
        .exchange_symbol("BTCUSDT")
        .build();
    let message = json!({"arg":{"instType":"spot","topic":"ticker","symbol":"BTCUSDT"},
        "data":[{"bid1Price":"99999","ask1Price":"100000"}],"ts":1736371332162i64});
    assert!(BitgetAdapter::parse_messages_for_feed(&feed, &message, 1736371332.2).is_empty());
    let mixed_quotes = cryptofeed_rs::bitget::Bitget::new()
        .liquidations()
        .symbol("BTC-USDT-PERP")
        .exchange_symbol("BTCUSDT")
        .symbol("BTC-USDC-PERP")
        .exchange_symbol("BTCUSDC")
        .build();
    let message = json!({"arg":{"instType":"usdc-futures","topic":"liquidation"},"data":[
        {"symbol":"BTCUSDT","side":"sell","price":"65000","amount":"65000","ts":"1736371332162"},
        {"symbol":"BTCUSDC","side":"sell","price":"65000","amount":"65000","ts":"1736371332162"}]});
    let events = BitgetAdapter::parse_messages_for_feed(&mixed_quotes, &message, 1736371332.2);
    assert_eq!(events.len(), 1);
    let cryptofeed_rs::bitget::adapter::BitgetEvent::Liquidation(value) = &events[0] else {
        panic!("liquidation");
    };
    assert_eq!(value.symbol.as_str(), "BTC-USDC-PERP");
}

#[test]
fn dated_futures_funding_is_rejected_before_connecting() {
    use cryptofeed_core::{error::Error, exchange::ExchangeId};
    for exchange in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ] {
        let dated = cryptofeed_rs::ExchangeFeedBuilder::new(exchange)
            .funding()
            .instrument(Symbol::futures("BTC", "USDT", "261225"))
            .build();
        assert!(matches!(
            cryptofeed_rs::markets::validate_feed(&dated),
            Err(Error::UnsupportedCapability(_))
        ));
        let perpetual = cryptofeed_rs::ExchangeFeedBuilder::new(exchange)
            .funding()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();
        assert!(cryptofeed_rs::markets::validate_feed(&perpetual).is_ok());
    }
}

#[test]
fn okx_monthly_candles_follow_calendar_boundaries() {
    for (interval, start, end) in [
        ("1M", 1706716800000i64, 1709222400000i64),
        ("1M", 1714492800000i64, 1717171200000i64),
        ("3M", 1704038400000i64, 1711900800000i64),
    ] {
        let message = json!({"arg":{"channel":format!("candle{interval}"),"instId":"BTC-USDT"},
            "data":[[start.to_string(),"100","101","99","100.5","1","1","100.5","1"]]});
        let candle = okx_parser::parse_candle(&message, end as f64 / 1000.0).unwrap();
        assert_eq!(candle.start, start as f64 / 1000.0);
        assert_eq!(candle.end, end as f64 / 1000.0);
    }
}

#[test]
fn bitget_candles_follow_current_official_interval_vocabulary() {
    for (normalized, wire) in [
        ("1h", "1H"),
        ("4h", "4H"),
        ("6h", "6H"),
        ("12h", "12H"),
        ("1d", "1D"),
    ] {
        assert_eq!(BitgetAdapter::candle_interval_wire(normalized), Some(wire));
    }
    for interval in ["3d", "1w", "1M"] {
        assert_eq!(BitgetAdapter::candle_interval_wire(interval), None);
    }
}

#[test]
fn binance_contract_index_uses_documented_mark_price_stream() {
    use cryptofeed_rs::binance::{Binance, adapter::BinanceEvent};
    for (symbol, native, product) in [
        (
            Symbol::perpetual("BTC", "USDT"),
            "BTCUSDT",
            BinanceProduct::UsdM,
        ),
        (
            Symbol::perpetual("BTC", "USD"),
            "BTCUSD_PERP",
            BinanceProduct::CoinM,
        ),
        (
            Symbol::futures("BTC", "USD", "261225"),
            "BTCUSD_261225",
            BinanceProduct::CoinM,
        ),
    ] {
        let feed = Binance::new()
            .index()
            .mark_price()
            .instrument(symbol.clone())
            .exchange_symbol(native)
            .build();
        let plans = BinanceAdapter::connection_plans(&feed).unwrap();
        let streams: Vec<_> = plans.iter().flat_map(|plan| plan.streams.iter()).collect();
        assert_eq!(streams.len(), 1);
        assert_eq!(
            streams[0],
            &format!("{}@markPrice@1s", native.to_ascii_lowercase())
        );
        let message = json!({"e":"markPriceUpdate","E":1562305380000i64,"s":native,"p":"11794.15000000",
            "i":"11784.62659091","P":"11784.25641265","r":"0.00038167","T":1562306400000i64});
        let instrument = BinanceInstrument::new(symbol.clone(), native, product);
        let events =
            BinanceAdapter::parse_messages_for_instrument(&message, 1562305381.0, &instrument);
        let index = events
            .into_iter()
            .find_map(|event| match event {
                BinanceEvent::IndexPrice(value) => Some(value),
                _ => None,
            })
            .unwrap();
        assert_eq!(index.symbol, symbol);
        assert_eq!(
            index.price,
            Decimal::from_str_exact("11784.62659091").unwrap()
        );
        assert_eq!(index.exchange_ts, 1562305380.0);
    }
}

#[test]
fn okx_uses_current_tls_endpoints_for_public_and_business_channels() {
    use cryptofeed_rs::exchange::{okx::Okx, okx::adapter::OkxAdapter};
    let feed = Okx::new().ticker().candles().symbol("BTC-USDT").build();
    assert_eq!(
        OkxAdapter::subscription_urls(&feed),
        vec![
            "wss://ws.okx.com/ws/v5/public".to_owned(),
            "wss://ws.okx.com/ws/v5/business".to_owned(),
        ]
    );
}

#[test]
fn bybit_spot_subscription_batches_respect_per_request_arg_limit() {
    use cryptofeed_rs::{
        bybit::{Bybit, adapter::BybitAdapter},
        exchange::ExchangeFeedBuilder,
    };
    let mut builder: ExchangeFeedBuilder = Bybit::new().trade();
    for i in 0..11 {
        builder = builder.symbol(&format!("S{i}-USDT"));
    }
    let messages = BybitAdapter::subscription_messages(&builder.build()).unwrap();
    let args: Vec<Vec<String>> = messages
        .iter()
        .map(|message| {
            let payload: serde_json::Value = serde_json::from_str(message).unwrap();
            assert_eq!(payload["op"], "subscribe");
            payload["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect()
        })
        .collect();
    assert_eq!(args.iter().map(Vec::len).collect::<Vec<_>>(), [10, 1]);
    assert_eq!(
        args.into_iter().flatten().collect::<Vec<_>>(),
        (0..11)
            .map(|i| format!("publicTrade.S{i}USDT"))
            .collect::<Vec<_>>()
    );
}
