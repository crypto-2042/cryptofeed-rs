//! HTX (Huobi) spot and USDT-M WebSocket payload parsing.
//!
//! Wire facts come from the official spot v1 / USDT-M swap v1 documentation and
//! the live framing verification recorded in `docs/htx-protocol.md`. Market
//! topics are `{"ch":"<topic>","ts":ms,"tick":..}`; notification topics are
//! `{"op":"notify","topic":"..","ts":ms,"data":[..]}`. Every frame is
//! GZIP-compressed binary, decompressed by the connection layer before this
//! parser sees the JSON text.

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(any(feature = "trade", feature = "liquidations"))]
use cryptofeed_core::model::Side;
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::{Liquidation, LiquidationStatus};
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L1Book, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub(crate) fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => decimal_from_text(text),
        Value::Number(number) => decimal_from_text(&number.to_string()),
        _ => None,
    }
}

/// Accepts both plain decimals and the scientific notation that JSON numbers
/// use for very small prices (e.g. `5.213462e-06`); `from_str_exact` alone
/// rejects the exponent form.
fn decimal_from_text(text: &str) -> Option<Decimal> {
    Decimal::from_str_exact(text)
        .or_else(|_| Decimal::from_scientific(text))
        .ok()
}

/// Milliseconds-since-epoch number/string -> Unix seconds.
pub fn parse_millis(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().map(|millis| millis / 1000.0),
        Value::String(text) => text.parse::<f64>().ok().map(|millis| millis / 1000.0),
        _ => None,
    }
}

/// Market push channel (`ch`), e.g. `market.btcusdt.ticker`.
pub fn channel(message: &Value) -> Option<&str> {
    message.get("ch").and_then(Value::as_str)
}

/// Notification topic, e.g. `public.BTC-USDT.funding_rate`.
pub fn topic(message: &Value) -> Option<&str> {
    message.get("topic").and_then(Value::as_str)
}

/// Notification operator (`notify` for data pushes).
pub fn op(message: &Value) -> Option<&str> {
    message.get("op").and_then(Value::as_str)
}

/// Splits `market.<native>.<kind>[.<extra>]` into `(native, kind, extra)`.
pub fn topic_parts(channel: &str) -> Option<(&str, &str, Option<&str>)> {
    let mut parts = channel.split('.');
    if parts.next()? != "market" {
        return None;
    }
    let native = parts.next()?;
    let kind = parts.next()?;
    let extra = parts.next();
    if native.is_empty() || kind.is_empty() {
        return None;
    }
    Some((native, kind, extra))
}

/// Spot kline period -> normalized interval.
pub fn spot_interval(period: &str) -> Option<&'static str> {
    match period {
        "1min" => Some("1m"),
        "5min" => Some("5m"),
        "15min" => Some("15m"),
        "30min" => Some("30m"),
        "60min" => Some("1h"),
        "4hour" => Some("4h"),
        "1day" => Some("1d"),
        "1week" => Some("1w"),
        "1mon" => Some("1M"),
        _ => None,
    }
}

/// USDT-M kline period -> normalized interval.
pub fn swap_interval(period: &str) -> Option<&'static str> {
    match period {
        "1min" => Some("1m"),
        "5min" => Some("5m"),
        "15min" => Some("15m"),
        "30min" => Some("30m"),
        "60min" | "1hour" => Some("1h"),
        "4hour" => Some("4h"),
        "1day" => Some("1d"),
        "1mon" => Some("1M"),
        _ => None,
    }
}

/// Normalized candle interval -> seconds.
pub fn interval_seconds(interval: &str) -> Option<f64> {
    Some(match interval {
        "1m" => 60.0,
        "5m" => 300.0,
        "15m" => 900.0,
        "30m" => 1_800.0,
        "1h" => 3_600.0,
        "4h" => 14_400.0,
        "1d" => 86_400.0,
        "1w" => 604_800.0,
        "1M" => 2_592_000.0,
        _ => return None,
    })
}

#[cfg(feature = "ticker")]
pub fn parse_ticker(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        bid: parse_decimal(data.get("bid")?)?,
        ask: parse_decimal(data.get("ask")?)?,
        exchange_ts: parse_millis(data.get("ts")?).unwrap_or(received_ts),
        received_ts,
        implied_volatility: None,
    })
}

/// Swap `market.<contract>.detail` ticker: `bid`/`ask` are `[price, size]`
/// arrays and the close is the latest trade price.
#[cfg(feature = "ticker")]
pub fn parse_swap_detail_ticker(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<Ticker> {
    let bid = data.get("bid")?.as_array()?.first()?;
    let ask = data.get("ask")?.as_array()?.first()?;
    Some(Ticker {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        bid: parse_decimal(bid)?,
        ask: parse_decimal(ask)?,
        exchange_ts: data.get("ts").and_then(parse_millis).unwrap_or(received_ts),
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_spot_bbo(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<L1Book> {
    Some(L1Book {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        bid: PriceLevel {
            price: parse_decimal(data.get("bid")?)?,
            amount: parse_decimal(data.get("bidSize")?)?,
        },
        ask: PriceLevel {
            price: parse_decimal(data.get("ask")?)?,
            amount: parse_decimal(data.get("askSize")?)?,
        },
        exchange_ts: parse_millis(data.get("quoteTime")?).unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_swap_bbo(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<L1Book> {
    let bid = data.get("bid")?.as_array()?;
    let ask = data.get("ask")?.as_array()?;
    if bid.len() < 2 || ask.len() < 2 {
        return None;
    }
    Some(L1Book {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        bid: PriceLevel {
            price: parse_decimal(&bid[0])?,
            amount: parse_decimal(&bid[1])?,
        },
        ask: PriceLevel {
            price: parse_decimal(&ask[0])?,
            amount: parse_decimal(&ask[1])?,
        },
        exchange_ts: parse_millis(data.get("ts")?).unwrap_or(received_ts),
        received_ts,
    })
}

fn trade_id(row: &Value) -> Option<String> {
    match row.get("tradeId").or_else(|| row.get("trade-id")) {
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Number(number)) => Some(number.to_string()),
        _ => None,
    }
}

#[cfg(any(feature = "trade", feature = "liquidations"))]
fn trade_side(row: &Value) -> Option<Side> {
    Some(match row.get("direction")?.as_str()? {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        _ => return None,
    })
}

/// Spot `trade.detail` row.
#[cfg(feature = "trade")]
pub fn parse_spot_trade(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        side: trade_side(row)?,
        amount: parse_decimal(row.get("amount")?)?,
        price: parse_decimal(row.get("price")?)?,
        exchange_ts: row.get("ts").and_then(parse_millis).unwrap_or(received_ts),
        received_ts,
        id: trade_id(row),
        implied_volatility: None,
    })
}

/// USDT-M `trade.detail` row. `amount` is contracts; the documented base-coin
/// `quantity` is required so a contract count is never published as coin.
#[cfg(feature = "trade")]
pub fn parse_swap_trade(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        side: trade_side(row)?,
        amount: parse_decimal(row.get("quantity")?)?,
        price: parse_decimal(row.get("price")?)?,
        exchange_ts: row.get("ts").and_then(parse_millis).unwrap_or(received_ts),
        received_ts,
        id: match row.get("id") {
            Some(Value::String(text)) => Some(text.clone()),
            Some(Value::Number(number)) => Some(number.to_string()),
            _ => None,
        },
        implied_volatility: None,
    })
}

/// One parsed depth push.
#[cfg(feature = "orderbook")]
#[derive(Clone, Debug, PartialEq)]
pub struct BookUpdate {
    pub snapshot: bool,
    pub version: u64,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(feature = "orderbook")]
fn parse_levels(rows: Option<&Value>) -> Option<Vec<PriceLevel>> {
    let mut levels = Vec::new();
    for row in rows?.as_array()? {
        let parts = row.as_array()?;
        if parts.len() < 2 {
            return None;
        }
        levels.push(PriceLevel {
            price: parse_decimal(&parts[0])?,
            amount: parse_decimal(&parts[1])?,
        });
    }
    Some(levels)
}

/// Spot `depth.stepX` is a full replacement snapshot per push; `version` is the
/// documented monotonic dedupe anchor (not a delta sequence).
#[cfg(feature = "orderbook")]
pub fn parse_spot_depth(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<BookUpdate> {
    Some(BookUpdate {
        snapshot: true,
        version: data.get("version")?.as_u64()?,
        symbol: symbol.clone(),
        bids: parse_levels(data.get("bids"))?,
        asks: parse_levels(data.get("asks"))?,
        exchange_ts: parse_millis(data.get("ts")?).unwrap_or(received_ts),
        received_ts,
    })
}

/// USDT-M incremental depth: first push `event:"snapshot"`, later
/// `event:"update"` rows with absolute sizes; `version` auto-increments within
/// one connection.
#[cfg(feature = "orderbook")]
pub fn parse_swap_depth(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<BookUpdate> {
    let snapshot = match data.get("event")?.as_str()? {
        "snapshot" => true,
        "update" => false,
        _ => return None,
    };
    Some(BookUpdate {
        snapshot,
        version: data.get("version")?.as_u64()?,
        symbol: symbol.clone(),
        bids: parse_levels(data.get("bids"))?,
        asks: parse_levels(data.get("asks"))?,
        exchange_ts: parse_millis(data.get("ts")?).unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "candles")]
fn parse_kline(data: &Value, interval: &str, received_ts: f64, symbol: &Symbol) -> Option<Candle> {
    let start = data.get("id")?.as_f64()?;
    let seconds = interval_seconds(interval)?;
    Some(Candle {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        start,
        end: start + seconds,
        interval: interval.to_owned(),
        trades: data.get("count").and_then(|count| count.as_u64()),
        open: parse_decimal(data.get("open")?)?,
        close: parse_decimal(data.get("close")?)?,
        high: parse_decimal(data.get("high")?)?,
        low: parse_decimal(data.get("low")?)?,
        // Spot uses `amount` (base volume); swap uses `vol` (contracts) and
        // `amount` (coin). Both documented; the base/coin value is preferred.
        volume: parse_decimal(data.get("amount").or_else(|| data.get("vol"))?)?,
        // HTX transmits no closed flag; completion is not inferred.
        closed: None,
        exchange_ts: start,
        received_ts,
    })
}

#[cfg(feature = "candles")]
pub fn parse_spot_candle(
    data: &Value,
    period: &str,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Candle> {
    let interval = spot_interval(period)?;
    parse_kline(data, interval, received_ts, symbol)
}

#[cfg(feature = "candles")]
pub fn parse_swap_candle(
    data: &Value,
    period: &str,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Candle> {
    let interval = swap_interval(period)?;
    parse_kline(data, interval, received_ts, symbol)
}

/// Funding-rate notification row.
#[cfg(feature = "funding")]
pub fn parse_funding(
    row: &Value,
    exchange_ts: f64,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Funding> {
    Some(Funding {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        mark_price: None,
        rate: Some(parse_decimal(row.get("funding_rate")?)?),
        // `settlement_time` is the documented funding settlement timestamp;
        // `funding_time` is the current period's reference time, not a future
        // settlement instant.
        next_funding_time: parse_millis(row.get("settlement_time")?),
        predicted_rate: row.get("estimated_rate").and_then(parse_decimal),
        exchange_ts,
        received_ts,
    })
}

/// Liquidation notification row.
#[cfg(feature = "liquidations")]
pub fn parse_liquidation(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Liquidation> {
    Some(Liquidation {
        exchange: ExchangeId::Htx,
        symbol: symbol.clone(),
        side: trade_side(row)?,
        // Documented `amount` is the liquidation amount in the base token;
        // `volume` is the contract count and is not substituted.
        quantity: parse_decimal(row.get("amount")?)?,
        price: parse_decimal(row.get("price")?)?,
        id: None,
        status: LiquidationStatus::Filled,
        exchange_ts: row
            .get("created_at")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

/// Native contract/symbol embedded in a notification topic
/// (`public.BTC-USDT.funding_rate` -> `BTC-USDT`).
pub fn notification_symbol(topic: &str) -> Option<&str> {
    let mut parts = topic.split('.');
    if parts.next()? != "public" {
        return None;
    }
    let native = parts.next()?;
    (!native.is_empty()).then_some(native)
}

/// Topic suffix of a notification (`funding_rate` / `liquidation_orders`).
pub fn notification_kind(topic: &str) -> Option<&str> {
    let mut parts = topic.split('.');
    if parts.next()? != "public" {
        return None;
    }
    parts.next()?;
    parts.next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spot() -> Symbol {
        Symbol::spot("BTC", "USDT")
    }

    fn perp() -> Symbol {
        Symbol::perpetual("BTC", "USDT")
    }

    #[cfg(all(feature = "ticker", feature = "trade"))]
    #[test]
    fn parses_documented_spot_ticker_bbo_and_trade() {
        let ticker = json!({"bid": 52732.88, "bidSize": 0.036, "ask": 52732.89,
            "askSize": 0.583653, "lastPrice": 52735.63, "ts": 1630982370526_i64});
        let value = parse_ticker(&ticker, 1.0, &spot()).expect("ticker");
        assert_eq!(value.bid, Decimal::from_str_exact("52732.88").unwrap());
        assert_eq!(value.ask, Decimal::from_str_exact("52732.89").unwrap());

        let bbo = json!({"seqId": 137005210233_i64, "ask": 52665.02, "askSize": 1.502181,
            "bid": 52665.01, "bidSize": 0.178567,
            "quoteTime": 1630994555539_i64, "symbol": "btcusdt"});
        let bbo = parse_spot_bbo(&bbo, 1.0, &spot()).expect("bbo");
        assert_eq!(bbo.bid.amount, Decimal::from_str_exact("0.178567").unwrap());
        assert_eq!(bbo.ask.amount, Decimal::from_str_exact("1.502181").unwrap());

        let trade = json!({"id": 1370054451093592864_i64,
            "ts": 1630994963173_i64, "tradeId": 102523573486_i64,
            "amount": 0.006754, "price": 52648.62, "direction": "buy"});
        let trade = parse_spot_trade(&trade, 1.0, &spot()).expect("trade");
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.amount, Decimal::from_str_exact("0.006754").unwrap());
        assert_eq!(trade.id.as_deref(), Some("102523573486"));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_documented_spot_snapshot_depth_with_version() {
        let data = json!({"bids": [[52690.69, 0.36281], [52690.68, 0.2]],
            "asks": [[52690.7, 0.372591]],
            "version": 136998124622_u64, "ts": 1630983549500_i64});
        let update = parse_spot_depth(&data, 1.0, &spot()).expect("depth");
        assert!(update.snapshot);
        assert_eq!(update.version, 136_998_124_622);
        assert_eq!(update.bids.len(), 2);
        assert!(parse_spot_depth(&json!({"bids": [], "asks": []}), 1.0, &spot()).is_none());
    }

    #[cfg(all(feature = "ticker", feature = "trade", feature = "orderbook"))]
    #[test]
    fn parses_documented_swap_detail_bbo_trade_and_incremental_depth() {
        let detail = json!({"id": 1603707840, "open": 12916.2, "close": 13065.8,
            "high": 13205.3, "low": 12852.8, "amount": 30.316, "vol": 30316,
            "trade_turnover": 395073.49, "count": 2983,
            "bid": [13684.5, 10615], "ask": [13684.6, 3440]});
        let ticker = parse_swap_detail_ticker(&detail, 1.0, &perp()).expect("detail");
        assert_eq!(ticker.bid, Decimal::from_str_exact("13684.5").unwrap());
        assert_eq!(ticker.ask, Decimal::from_str_exact("13684.6").unwrap());

        let bbo = json!({"mrid": 131599726, "id": 1603707934, "bid": [13064, 38],
            "ask": [13072.3, 205], "ts": 1603707934525_i64, "version": 131599726});
        let bbo = parse_swap_bbo(&bbo, 1.0, &perp()).expect("bbo");
        assert_eq!(bbo.bid.amount, Decimal::from(38));
        assert_eq!(bbo.ask.amount, Decimal::from(205));

        let trade = json!({"amount": 2, "quantity": 0.002, "trade_turnover": 26.1626,
            "ts": 1603695383124_i64, "id": 1314767870000_i64,
            "price": 13081.3, "direction": "buy"});
        let trade = parse_swap_trade(&trade, 1.0, &perp()).expect("trade");
        assert_eq!(trade.amount, Decimal::from_str_exact("0.002").unwrap());
        assert_eq!(trade.id.as_deref(), Some("1314767870000"));

        let snapshot = json!({"asks": [[13081.9, 206]], "bids": [[13071.9, 38]],
            "event": "snapshot", "version": 1512467_u64, "ts": 1603707712356_i64});
        let update = parse_swap_depth(&snapshot, 1.0, &perp()).expect("snapshot");
        assert!(update.snapshot);
        let update_row = json!({"asks": [[13081.9, 0]], "bids": [[13071.9, 40]],
            "event": "update", "version": 1512468_u64, "ts": 1603707712400_i64});
        let update = parse_swap_depth(&update_row, 1.0, &perp()).expect("update");
        assert!(!update.snapshot);
        assert_eq!(update.asks[0].amount, Decimal::ZERO);
        assert!(parse_swap_depth(&json!({"event": "reset", "version": 1}), 1.0, &perp()).is_none());
        // A swap trade row without the documented base-coin quantity fails closed.
        let no_quantity = json!({"amount": 2, "price": 1.0, "direction": "buy"});
        assert!(parse_swap_trade(&no_quantity, 1.0, &perp()).is_none());
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_documented_spot_and_swap_klines() {
        let spot_kline = json!({"id": 1630981680_i64, "open": 0.074849,
            "close": 0.0749, "high": 0.075, "low": 0.0748, "amount": 12.5,
            "count": 3, "vol": 0.93});
        let candle = parse_spot_candle(&spot_kline, "1min", 1.0, &spot()).expect("candle");
        assert_eq!(candle.interval, "1m");
        assert_eq!(candle.start, 1_630_981_680.0);
        assert_eq!(candle.end, 1_630_981_740.0);
        assert_eq!(candle.volume, Decimal::from_str_exact("12.5").unwrap());
        assert_eq!(candle.closed, None);

        let swap_kline = json!({"id": 1603695060_i64, "open": 13076.8, "close": 13076.8,
            "high": 13076.8, "low": 13076.8, "amount": 0.004, "vol": 4,
            "count": 1, "trade_turnover": 52.3});
        let candle = parse_swap_candle(&swap_kline, "1min", 1.0, &perp()).expect("candle");
        assert_eq!(candle.interval, "1m");
        assert_eq!(candle.volume, Decimal::from_str_exact("0.004").unwrap());
        assert_eq!(
            parse_spot_candle(&spot_kline, "1min", 1.0, &spot())
                .unwrap()
                .trades,
            Some(3)
        );
    }

    #[cfg(feature = "funding")]
    #[test]
    fn parses_documented_funding_notification() {
        let row = json!({"symbol": "BTC", "contract_code": "BTC-USDT",
            "fee_asset": "USDT", "funding_time": "1603778700000",
            "funding_rate": "-0.000220068774978695",
            "estimated_rate": "-0.000684397270167616",
            "settlement_time": "1603785600000"});
        let funding = parse_funding(&row, 1_603_778_748.166, 1.0, &perp()).expect("funding");
        assert_eq!(
            funding.rate,
            Some(Decimal::from_str_exact("-0.000220068774978695").unwrap())
        );
        assert_eq!(
            funding.predicted_rate,
            Some(Decimal::from_str_exact("-0.000684397270167616").unwrap())
        );
        assert_eq!(funding.next_funding_time, Some(1_603_785_600.0));
    }

    #[cfg(feature = "liquidations")]
    #[test]
    fn parses_documented_liquidation_notification() {
        let row = json!({"symbol": "O3", "contract_code": "O3-USDT",
            "direction": "sell", "offset": "close", "volume": 432,
            "price": 0.7858, "created_at": 1639122193172_i64, "amount": 432,
            "trade_turnover": 339.4656, "contract_type": "swap",
            "pair": "O3-USDT", "business_type": "swap"});
        let symbol = Symbol::perpetual("O3", "USDT");
        let liquidation = parse_liquidation(&row, 1.0, &symbol).expect("liquidation");
        assert_eq!(liquidation.side, Side::Sell);
        assert_eq!(liquidation.quantity, Decimal::from(432));
        assert_eq!(
            liquidation.price,
            Decimal::from_str_exact("0.7858").unwrap()
        );
        assert_eq!(liquidation.status, LiquidationStatus::Filled);
        assert_eq!(liquidation.id, None);
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_scientific_notation_prices_from_live_json_numbers() {
        // Live PEPE spot trade.detail rows serialize a tiny price as a JSON
        // number in scientific notation; `Decimal::from_str_exact` rejects the
        // exponent form, so the parser must accept it.
        let spot_row = json!({
            "id": 1793761200000000001_i64, "ts": 1793761200000_i64,
            "tradeId": 1793761200000000002_i64,
            "amount": 14659348.6, "price": 5.213462e-06, "direction": "sell"
        });
        let spot_trade = parse_spot_trade(&spot_row, 1.0, &spot()).expect("scientific spot trade");
        assert_eq!(spot_trade.side, Side::Sell);
        assert_eq!(
            spot_trade.price,
            Decimal::from_scientific("5.213462e-06").unwrap()
        );
        assert_eq!(
            spot_trade.amount,
            Decimal::from_str_exact("14659348.6").unwrap()
        );

        // USDT-M rows publish contracts in `amount` and base quantity in
        // `quantity`; the exponent price must parse without publishing the
        // contract count as coin.
        let swap_row = json!({
            "id": 1793761200000000003_i64, "ts": 1793761200000_i64,
            "amount": 2, "quantity": 2000000,
            "price": 5.1935e-06, "direction": "buy", "contract_code": "PEPE-USDT"
        });
        let swap_trade = parse_swap_trade(&swap_row, 1.0, &perp()).expect("scientific swap trade");
        assert_eq!(swap_trade.side, Side::Buy);
        assert_eq!(
            swap_trade.price,
            Decimal::from_scientific("5.1935e-06").unwrap()
        );
        assert_eq!(swap_trade.amount, Decimal::from(2_000_000));
    }

    #[test]
    fn splits_market_topics_and_notification_topics() {
        let market = json!({"ch": "market.btcusdt.depth.step0"});
        assert_eq!(channel(&market), Some("market.btcusdt.depth.step0"));
        assert_eq!(
            topic_parts("market.btcusdt.depth.step0"),
            Some(("btcusdt", "depth", Some("step0")))
        );
        assert_eq!(
            topic_parts("market.BTC-USDT.trade.detail"),
            Some(("BTC-USDT", "trade", Some("detail")))
        );
        assert_eq!(
            notification_symbol("public.BTC-USDT.funding_rate"),
            Some("BTC-USDT")
        );
        assert_eq!(
            notification_kind("public.BTC-USDT.liquidation_orders"),
            Some("liquidation_orders")
        );
        assert_eq!(spot_interval("1week"), Some("1w"));
        assert_eq!(swap_interval("1hour"), Some("1h"));
        assert_eq!(spot_interval("2hour"), None);
    }
}
