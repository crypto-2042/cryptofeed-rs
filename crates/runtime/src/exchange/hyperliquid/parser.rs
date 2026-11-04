//! Hyperliquid public WebSocket payload parsing.
//!
//! Wire facts come from the official GitBook WebSocket subscription pages
//! (`subscriptions`, `timeouts-and-heartbeats`) and the read-only live
//! verification recorded in `docs/hyperliquid-protocol.md` (2026-11-04). Data
//! frames are `{"channel":"<type>","data":..}`; the spot active-asset context
//! uses the distinct `activeSpotAssetCtx` channel even though the subscription
//! type is `activeAssetCtx`. Every decimal is transmitted as a JSON string
//! except candle `n` (a number) and the whole-number `tid`; parsers accept
//! string or number where the live shape has been observed to vary.
//!
//! The `l2Book` push is a complete replacement snapshot (`WsBook.levels` is
//! `[bids, asks]`, best-first, 20 levels by default and 5 with `fast:true`).
//! There is no sequence number, checksum or delta form, so the parser never
//! invents one.

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "trade")]
use cryptofeed_core::model::Side;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::PriceLevel;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

/// Documented `candle` intervals, in the normalized vocabulary.
pub const CANDLE_INTERVALS: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M",
];

/// Normalized candle interval -> documented wire interval. Hyperliquid spells
/// every interval exactly like the normalized vocabulary.
pub fn interval_wire(interval: &str) -> Option<&'static str> {
    CANDLE_INTERVALS
        .iter()
        .copied()
        .find(|candidate| *candidate == interval)
}

/// Normalized candle interval -> seconds, used to derive the bar end where the
/// exchange does not transmit an interval length.
pub fn interval_seconds(interval: &str) -> Option<f64> {
    Some(match interval {
        "1m" => 60.0,
        "3m" => 180.0,
        "5m" => 300.0,
        "15m" => 900.0,
        "30m" => 1_800.0,
        "1h" => 3_600.0,
        "2h" => 7_200.0,
        "4h" => 14_400.0,
        "8h" => 28_800.0,
        "12h" => 43_200.0,
        "1d" => 86_400.0,
        "3d" => 259_200.0,
        "1w" => 604_800.0,
        "1M" => 2_592_000.0,
        _ => return None,
    })
}

/// Parses a decimal transmitted as a JSON string or number.
pub fn parse_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str_exact(text).ok(),
        Value::Number(number) => Decimal::from_str_exact(&number.to_string()).ok(),
        _ => None,
    }
}

/// Milliseconds-since-epoch number/string -> Unix seconds.
pub fn parse_millis(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().map(|millis| millis / 1000.0),
        Value::String(text) => text.parse::<f64>().ok().map(|millis| millis / 1000.0),
        _ => None,
    }
}

/// Data frame channel (`trades`, `l2Book`, `bbo`, `candle`,
/// `activeAssetCtx`, `activeSpotAssetCtx`, `subscriptionResponse`, `error`,
/// `pong`).
pub fn channel(message: &Value) -> Option<&str> {
    message.get("channel").and_then(Value::as_str)
}

/// Payload of a data frame.
pub fn data(message: &Value) -> Option<&Value> {
    message.get("data")
}

/// Coin/identity carried by a data frame payload.
pub fn data_coin(data: &Value) -> Option<&str> {
    data.get("coin").and_then(Value::as_str)
}

/// One parsed `l2Book` whole-book snapshot.
#[cfg(feature = "orderbook")]
#[derive(Clone, Debug, PartialEq)]
pub struct BookUpdate {
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
        let level = row.as_object()?;
        levels.push(PriceLevel {
            price: parse_decimal(level.get("px")?)?,
            amount: parse_decimal(level.get("sz")?)?,
        });
    }
    Some(levels)
}

/// `l2Book` payload: `{"coin":..,"time":ms,"levels":[[WsLevel..],[WsLevel..]]}`.
/// Both sides are complete replacements; empty sides are valid.
#[cfg(feature = "orderbook")]
pub fn parse_book(data: &Value, received_ts: f64, symbol: &Symbol) -> Option<BookUpdate> {
    let levels = data.get("levels")?.as_array()?;
    if levels.len() != 2 {
        return None;
    }
    Some(BookUpdate {
        symbol: symbol.clone(),
        bids: parse_levels(Some(&levels[0]))?,
        asks: parse_levels(Some(&levels[1]))?,
        exchange_ts: parse_millis(data.get("time")?)?,
        received_ts,
    })
}

/// One documented `trades` row.
#[cfg(feature = "trade")]
pub fn parse_trade(row: &Value, received_ts: f64, symbol: &Symbol) -> Option<Trade> {
    let side = match row.get("side")?.as_str()? {
        "B" => Side::Buy,
        "A" => Side::Sell,
        _ => return None,
    };
    Some(Trade {
        exchange: ExchangeId::Hyperliquid,
        symbol: symbol.clone(),
        side,
        amount: parse_decimal(row.get("sz")?)?,
        price: parse_decimal(row.get("px")?)?,
        exchange_ts: parse_millis(row.get("time")?)?,
        received_ts,
        // `tid` is a 50-bit hash of (buyer_oid, seller_oid), not a globally
        // unique trade id; it is preserved verbatim and never combined with
        // block time or coin into an invented identifier.
        id: match row.get("tid")? {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => return None,
        },
        implied_volatility: None,
    })
}

/// One parsed `bbo` payload. Either side may be documented `null`; callers
/// emit a quote only when both sides are present.
#[cfg(any(feature = "ticker", feature = "orderbook"))]
#[derive(Clone, Debug, PartialEq)]
pub struct BboQuote {
    pub bid: Option<(Decimal, Decimal)>,
    pub ask: Option<(Decimal, Decimal)>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

/// `bbo` payload: `{"coin":..,"time":ms,"bbo":[WsLevel|null, WsLevel|null]}`.
#[cfg(any(feature = "ticker", feature = "orderbook"))]
pub fn parse_bbo(data: &Value, received_ts: f64) -> Option<BboQuote> {
    let bbo = data.get("bbo")?.as_array()?;
    if bbo.len() != 2 {
        return None;
    }
    let side = |value: &Value| -> Option<Option<(Decimal, Decimal)>> {
        if value.is_null() {
            return Some(None);
        }
        Some(Some((
            parse_decimal(value.get("px")?)?,
            parse_decimal(value.get("sz")?)?,
        )))
    };
    Some(BboQuote {
        bid: side(&bbo[0])?,
        ask: side(&bbo[1])?,
        exchange_ts: parse_millis(data.get("time")?)?,
        received_ts,
    })
}

/// `candle` payload: `{"t":open_ms,"T":close_ms,"s":coin,"i":interval,
/// "o":..,"c":..,"h":..,"l":..,"v":base_volume,"n":trade_count}`. The
/// documented `data` format is `Candle[]` while the live shape is one object;
/// both are accepted. No closed flag is transmitted.
#[cfg(feature = "candles")]
pub fn parse_candle(
    row: &Value,
    interval: &str,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Candle> {
    if let Some(wire_interval) = row.get("i").and_then(Value::as_str) {
        if wire_interval != interval {
            return None;
        }
    }
    let start = parse_millis(row.get("t")?)?;
    Some(Candle {
        exchange: ExchangeId::Hyperliquid,
        symbol: symbol.clone(),
        start,
        end: start + interval_seconds(interval)?,
        interval: interval.to_owned(),
        trades: row.get("n").and_then(Value::as_u64),
        open: parse_decimal(row.get("o")?)?,
        close: parse_decimal(row.get("c")?)?,
        high: parse_decimal(row.get("h")?)?,
        low: parse_decimal(row.get("l")?)?,
        volume: parse_decimal(row.get("v")?)?,
        closed: None,
        exchange_ts: start,
        received_ts,
    })
}

/// Product family of an `activeAssetCtx` payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssetCtxProduct {
    Perpetual,
    Spot,
}

/// Perpetual fields split from one `activeAssetCtx` payload. All four values
/// are documented as required for a perpetual context; a missing or
/// unparsable field fails the frame closed.
#[cfg(any(
    feature = "funding",
    feature = "openinterest",
    feature = "markprice",
    feature = "index"
))]
#[derive(Clone, Debug, PartialEq)]
pub struct AssetCtx {
    pub funding: Option<Decimal>,
    pub open_interest: Option<Decimal>,
    pub mark_px: Option<Decimal>,
    pub oracle_px: Option<Decimal>,
    pub received_ts: f64,
}

/// `activeAssetCtx` / `activeSpotAssetCtx` payload:
/// `{"coin":..,"ctx":{...}}`. The payload carries no exchange timestamp, so
/// the receive timestamp is the only time anchor.
#[cfg(any(
    feature = "funding",
    feature = "openinterest",
    feature = "markprice",
    feature = "index"
))]
pub fn parse_asset_ctx(
    data: &Value,
    product: AssetCtxProduct,
    received_ts: f64,
) -> Option<AssetCtx> {
    let ctx = data.get("ctx")?;
    let mark_px = parse_decimal(ctx.get("markPx")?)?;
    match product {
        AssetCtxProduct::Perpetual => Some(AssetCtx {
            funding: Some(parse_decimal(ctx.get("funding")?)?),
            open_interest: Some(parse_decimal(ctx.get("openInterest")?)?),
            mark_px: Some(mark_px),
            oracle_px: Some(parse_decimal(ctx.get("oraclePx")?)?),
            received_ts,
        }),
        AssetCtxProduct::Spot => Some(AssetCtx {
            funding: None,
            open_interest: None,
            mark_px: Some(mark_px),
            oracle_px: None,
            received_ts,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn perp() -> Symbol {
        Symbol::perpetual("BTC", "USD")
    }

    fn spot() -> Symbol {
        Symbol::spot("PURR", "USDC")
    }

    #[test]
    fn maps_documented_candle_intervals_to_the_normalized_vocabulary() {
        for interval in [
            "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M",
        ] {
            assert_eq!(interval_wire(interval), Some(interval));
            assert!(interval_seconds(interval).is_some());
        }
        assert_eq!(interval_wire("6h"), None);
        assert_eq!(interval_wire("1s"), None);
        assert_eq!(interval_seconds("6h"), None);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_documented_whole_book_snapshot() {
        let data = json!({
            "coin": "BTC",
            "time": 1_793_782_877_454_i64,
            "levels": [
                [{"px": "78454.0", "sz": "0.09296", "n": 5},
                 {"px": "78452.0", "sz": "0.00236", "n": 2}],
                [{"px": "78455.0", "sz": "20.3117", "n": 62}]
            ]
        });
        let update = parse_book(&data, 2.0, &perp()).expect("book");
        assert_eq!(update.bids.len(), 2);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(
            update.bids[0].price,
            Decimal::from_str_exact("78454.0").unwrap()
        );
        assert_eq!(
            update.asks[0].amount,
            Decimal::from_str_exact("20.3117").unwrap()
        );
        assert_eq!(update.exchange_ts, 1_793_782_877.454);
        // An empty side is a valid whole-book snapshot; a missing side array,
        // a missing time, or a malformed level fails closed.
        let empty =
            parse_book(&json!({"time": 1, "levels": [[], []]}), 2.0, &perp()).expect("empty sides");
        assert!(empty.bids.is_empty() && empty.asks.is_empty());
        assert!(parse_book(&json!({"time": 1, "levels": [[]]}), 2.0, &perp()).is_none());
        assert!(parse_book(&json!({"levels": [[], []]}), 2.0, &perp()).is_none());
        assert!(
            parse_book(
                &json!({"time": 1, "levels": [[{"px": "x", "sz": "1"}], []]}),
                2.0,
                &perp()
            )
            .is_none()
        );
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_documented_trades_with_buyer_and_seller_sides() {
        let row = json!({
            "coin": "BTC", "side": "A", "px": "78468.0", "sz": "0.10569",
            "time": 1_793_782_868_033_i64,
            "hash": "0x6fb7ddab68ab54cf713104480a64d3020593009103ae73a1138088fe27af2eba",
            "tid": 282_768_822_863_722_i64,
            "users": ["0xd72cfd2424e26ba32e46b7b2aff95eff34247a71",
                      "0x49a648936441b22f28f069d3c088928682b277ae"]
        });
        let trade = parse_trade(&row, 2.0, &perp()).expect("trade");
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.price, Decimal::from_str_exact("78468.0").unwrap());
        assert_eq!(trade.amount, Decimal::from_str_exact("0.10569").unwrap());
        assert_eq!(trade.id.as_deref(), Some("282768822863722"));
        assert_eq!(trade.exchange_ts, 1_793_782_868.033);
        let buy = json!({"side": "B", "px": "1", "sz": "2", "time": 1, "tid": 3});
        assert_eq!(parse_trade(&buy, 2.0, &perp()).unwrap().side, Side::Buy);
        // Unknown sides and missing documented fields fail closed.
        assert!(
            parse_trade(
                &json!({"side": "X", "px": "1", "sz": "2", "time": 1}),
                2.0,
                &perp()
            )
            .is_none()
        );
        assert!(parse_trade(&json!({"side": "B", "px": "1", "time": 1}), 2.0, &perp()).is_none());
    }

    #[cfg(any(feature = "ticker", feature = "orderbook"))]
    #[test]
    fn parses_documented_bbo_and_preserves_documented_null_sides() {
        let data = json!({
            "coin": "BTC", "time": 1_793_782_877_739_i64,
            "bbo": [{"px": "78451.0", "sz": "0.22297", "n": 1},
                    {"px": "78452.0", "sz": "13.21482", "n": 74}]
        });
        let quote = parse_bbo(&data, 2.0).expect("bbo");
        assert_eq!(
            quote.bid,
            Some((
                Decimal::from_str_exact("78451.0").unwrap(),
                Decimal::from_str_exact("0.22297").unwrap()
            ))
        );
        assert_eq!(
            quote.ask,
            Some((
                Decimal::from_str_exact("78452.0").unwrap(),
                Decimal::from_str_exact("13.21482").unwrap()
            ))
        );
        // A null side is documented, not malformed: the parser reports it so a
        // caller never fabricates the missing quote.
        let one_sided = json!({
            "coin": "PURR/USDC", "time": 1_i64,
            "bbo": [{"px": "1", "sz": "2", "n": 1}, null]
        });
        let quote = parse_bbo(&one_sided, 2.0).expect("one-sided bbo");
        assert!(quote.bid.is_some());
        assert_eq!(quote.ask, None);
        assert!(parse_bbo(&json!({"time": 1, "bbo": [null]}), 2.0).is_none());
        assert!(parse_bbo(&json!({"bbo": [null, null]}), 2.0).is_none());
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_documented_candle_object_and_array_shapes() {
        let row = json!({
            "t": 1_793_782_860_000_i64, "T": 1_793_782_919_999_i64, "s": "BTC", "i": "1m",
            "o": "78469.0", "c": "78451.0", "h": "78469.0", "l": "78451.0",
            "v": "11.58778", "n": 86
        });
        let candle = parse_candle(&row, "1m", 2.0, &perp()).expect("candle");
        assert_eq!(candle.interval, "1m");
        assert_eq!(candle.start, 1_793_782_860.0);
        assert_eq!(candle.end, 1_793_782_920.0);
        assert_eq!(candle.trades, Some(86));
        assert_eq!(candle.open, Decimal::from_str_exact("78469.0").unwrap());
        assert_eq!(candle.volume, Decimal::from_str_exact("11.58778").unwrap());
        assert_eq!(candle.closed, None);
        // A candle whose documented `i` contradicts the subscription interval
        // is malformed for this feed.
        assert!(parse_candle(&row, "5m", 2.0, &perp()).is_none());
        assert!(parse_candle(&json!({"t": 1}), "1m", 2.0, &perp()).is_none());
        assert_eq!(
            parse_candle(&row, "1m", 2.0, &spot())
                .unwrap()
                .symbol
                .as_str(),
            "PURR-USDC"
        );
    }

    #[cfg(any(
        feature = "funding",
        feature = "openinterest",
        feature = "markprice",
        feature = "index"
    ))]
    #[test]
    fn splits_documented_perpetual_asset_context_fields() {
        let data = json!({
            "coin": "BTC",
            "ctx": {
                "funding": "0.0000125",
                "openInterest": "39370.73328",
                "prevDayPx": "76611.0",
                "dayNtlVlm": "3444111352.420372963",
                "premium": "-0.0004510106",
                "oraclePx": "78490.4",
                "markPx": "78464.8",
                "midPx": "78454.5",
                "impactPxs": ["78454.0", "78455.0"],
                "dayBaseVlm": "44617.00231"
            }
        });
        let ctx = parse_asset_ctx(&data, AssetCtxProduct::Perpetual, 2.0).expect("ctx");
        assert_eq!(
            ctx.funding,
            Some(Decimal::from_str_exact("0.0000125").unwrap())
        );
        assert_eq!(
            ctx.open_interest,
            Some(Decimal::from_str_exact("39370.73328").unwrap())
        );
        assert_eq!(
            ctx.mark_px,
            Some(Decimal::from_str_exact("78464.8").unwrap())
        );
        assert_eq!(
            ctx.oracle_px,
            Some(Decimal::from_str_exact("78490.4").unwrap())
        );
        assert_eq!(ctx.received_ts, 2.0);
        // A perpetual context missing a documented required field fails closed.
        assert!(
            parse_asset_ctx(
                &json!({"coin": "BTC", "ctx": {"markPx": "1", "funding": "0"}}),
                AssetCtxProduct::Perpetual,
                2.0
            )
            .is_none()
        );
    }

    #[cfg(any(
        feature = "funding",
        feature = "openinterest",
        feature = "markprice",
        feature = "index"
    ))]
    #[test]
    fn parses_documented_spot_asset_context_mark_price_only() {
        let data = json!({
            "coin": "PURR/USDC",
            "ctx": {
                "prevDayPx": "0.11235",
                "dayNtlVlm": "1869165.0426500002",
                "markPx": "0.11552",
                "midPx": "0.115385",
                "circulatingSupply": "594666965.8429700136",
                "coin": "PURR/USDC",
                "totalSupply": "594666977.1494899988",
                "dayBaseVlm": "16536048.0"
            }
        });
        let ctx = parse_asset_ctx(&data, AssetCtxProduct::Spot, 2.0).expect("spot ctx");
        assert_eq!(
            ctx.mark_px,
            Some(Decimal::from_str_exact("0.11552").unwrap())
        );
        assert_eq!(ctx.funding, None);
        assert_eq!(ctx.open_interest, None);
        assert_eq!(ctx.oracle_px, None);
        assert!(
            parse_asset_ctx(
                &json!({"coin": "PURR/USDC", "ctx": {"midPx": "0.1"}}),
                AssetCtxProduct::Spot,
                2.0
            )
            .is_none()
        );
    }

    #[test]
    fn reads_frame_channel_and_payload_identity() {
        let message = json!({"channel": "trades", "data": [{"coin": "BTC"}]});
        assert_eq!(channel(&message), Some("trades"));
        assert_eq!(data(&message).and_then(data_coin), None);
        let spot = json!({"channel": "activeSpotAssetCtx", "data": {"coin": "PURR/USDC"}});
        assert_eq!(channel(&spot), Some("activeSpotAssetCtx"));
        assert_eq!(data(&spot).and_then(data_coin), Some("PURR/USDC"));
        assert_eq!(channel(&json!({"data": {}})), None);
    }
}
