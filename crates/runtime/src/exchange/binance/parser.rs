use super::adapter::BinanceInstrument;
#[cfg(feature = "orderbook")]
use super::book_sync::{BinanceDepthDelta, BinanceSequencedDepthDelta};
#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(any(feature = "trade", feature = "liquidations"))]
use cryptofeed_core::model::Side;
use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPrice;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::{Liquidation, LiquidationStatus};
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L1Book, L2Book, L2BookDelta, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;
use rust_decimal::Decimal;
use serde_json::Value;

pub fn parse_trade_symbol(raw: &str) -> String {
    raw.strip_suffix("USDT")
        .filter(|base| !base.is_empty())
        .map(|base| format!("{base}-USDT"))
        .unwrap_or_else(|| raw.to_owned())
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_trade_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "trade")]
pub fn parse_trade_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<Trade> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_trade_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "trade")]
fn parse_trade_with_symbol(message: &Value, received_ts: f64, symbol: Symbol) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Binance,
        symbol,
        side: if message.get("m")?.as_bool()? {
            Side::Sell
        } else {
            Side::Buy
        },
        amount: parse_decimal(message.get("q")?)?,
        price: parse_decimal(message.get("p")?)?,
        exchange_ts: parse_millis(message.get("T")?)?,
        received_ts,
        id: Some(message.get("a")?.to_string()),
        implied_volatility: None,
    })
}

#[cfg(feature = "ticker")]
pub fn parse_option_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_option_ticker_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "ticker")]
pub fn parse_option_ticker_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<Ticker> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_option_ticker_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "ticker")]
fn parse_option_ticker_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Symbol,
) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Binance,
        symbol,
        bid: parse_decimal(message.get("bidOpenPrice")?)?,
        ask: parse_decimal(message.get("askOpenPrice")?)?,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
        // Binance option tickers carry the implied volatility.
        implied_volatility: message.get("volatility").and_then(parse_decimal),
    })
}

#[cfg(feature = "trade")]
pub fn parse_option_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_option_trade_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "trade")]
pub fn parse_option_trade_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<Trade> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_option_trade_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "trade")]
fn parse_option_trade_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Symbol,
) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Binance,
        symbol,
        // Option trade side is -1 (active sell) or 1 (active buy).
        side: if message.get("side")?.as_i64()? < 0 {
            Side::Sell
        } else {
            Side::Buy
        },
        amount: parse_decimal(message.get("quantity")?)?,
        price: parse_decimal(message.get("price")?)?,
        exchange_ts: message
            .get("tradeTime")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
        id: message
            .get("tradeId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        implied_volatility: None,
    })
}

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_ticker_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "ticker")]
pub fn parse_ticker_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<Ticker> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_ticker_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "ticker")]
fn parse_ticker_with_symbol(message: &Value, received_ts: f64, symbol: Symbol) -> Option<Ticker> {
    Some(Ticker {
        exchange: ExchangeId::Binance,
        symbol,
        bid: parse_decimal(message.get("b")?)?,
        ask: parse_decimal(message.get("a")?)?,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_book(message: &Value, received_ts: f64) -> Option<L1Book> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_l1_book_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_book_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<L1Book> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_l1_book_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "orderbook")]
fn parse_l1_book_with_symbol(message: &Value, received_ts: f64, symbol: Symbol) -> Option<L1Book> {
    // Binance spot and USD-M bookTicker streams carry only `u,s,b,B,a,A`
    // (no event type or event time); the receive timestamp is the only
    // timestamp available, exactly as with the BBO Ticker parse.
    Some(L1Book {
        exchange: ExchangeId::Binance,
        symbol,
        bid: PriceLevel {
            price: parse_decimal(message.get("b")?)?,
            amount: parse_decimal(message.get("B")?)?,
        },
        ask: PriceLevel {
            price: parse_decimal(message.get("a")?)?,
            amount: parse_decimal(message.get("A")?)?,
        },
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "index")]
pub fn parse_index_price(message: &Value, received_ts: f64) -> Option<IndexPrice> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_index_price_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "index")]
pub fn parse_index_price_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<IndexPrice> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_index_price_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "index")]
fn parse_index_price_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Symbol,
) -> Option<IndexPrice> {
    let from_mark = message.get("e").and_then(Value::as_str) == Some("markPriceUpdate");
    // In markPriceUpdate, i is the index price and E the event timestamp;
    // T is the next funding time. Legacy index fixtures retain p/T parsing.
    Some(IndexPrice {
        exchange: ExchangeId::Binance,
        symbol,
        price: parse_decimal(message.get(if from_mark { "i" } else { "p" })?)?,
        open_24h: None,
        high_24h: None,
        low_24h: None,
        exchange_ts: message
            .get(if from_mark { "E" } else { "T" })
            .and_then(parse_millis)
            .or_else(|| message.get("E").and_then(parse_millis))
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest(message: &Value, received_ts: f64) -> Option<OpenInterest> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_open_interest_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<OpenInterest> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_open_interest_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "openinterest")]
fn parse_open_interest_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Symbol,
) -> Option<OpenInterest> {
    // USD-M `<symbol>@openInterest` pushes `o` (open interest in the base
    // asset) every five minutes together with 24h `h/l` bounds and `c`
    // change; USD value is not carried by the stream.
    Some(OpenInterest {
        exchange: ExchangeId::Binance,
        symbol,
        open_interest: parse_decimal(message.get("o")?)?,
        coin_quantity: None,
        value_usd: None,
        exchange_ts: message
            .get("T")
            .and_then(parse_millis)
            .or_else(|| message.get("E").and_then(parse_millis))
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_candle_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "candles")]
pub fn parse_candle_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<Candle> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_candle_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "candles")]
fn parse_candle_with_symbol(message: &Value, received_ts: f64, symbol: Symbol) -> Option<Candle> {
    let candle = message.get("k")?;
    Some(Candle {
        exchange: ExchangeId::Binance,
        symbol,
        start: parse_millis(candle.get("t")?)?,
        end: parse_millis(candle.get("T")?)?,
        interval: candle.get("i")?.as_str()?.to_owned(),
        trades: candle.get("n").and_then(|v| v.as_u64()),
        open: parse_decimal(candle.get("o")?)?,
        close: parse_decimal(candle.get("c")?)?,
        high: parse_decimal(candle.get("h")?)?,
        low: parse_decimal(candle.get("l")?)?,
        volume: parse_decimal(candle.get("v")?)?,
        closed: candle.get("x").and_then(|v| v.as_bool()),
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "funding")]
pub fn parse_funding(message: &Value, received_ts: f64) -> Option<Funding> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_funding_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "funding")]
pub fn parse_funding_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<Funding> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_funding_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "funding")]
fn parse_funding_with_symbol(message: &Value, received_ts: f64, symbol: Symbol) -> Option<Funding> {
    let next_funding_time = message
        .get("T")
        .and_then(|v| v.as_i64())
        .filter(|v| *v > 0)
        .map(|v| v as f64 / 1000.0);
    let rate = if next_funding_time.is_some() {
        message
            .get("r")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .and_then(|v| Decimal::from_str_exact(v).ok())
    } else {
        None
    };

    Some(Funding {
        exchange: ExchangeId::Binance,
        symbol,
        mark_price: message.get("p").and_then(parse_decimal),
        rate,
        next_funding_time,
        predicted_rate: None,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "markprice")]
pub fn parse_mark_price(message: &Value, received_ts: f64) -> Option<MarkPrice> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_mark_price_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "markprice")]
pub fn parse_mark_price_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<MarkPrice> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_mark_price_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "markprice")]
fn parse_mark_price_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Symbol,
) -> Option<MarkPrice> {
    // The USD-M/Coin-M `markPriceUpdate` payload doubles as the funding
    // source; `p` is the mark price, `T` the next funding time, `P` the
    // estimated settlement price (not a rate), and `E` the event time.
    Some(MarkPrice {
        exchange: ExchangeId::Binance,
        symbol,
        price: parse_decimal(message.get("p")?)?,
        next_funding_time: message
            .get("T")
            .and_then(|v| v.as_i64())
            .filter(|v| *v > 0)
            .map(|v| v as f64 / 1000.0),
        predicted_rate: None,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidation(message: &Value, received_ts: f64) -> Option<Liquidation> {
    let order = message.get("o")?;
    let symbol = parse_symbol(order.get("s")?.as_str()?)?;
    parse_liquidation_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidation_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<Liquidation> {
    let order = message.get("o")?;
    let symbol = resolved_symbol(order.get("s")?.as_str()?, instrument)?;
    parse_liquidation_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "liquidations")]
fn parse_liquidation_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Symbol,
) -> Option<Liquidation> {
    let order = message.get("o")?;
    Some(Liquidation {
        exchange: ExchangeId::Binance,
        symbol,
        side: if order.get("S")?.as_str()? == "SELL" {
            Side::Sell
        } else {
            Side::Buy
        },
        quantity: parse_decimal(order.get("q")?)?,
        price: parse_decimal(order.get("p")?)?,
        // The `forceOrder` payload carries the order id in `o.i`.
        id: order
            .get("i")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        status: if order.get("X")?.as_str()? == "FILLED" {
            LiquidationStatus::Filled
        } else {
            LiquidationStatus::Unfilled
        },
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_l2_book_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<L2Book> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    parse_l2_book_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "orderbook")]
fn parse_l2_book_with_symbol(message: &Value, received_ts: f64, symbol: Symbol) -> Option<L2Book> {
    Some(L2Book::Delta(L2BookDelta {
        exchange: ExchangeId::Binance,
        symbol,
        bids: parse_levels(message.get("b")?)?,
        asks: parse_levels(message.get("a")?)?,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    }))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_snapshot(
    message: &Value,
    symbol: &str,
    received_ts: f64,
) -> Option<(u64, cryptofeed_orderbook::L2BookSnapshot)> {
    Some((
        message.get("lastUpdateId")?.as_u64()?,
        cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: parse_symbol(symbol)?,
            bids: parse_levels(message.get("bids")?)?,
            asks: parse_levels(message.get("asks")?)?,
            exchange_ts: received_ts,
            received_ts,
        },
    ))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_snapshot_for_instrument(
    message: &Value,
    instrument: &BinanceInstrument,
    received_ts: f64,
) -> Option<(u64, cryptofeed_orderbook::L2BookSnapshot)> {
    Some((
        message.get("lastUpdateId")?.as_u64()?,
        cryptofeed_orderbook::L2BookSnapshot {
            exchange: ExchangeId::Binance,
            symbol: instrument.symbol.clone(),
            bids: parse_levels(message.get("bids")?)?,
            asks: parse_levels(message.get("asks")?)?,
            exchange_ts: message
                .get("E")
                .and_then(parse_millis)
                .unwrap_or(received_ts),
            received_ts,
        },
    ))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update(message: &Value, received_ts: f64) -> Option<BinanceDepthDelta> {
    let symbol = parse_symbol(message.get("s")?.as_str()?)?;
    parse_l2_book_update_with_symbol(message, received_ts, symbol)
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<BinanceSequencedDepthDelta> {
    let symbol = resolved_symbol(message.get("s")?.as_str()?, instrument)?;
    Some(BinanceSequencedDepthDelta {
        delta: parse_l2_book_update_with_symbol(message, received_ts, symbol)?,
        previous_update_id: message.get("pu").and_then(Value::as_u64),
    })
}

/// Parse a spot partial-depth push (`@depth5@100ms` etc.): the payload is
/// `{"lastUpdateId": <id>, "bids": [...], "asks": [...]}` — the complete
/// top-N book with no `e`/`s`/`U`/`u`. `lastUpdateId` doubles as both the
/// first and last update id; each push replaces the local top-N book.
#[cfg(feature = "orderbook")]
pub fn parse_partial_depth_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &BinanceInstrument,
) -> Option<BinanceSequencedDepthDelta> {
    let update_id = message.get("lastUpdateId")?.as_u64()?;
    let book = L2BookDelta {
        exchange: ExchangeId::Binance,
        symbol: instrument.symbol.clone(),
        bids: parse_levels(message.get("bids")?)?,
        asks: parse_levels(message.get("asks")?)?,
        exchange_ts: message
            .get("E")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    };
    Some(BinanceSequencedDepthDelta {
        delta: BinanceDepthDelta {
            first_update_id: update_id,
            last_update_id: update_id,
            book,
        },
        previous_update_id: None,
    })
}

#[cfg(feature = "orderbook")]
fn parse_l2_book_update_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Symbol,
) -> Option<BinanceDepthDelta> {
    Some(BinanceDepthDelta {
        first_update_id: message.get("U")?.as_u64()?,
        last_update_id: message.get("u")?.as_u64()?,
        book: L2BookDelta {
            exchange: ExchangeId::Binance,
            symbol,
            bids: parse_levels(message.get("b")?)?,
            asks: parse_levels(message.get("a")?)?,
            exchange_ts: message
                .get("E")
                .and_then(parse_millis)
                .unwrap_or(received_ts),
            received_ts,
        },
    })
}

fn parse_symbol(raw: &str) -> Option<Symbol> {
    let normalized = parse_trade_symbol(raw);
    let parts: Vec<_> = normalized.split('-').collect();
    match parts.as_slice() {
        [base, quote] => Some(Symbol::spot(base, quote)),
        _ => None,
    }
}

fn resolved_symbol(raw: &str, instrument: &BinanceInstrument) -> Option<Symbol> {
    raw.eq_ignore_ascii_case(&instrument.exchange_symbol)
        .then(|| instrument.symbol.clone())
}

/// Parse wire decimals from strings or lossless JSON numbers. Preserve source
/// scale and never convert prices/quantities through binary floating point.
fn parse_decimal(value: &Value) -> Option<Decimal> {
    if let Some(text) = value.as_str() {
        return Decimal::from_str_exact(text).ok();
    }
    value
        .as_number()
        .map(|number| number.to_string())
        .and_then(|text| Decimal::from_str_exact(&text).ok())
}

#[cfg(feature = "orderbook")]
fn parse_levels(value: &Value) -> Option<Vec<PriceLevel>> {
    let levels = value.as_array()?;
    levels
        .iter()
        .map(|level| {
            let pair = level.as_array()?;
            Some(PriceLevel {
                price: parse_decimal(pair.first()?)?,
                amount: parse_decimal(pair.get(1)?)?,
            })
        })
        .collect()
}

fn parse_millis(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|v| v as f64))
        .map(|v| v / 1000.0)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "candles")]
    use super::parse_candle;
    #[cfg(feature = "funding")]
    use super::parse_funding;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book;
    #[cfg(feature = "liquidations")]
    use super::parse_liquidation;
    #[cfg(feature = "ticker")]
    use super::parse_ticker;
    #[cfg(feature = "orderbook")]
    use super::{parse_l2_book_snapshot, parse_l2_book_snapshot_for_instrument};
    #[cfg(feature = "orderbook")]
    use super::{parse_l2_book_update, parse_l2_book_update_for_instrument};
    use super::{parse_symbol, parse_trade_symbol};
    #[cfg(feature = "trade")]
    use super::{parse_trade, parse_trade_for_instrument};
    use crate::exchange::binance::adapter::{BinanceInstrument, BinanceProduct};
    use rust_decimal::Decimal;
    use serde_json::json;

    #[test]
    fn parses_binance_trade_symbol() {
        assert_eq!(parse_trade_symbol("BTCUSDT"), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn resolved_instrument_preserves_non_usdt_and_derivative_identity() {
        let spot = BinanceInstrument::new(
            cryptofeed_core::symbol::Symbol::spot("eth", "btc"),
            "ETHBTC",
            BinanceProduct::Spot,
        );
        let spot_message = json!({
            "s": "ETHBTC", "a": 1, "p": "0.05", "q": "2", "T": 1, "m": false
        });
        let spot_trade = parse_trade_for_instrument(&spot_message, 1.0, &spot).expect("spot");
        assert_eq!(spot_trade.symbol.as_str(), "ETH-BTC");

        let future = BinanceInstrument::new(
            cryptofeed_core::symbol::Symbol::futures("btc", "usd", "240927"),
            "BTCUSD_240927",
            BinanceProduct::CoinM,
        );
        let future_message = json!({
            "s": "BTCUSD_240927", "a": 2, "p": "65000", "q": "1", "T": 1, "m": true
        });
        let future_trade =
            parse_trade_for_instrument(&future_message, 1.0, &future).expect("future");
        assert_eq!(future_trade.symbol.as_str(), "BTC-USD-240927");
    }

    #[test]
    fn malformed_native_symbol_does_not_panic() {
        assert_eq!(parse_trade_symbol("ETHBTC"), "ETHBTC");
        assert!(parse_symbol("ETHBTC").is_none());
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_binance_trade_message() {
        let message = json!({
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        });

        let trade = parse_trade(&message, 1710000001.5).expect("trade");

        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
        assert_eq!(trade.price, Decimal::from_str_exact("65000.50").unwrap());
        assert_eq!(trade.amount, Decimal::from_str_exact("0.01000000").unwrap());
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_binance_ticker_message() {
        let message = json!({
            "s": "BTCUSDT",
            "b": "64999.10",
            "a": "65000.20",
            "E": 1710000000456u64
        });

        let ticker = parse_ticker(&message, 1710000001.5).expect("ticker");

        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
        assert_eq!(ticker.bid, Decimal::from_str_exact("64999.10").unwrap());
        assert_eq!(ticker.ask, Decimal::from_str_exact("65000.20").unwrap());
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_binance_candle_message() {
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

        let candle = parse_candle(&message, 1615927656.0).expect("candle");

        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
        assert_eq!(candle.interval, "1m");
        assert_eq!(candle.start, 1615927620.0);
        assert_eq!(candle.closed, Some(true));
    }

    #[cfg(feature = "funding")]
    #[test]
    fn parses_binance_funding_message() {
        let message = json!({
            "e": "markPriceUpdate",
            "E": 1562305380000i64,
            "s": "BTCUSDT",
            "p": "11185.87786614",
            "r": "0.00030000",
            "T": 1562306400000i64
        });

        let funding = parse_funding(&message, 1562305381.0).expect("funding");

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
    }

    #[cfg(feature = "liquidations")]
    #[test]
    fn parses_binance_liquidation_message() {
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

        let liquidation = parse_liquidation(&message, 1568014461.0).expect("liquidation");

        assert_eq!(liquidation.symbol.as_str(), "BTC-USDT");
        assert_eq!(liquidation.side, cryptofeed_trade::model::Side::Sell);
        assert_eq!(
            liquidation.quantity,
            Decimal::from_str_exact("0.014").unwrap()
        );
        assert_eq!(liquidation.price, Decimal::from_str_exact("9910").unwrap());
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_binance_l2_book_message() {
        let message = json!({
            "e": "depthUpdate",
            "E": 1710000000456u64,
            "s": "BTCUSDT",
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let book = parse_l2_book(&message, 1710000001.5).expect("book");

        match book {
            cryptofeed_orderbook::L2Book::Delta(delta) => {
                assert_eq!(delta.symbol.as_str(), "BTC-USDT");
                assert_eq!(delta.bids.len(), 1);
                assert_eq!(delta.asks.len(), 1);
                assert_eq!(
                    delta.bids[0].price,
                    Decimal::from_str_exact("64999.10").unwrap()
                );
                assert_eq!(
                    delta.asks[0].amount,
                    Decimal::from_str_exact("0.75").unwrap()
                );
            }
            cryptofeed_orderbook::L2Book::Snapshot(_) => panic!("expected delta"),
        }
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_binance_l2_book_update_ids() {
        let message = json!({
            "e": "depthUpdate",
            "E": 1710000000456u64,
            "s": "BTCUSDT",
            "U": 100,
            "u": 101,
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let update = parse_l2_book_update(&message, 1710000001.5).expect("update");
        assert_eq!(update.first_update_id, 100);
        assert_eq!(update.last_update_id, 101);
        assert_eq!(update.book.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_futures_previous_update_id() {
        let message = json!({
            "e": "depthUpdate", "E": 1710000000456u64, "s": "BTCUSD_PERP",
            "U": 100, "u": 101, "pu": 99,
            "b": [["64999.10", "1.25"]], "a": [["65000.20", "0.75"]]
        });
        let instrument = BinanceInstrument::new(
            cryptofeed_core::symbol::Symbol::perpetual("btc", "usd"),
            "BTCUSD_PERP",
            BinanceProduct::CoinM,
        );

        let update = parse_l2_book_update_for_instrument(&message, 1710000001.5, &instrument)
            .expect("update");
        assert_eq!(update.previous_update_id, Some(99));
        assert_eq!(update.delta.book.symbol.as_str(), "BTC-USD-PERP");
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_binance_l2_book_snapshot_response() {
        let message = json!({
            "lastUpdateId": 101u64,
            "bids": [["64999.10", "1.25"]],
            "asks": [["65000.20", "0.75"]]
        });

        let (last_update_id, snapshot) =
            parse_l2_book_snapshot(&message, "BTCUSDT", 1710000001.5).expect("snapshot");

        assert_eq!(last_update_id, 101);
        assert_eq!(snapshot.symbol.as_str(), "BTC-USDT");
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.asks.len(), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn snapshot_uses_resolved_derivative_instrument() {
        let message = json!({
            "lastUpdateId": 101u64,
            "E": 1710000000456u64,
            "bids": [["64999.10", "1.25"]],
            "asks": [["65000.20", "0.75"]]
        });
        let instrument = BinanceInstrument::new(
            cryptofeed_core::symbol::Symbol::perpetual("btc", "usd"),
            "BTCUSD_PERP",
            BinanceProduct::CoinM,
        );

        let (_, snapshot) =
            parse_l2_book_snapshot_for_instrument(&message, &instrument, 1710000001.5)
                .expect("snapshot");
        assert_eq!(snapshot.symbol.as_str(), "BTC-USD-PERP");
        assert_eq!(snapshot.exchange_ts, 1710000000.456);
    }
}
