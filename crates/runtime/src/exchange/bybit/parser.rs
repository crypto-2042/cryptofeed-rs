#[cfg(feature = "orderbook")]
use super::book_sync::{BybitBookAction, BybitDepthUpdate};
#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(any(feature = "trade", feature = "liquidations"))]
use cryptofeed_core::model::Side;
use cryptofeed_core::{
    exchange::ExchangeId,
    symbol::{InstrumentKind, Symbol},
};
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
use cryptofeed_orderbook::{L1Book, L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;
use rust_decimal::Decimal;
use serde_json::Value;

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    parse_tickers(message, received_ts).into_iter().next()
}

#[cfg(feature = "ticker")]
pub fn parse_tickers(message: &Value, received_ts: f64) -> Vec<Ticker> {
    data_rows(message)
        .into_iter()
        .filter_map(|data| parse_ticker_row(message, data, received_ts, None))
        .collect()
}

#[cfg(feature = "ticker")]
pub fn parse_tickers_for_symbol(message: &Value, received_ts: f64, symbol: &Symbol) -> Vec<Ticker> {
    data_rows(message)
        .into_iter()
        .filter_map(|data| parse_ticker_row(message, data, received_ts, Some(symbol)))
        .collect()
}

#[cfg(feature = "ticker")]
fn parse_ticker_row(
    message: &Value,
    data: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Option<Ticker> {
    let is_option = symbol.is_some_and(|symbol| symbol.kind() == InstrumentKind::Option)
        || data.get("bidPrice").is_some()
        || data.get("askPrice").is_some();
    let (bid_field, ask_field) = if is_option {
        ("bidPrice", "askPrice")
    } else {
        ("bid1Price", "ask1Price")
    };
    Some(Ticker {
        exchange: ExchangeId::Bybit,
        symbol: symbol.cloned().unwrap_or_else(|| {
            parse_symbol(
                data.get("symbol")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
        }),
        bid: parse_decimal(data.get(bid_field)?)?,
        ask: parse_decimal(data.get(ask_field)?)?,
        exchange_ts: message
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
        implied_volatility: data
            .get("markPriceIv")
            .or_else(|| data.get("iv"))
            .and_then(parse_decimal),
    })
}

#[cfg(feature = "ticker")]
pub fn parse_bbo_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    parse_bbo_ticker_with_symbol(message, received_ts, None)
}

#[cfg(feature = "ticker")]
pub fn parse_bbo_ticker_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<Ticker> {
    parse_bbo_ticker_with_symbol(message, received_ts, Some(symbol))
}

#[cfg(feature = "ticker")]
fn parse_bbo_ticker_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Option<Ticker> {
    let data = message.get("data")?;
    let bid = data.get("b")?.as_array()?.first()?.as_array()?.first()?;
    let ask = data.get("a")?.as_array()?.first()?.as_array()?.first()?;
    Some(Ticker {
        exchange: ExchangeId::Bybit,
        symbol: symbol.cloned().unwrap_or_else(|| {
            parse_symbol(data.get("s").and_then(Value::as_str).unwrap_or_default())
        }),
        bid: parse_decimal(bid)?,
        ask: parse_decimal(ask)?,
        exchange_ts: message
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
        implied_volatility: None,
    })
}

#[cfg(feature = "trade")]
pub fn parse_trade(message: &Value, received_ts: f64) -> Option<Trade> {
    parse_trades(message, received_ts).into_iter().next()
}

#[cfg(feature = "trade")]
pub fn parse_trades(message: &Value, received_ts: f64) -> Vec<Trade> {
    parse_trades_with_symbol(message, received_ts, None)
}

#[cfg(feature = "trade")]
pub fn parse_trades_for_symbol(message: &Value, received_ts: f64, symbol: &Symbol) -> Vec<Trade> {
    parse_trades_with_symbol(message, received_ts, Some(symbol))
}

/// Trades resolved per row through `resolve`: base-coin option streams
/// (`publicTrade.{base}`) cover every series of the base coin, so each
/// row's own `s` must determine its instrument. Rows whose symbol does not
/// resolve (series outside the feed) are skipped instead of mislabeled.
#[cfg(feature = "trade")]
pub fn parse_trades_for_rows<F>(message: &Value, received_ts: f64, resolve: F) -> Vec<Trade>
where
    F: Fn(&str) -> Option<Symbol>,
{
    message
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let native = row.get("s").and_then(Value::as_str)?;
            let symbol = resolve(native)?;
            parse_trade_row(row, received_ts, Some(&symbol))
        })
        .collect()
}

#[cfg(feature = "trade")]
fn parse_trades_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Vec<Trade> {
    message
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| parse_trade_row(row, received_ts, symbol))
        .collect()
}

#[cfg(feature = "trade")]
fn parse_trade_row(first: &Value, received_ts: f64, symbol: Option<&Symbol>) -> Option<Trade> {
    Some(Trade {
        exchange: ExchangeId::Bybit,
        symbol: symbol.cloned().unwrap_or_else(|| {
            parse_symbol(first.get("s").and_then(Value::as_str).unwrap_or_default())
        }),
        side: match first.get("S")?.as_str()? {
            "Sell" => Side::Sell,
            _ => Side::Buy,
        },
        amount: parse_decimal(first.get("v")?)?,
        price: parse_decimal(first.get("p")?)?,
        exchange_ts: first.get("T").and_then(parse_millis)?,
        received_ts,
        id: first
            .get("i")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned),
        // Option trades carry the implied volatility in `iv`.
        implied_volatility: first.get("iv").and_then(parse_decimal),
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_book(message: &Value, received_ts: f64) -> Option<L1Book> {
    parse_l1_book_for_symbol(message, received_ts, None)
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_book_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Option<L1Book> {
    let data = message.get("data")?;
    let symbol = symbol
        .cloned()
        .unwrap_or_else(|| parse_symbol(data.get("s").and_then(Value::as_str).unwrap_or_default()));
    let bids = parse_levels(data.get("b")?)?;
    let asks = parse_levels(data.get("a")?)?;
    Some(L1Book {
        exchange: ExchangeId::Bybit,
        symbol,
        bid: bids.first()?.clone(),
        ask: asks.first()?.clone(),
        exchange_ts: message
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    parse_l2_book_with_symbol(message, received_ts, None)
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<L2Book> {
    parse_l2_book_with_symbol(message, received_ts, Some(symbol))
}

#[cfg(feature = "orderbook")]
fn parse_l2_book_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Option<L2Book> {
    let data = message.get("data")?;
    let symbol = symbol
        .cloned()
        .unwrap_or_else(|| parse_symbol(data.get("s").and_then(Value::as_str).unwrap_or_default()));
    let bids = parse_levels(data.get("b")?)?;
    let asks = parse_levels(data.get("a")?)?;
    let exchange_ts = message
        .get("ts")
        .and_then(parse_millis)
        .unwrap_or(received_ts);
    let snapshot = message.get("type").and_then(Value::as_str) == Some("snapshot")
        || data.get("u").and_then(Value::as_u64) == Some(1);
    Some(if snapshot {
        L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Bybit,
            symbol,
            bids,
            asks,
            exchange_ts,
            received_ts,
        })
    } else {
        L2Book::Delta(L2BookDelta {
            exchange: ExchangeId::Bybit,
            symbol,
            bids,
            asks,
            exchange_ts,
            received_ts,
        })
    })
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update(message: &Value, received_ts: f64) -> Option<BybitDepthUpdate> {
    parse_l2_book_update_with_symbol(message, received_ts, None)
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Option<BybitDepthUpdate> {
    parse_l2_book_update_with_symbol(message, received_ts, Some(symbol))
}

#[cfg(feature = "orderbook")]
fn parse_l2_book_update_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Option<BybitDepthUpdate> {
    let data = message.get("data")?;
    Some(BybitDepthUpdate {
        action: match (message.get("type")?.as_str()?, data.get("u")?.as_u64()?) {
            ("snapshot", _) | (_, 1) => BybitBookAction::Snapshot,
            _ => BybitBookAction::Delta,
        },
        update_id: data.get("u")?.as_u64()?,
        seq: data.get("seq").and_then(|v| v.as_u64()),
        book: parse_l2_book_with_symbol(message, received_ts, symbol)?,
    })
}

#[cfg(feature = "candles")]
pub fn parse_candle(message: &Value, received_ts: f64) -> Option<Candle> {
    parse_candles(message, received_ts).into_iter().next()
}

#[cfg(feature = "candles")]
pub fn parse_candles(message: &Value, received_ts: f64) -> Vec<Candle> {
    parse_candles_with_symbol(message, received_ts, None)
}

#[cfg(feature = "candles")]
pub fn parse_candles_for_symbol(message: &Value, received_ts: f64, symbol: &Symbol) -> Vec<Candle> {
    parse_candles_with_symbol(message, received_ts, Some(symbol))
}

#[cfg(feature = "candles")]
fn parse_candles_with_symbol(
    message: &Value,
    received_ts: f64,
    normalized_symbol: Option<&Symbol>,
) -> Vec<Candle> {
    let Some(topic) = message.get("topic").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Some(raw_symbol) = topic.rsplit('.').next() else {
        return Vec::new();
    };
    let Some(rows) = message.get("data").and_then(Value::as_array) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            parse_candle_row(message, row, received_ts, raw_symbol, normalized_symbol)
        })
        .collect()
}

#[cfg(feature = "candles")]
fn parse_candle_row(
    message: &Value,
    first: &Value,
    received_ts: f64,
    raw_symbol: &str,
    normalized_symbol: Option<&Symbol>,
) -> Option<Candle> {
    Some(Candle {
        exchange: ExchangeId::Bybit,
        symbol: normalized_symbol
            .cloned()
            .unwrap_or_else(|| parse_symbol(raw_symbol)),
        start: first.get("start").and_then(parse_millis)?,
        end: first.get("end").and_then(parse_millis)?,
        interval: first.get("interval")?.as_str()?.to_owned(),
        trades: None,
        open: parse_decimal(first.get("open")?)?,
        close: parse_decimal(first.get("close")?)?,
        high: parse_decimal(first.get("high")?)?,
        low: parse_decimal(first.get("low")?)?,
        volume: parse_decimal(first.get("volume")?)?,
        closed: first.get("confirm").and_then(|v| v.as_bool()),
        exchange_ts: message
            .get("ts")
            .and_then(parse_millis)
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "funding")]
pub fn parse_funding(message: &Value, received_ts: f64) -> Option<Funding> {
    parse_fundings(message, received_ts).into_iter().next()
}

#[cfg(feature = "funding")]
pub fn parse_fundings(message: &Value, received_ts: f64) -> Vec<Funding> {
    data_rows(message)
        .into_iter()
        .filter_map(|data| parse_funding_row(message, data, received_ts, None))
        .collect()
}

#[cfg(feature = "funding")]
pub fn parse_fundings_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Vec<Funding> {
    data_rows(message)
        .into_iter()
        .filter_map(|data| parse_funding_row(message, data, received_ts, Some(symbol)))
        .collect()
}

#[cfg(feature = "funding")]
fn parse_funding_row(
    message: &Value,
    data: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Option<Funding> {
    // The tickers stream carries `fundingRate` only on instruments that
    // have funding; option tickers (which share the stream parse path) do
    // not. Gate on the rate so non-funding rows never emit a funding event.
    let rate = data.get("fundingRate").and_then(parse_decimal)?;
    Some(Funding {
        exchange: ExchangeId::Bybit,
        symbol: symbol.cloned().unwrap_or_else(|| {
            parse_symbol(
                data.get("symbol")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
        }),
        mark_price: data.get("markPrice").and_then(parse_decimal),
        rate: Some(rate),
        // The derivative `tickers.{symbol}` stream carries `nextFundingTime`
        // (ms string); the legacy `funding.{symbol}` channel is no longer
        // served (verified live 2026-08-06), so the tickers stream is the
        // only funding source.
        next_funding_time: data
            .get("nextFundingTime")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .map(|value| value as f64 / 1000.0),
        predicted_rate: None,
        exchange_ts: data
            .get("fundingRateTimestamp")
            .and_then(parse_millis)
            .or_else(|| message.get("ts").and_then(parse_millis))
            .unwrap_or(received_ts),
        received_ts,
    })
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest(message: &Value, received_ts: f64) -> Option<OpenInterest> {
    parse_open_interests(message, received_ts)
        .into_iter()
        .next()
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interests(message: &Value, received_ts: f64) -> Vec<OpenInterest> {
    parse_open_interests_with_symbol(message, received_ts, None)
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interests_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Vec<OpenInterest> {
    parse_open_interests_with_symbol(message, received_ts, Some(symbol))
}

#[cfg(feature = "openinterest")]
fn parse_open_interests_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Vec<OpenInterest> {
    // Bybit has no standalone open-interest channel: the linear/inverse
    // `tickers.{symbol}` stream carries `openInterest` and
    // `openInterestValue` fields.
    data_rows(message)
        .into_iter()
        .filter_map(|data| {
            Some(OpenInterest {
                exchange: ExchangeId::Bybit,
                symbol: symbol.cloned().unwrap_or_else(|| {
                    parse_symbol(
                        data.get("symbol")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                }),
                open_interest: parse_decimal(data.get("openInterest")?)?,
                coin_quantity: None,
                value_usd: data.get("openInterestValue").and_then(parse_decimal),
                exchange_ts: message
                    .get("ts")
                    .and_then(parse_millis)
                    .unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "index")]
pub fn parse_index_price(message: &Value, received_ts: f64) -> Option<IndexPrice> {
    parse_index_prices(message, received_ts).into_iter().next()
}

#[cfg(feature = "index")]
pub fn parse_index_prices(message: &Value, received_ts: f64) -> Vec<IndexPrice> {
    parse_index_prices_with_symbol(message, received_ts, None)
}

#[cfg(feature = "index")]
pub fn parse_index_prices_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Vec<IndexPrice> {
    parse_index_prices_with_symbol(message, received_ts, Some(symbol))
}

#[cfg(feature = "index")]
fn parse_index_prices_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Vec<IndexPrice> {
    // Bybit has no standalone index-price channel (current official docs
    // expose index price through the derivative `tickers.{symbol}` stream
    // only); the `indexPrice` field is extracted from the same stream that
    // carries the derivative ticker. The event symbol is the derivative
    // symbol itself (e.g. `BTC-USDT-PERP`), mirroring the open-interest
    // extraction convention.
    data_rows(message)
        .into_iter()
        .filter_map(|data| {
            Some(IndexPrice {
                exchange: ExchangeId::Bybit,
                symbol: symbol.cloned().unwrap_or_else(|| {
                    parse_symbol(
                        data.get("symbol")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                }),
                price: parse_decimal(data.get("indexPrice")?)?,
                open_24h: None,
                high_24h: None,
                low_24h: None,
                exchange_ts: message
                    .get("ts")
                    .and_then(parse_millis)
                    .unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "markprice")]
pub fn parse_mark_price(message: &Value, received_ts: f64) -> Option<MarkPrice> {
    parse_mark_prices(message, received_ts).into_iter().next()
}

#[cfg(feature = "markprice")]
pub fn parse_mark_prices(message: &Value, received_ts: f64) -> Vec<MarkPrice> {
    parse_mark_prices_with_symbol(message, received_ts, None)
}

#[cfg(feature = "markprice")]
pub fn parse_mark_prices_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Vec<MarkPrice> {
    parse_mark_prices_with_symbol(message, received_ts, Some(symbol))
}

#[cfg(feature = "markprice")]
fn parse_mark_prices_with_symbol(
    message: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Vec<MarkPrice> {
    // Bybit exposes no standalone mark-price channel: the derivative
    // `tickers.{symbol}` stream carries `markPrice` on every push. The event
    // symbol follows the derivative symbol, mirroring the index convention.
    data_rows(message)
        .into_iter()
        .filter_map(|data| {
            Some(MarkPrice {
                exchange: ExchangeId::Bybit,
                symbol: symbol.cloned().unwrap_or_else(|| {
                    parse_symbol(
                        data.get("symbol")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                }),
                price: parse_decimal(data.get("markPrice")?)?,
                next_funding_time: None,
                predicted_rate: None,
                exchange_ts: message
                    .get("ts")
                    .and_then(parse_millis)
                    .unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidation(message: &Value, received_ts: f64) -> Option<Liquidation> {
    parse_liquidations(message, received_ts).into_iter().next()
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidations(message: &Value, received_ts: f64) -> Vec<Liquidation> {
    data_rows(message)
        .into_iter()
        .filter_map(|data| parse_liquidation_row(message, data, received_ts, None))
        .collect()
}

#[cfg(feature = "liquidations")]
pub fn parse_liquidations_for_symbol(
    message: &Value,
    received_ts: f64,
    symbol: &Symbol,
) -> Vec<Liquidation> {
    data_rows(message)
        .into_iter()
        .filter_map(|data| parse_liquidation_row(message, data, received_ts, Some(symbol)))
        .collect()
}

#[cfg(feature = "liquidations")]
fn parse_liquidation_row(
    _message: &Value,
    data: &Value,
    received_ts: f64,
    symbol: Option<&Symbol>,
) -> Option<Liquidation> {
    Some(Liquidation {
        exchange: ExchangeId::Bybit,
        symbol: symbol.cloned().unwrap_or_else(|| {
            parse_symbol(data.get("s").and_then(Value::as_str).unwrap_or_default())
        }),
        // `S` is the liquidated position side, not the liquidation order
        // side. Normalize it to the aggressor side exposed by the SDK.
        side: match data.get("S")?.as_str()? {
            "Buy" => Side::Sell,
            "Sell" => Side::Buy,
            _ => return None,
        },
        quantity: parse_decimal(data.get("v")?)?,
        price: parse_decimal(data.get("p")?)?,
        // `allLiquidation.{symbol}` carries no order id or fill status.
        id: None,
        status: LiquidationStatus::Filled,
        exchange_ts: data.get("T").and_then(parse_millis).unwrap_or(received_ts),
        received_ts,
    })
}

fn data_rows(message: &Value) -> Vec<&Value> {
    match message.get("data") {
        Some(Value::Array(rows)) => rows.iter().collect(),
        Some(row @ Value::Object(_)) => vec![row],
        _ => Vec::new(),
    }
}

fn parse_symbol(raw: &str) -> Symbol {
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if let Some(base) = raw.strip_suffix(quote).filter(|base| !base.is_empty()) {
            return Symbol::spot(base, quote);
        }
    }
    Symbol::from_input(raw)
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    Decimal::from_str_exact(value.as_str()?).ok()
}

#[cfg(feature = "orderbook")]
fn parse_levels(value: &Value) -> Option<Vec<PriceLevel>> {
    value
        .as_array()?
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
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
        .map(|v| v / 1000.0)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[cfg(feature = "candles")]
    use super::parse_candle;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book_update;
    #[cfg(feature = "ticker")]
    use super::parse_ticker;
    #[cfg(feature = "trade")]
    use super::parse_trade;
    #[cfg(feature = "orderbook")]
    use cryptofeed_orderbook::L2Book;

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_bybit_ticker_message() {
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

        let ticker = parse_ticker(&message, 1672304487.0).expect("ticker");
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_bybit_spot_bbo_from_level_one_orderbook() {
        let message = json!({
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "0.001"]],
                "a": [["16579.00", "0.002"]],
                "u": 123,
                "seq": 456
            }
        });

        let ticker = super::parse_bbo_ticker(&message, 1672304487.0).expect("ticker");
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
        assert_eq!(ticker.bid.to_string(), "16578.50");
        assert_eq!(ticker.ask.to_string(), "16579.00");
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn does_not_parse_spot_24h_statistics_without_bbo_as_ticker() {
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

        assert!(super::parse_ticker(&message, 1672304487.0).is_none());
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_bybit_trade_message() {
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

        let trade = parse_trade(&message, 1672304487.0).expect("trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_every_bybit_trade_in_a_batch() {
        let message = json!({
            "topic": "publicTrade.BTCUSDT",
            "ts": 1672304486868i64,
            "data": [
                {"T": 1672304486865i64, "s": "BTCUSDT", "S": "Buy", "v": "0.001", "p": "1", "i": "a"},
                {"T": 1672304486866i64, "s": "BTCUSDT", "S": "Sell", "v": "0.002", "p": "2", "i": "b"}
            ]
        });

        let trades = super::parse_trades(&message, 1672304487.0);
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id.as_deref(), Some("a"));
        assert_eq!(trades[1].id.as_deref(), Some("b"));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_bybit_l2_book_message() {
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

        let book = parse_l2_book(&message, 1672304485.0).expect("book");
        match book {
            L2Book::Snapshot(snapshot) => assert_eq!(snapshot.symbol.as_str(), "BTC-USDT"),
            L2Book::Delta(_) => panic!("expected snapshot event model"),
        }
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_bybit_l2_book_update_ids() {
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

        let update = parse_l2_book_update(&message, 1672304485.0).expect("update");
        assert_eq!(update.update_id, 18521288);
        assert_eq!(update.seq, Some(7961638724));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn treats_update_id_one_as_a_restart_snapshot() {
        let message = json!({
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "ts": 1672304484978i64,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50", "0.006"]],
                "a": [["16493.60", "0.100"]],
                "u": 1u64,
                "seq": 7961638724u64
            }
        });

        let update = parse_l2_book_update(&message, 1672304485.0).expect("update");
        assert!(matches!(update.action, super::BybitBookAction::Snapshot));
        assert!(matches!(update.book, L2Book::Snapshot(_)));
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_bybit_candle_message() {
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

        let candle = parse_candle(&message, 1672324989.0).expect("candle");
        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
        assert_eq!(candle.interval, "1");
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_every_bybit_candle_in_a_batch() {
        let message = json!({
            "topic": "kline.1.BTCUSDT",
            "ts": 1672324988882i64,
            "data": [
                {"start": 1672324800000i64, "end": 1672324859999i64, "interval": "1", "open": "1", "close": "2", "high": "3", "low": "0.5", "volume": "4", "confirm": true},
                {"start": 1672324860000i64, "end": 1672324919999i64, "interval": "1", "open": "2", "close": "3", "high": "4", "low": "1", "volume": "5", "confirm": false}
            ]
        });

        let candles = super::parse_candles(&message, 1672324989.0);
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].start, 1672324800.0);
        assert_eq!(candles[1].start, 1672324860.0);
    }
}
