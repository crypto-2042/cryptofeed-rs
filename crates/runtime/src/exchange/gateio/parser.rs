#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(any(feature = "liquidations", feature = "trade"))]
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
use cryptofeed_orderbook::{L1Book, L2Book, L2BookDelta, L2BookSnapshot, PriceLevel};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;
use rust_decimal::Decimal;
use serde_json::Value;

use super::adapter::GateioInstrument;
#[cfg(feature = "orderbook")]
use super::book_sync::{GateioBookSnapshot, GateioBookUpdate, GateioDepthDelta};

#[cfg(feature = "liquidations")]
pub fn parse_liquidations_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<Liquidation> {
    if message.get("channel").and_then(Value::as_str) != Some("futures.public_liquidates") {
        return Vec::new();
    }
    result_rows(message)
        .into_iter()
        .filter_map(|row| {
            let amount = parse_decimal(row.get("size")?)?;
            if amount.is_zero() {
                return None;
            }
            Some(Liquidation {
                exchange: ExchangeId::Gateio,
                symbol: resolved_or_parsed_symbol(
                    row.get("contract")?.as_str()?,
                    Some("futures.public_liquidates"),
                    instrument,
                )?,
                side: if amount.is_sign_negative() {
                    Side::Sell
                } else {
                    Side::Buy
                },
                quantity: amount.abs(),
                price: parse_decimal(row.get("price")?)?,
                id: None,
                status: LiquidationStatus::Filled,
                exchange_ts: row.get("time_ms").and_then(parse_gateio_timestamp)?,
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "ticker")]
pub fn parse_ticker(message: &Value, received_ts: f64) -> Option<Ticker> {
    parse_tickers(message, received_ts).into_iter().next()
}

#[cfg(feature = "ticker")]
pub fn parse_tickers(message: &Value, received_ts: f64) -> Vec<Ticker> {
    parse_tickers_with_symbol(message, received_ts, None)
}

#[cfg(feature = "ticker")]
pub fn parse_tickers_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<Ticker> {
    parse_tickers_with_symbol(message, received_ts, Some(instrument))
}

#[cfg(feature = "ticker")]
fn parse_tickers_with_symbol(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<Ticker> {
    result_rows(message)
        .into_iter()
        .filter_map(|result| {
            let native = result
                .get("s")
                .or_else(|| result.get("contract"))?
                .as_str()?;
            let symbol = resolved_or_parsed_symbol(
                native,
                message.get("channel").and_then(Value::as_str),
                instrument,
            )?;
            Some(Ticker {
                exchange: ExchangeId::Gateio,
                symbol,
                bid: parse_decimal(result.get("b")?)?,
                ask: parse_decimal(result.get("a")?)?,
                exchange_ts: result
                    .get("t")
                    .and_then(parse_gateio_timestamp)
                    .unwrap_or(received_ts),
                received_ts,
                implied_volatility: None,
            })
        })
        .collect()
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_book(message: &Value, received_ts: f64) -> Option<L1Book> {
    parse_l1_books(message, received_ts).into_iter().next()
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_books(message: &Value, received_ts: f64) -> Vec<L1Book> {
    parse_l1_books_with_instrument(message, received_ts, None)
}

#[cfg(feature = "orderbook")]
pub fn parse_l1_books_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<L1Book> {
    parse_l1_books_with_instrument(message, received_ts, Some(instrument))
}

#[cfg(feature = "orderbook")]
fn parse_l1_books_with_instrument(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<L1Book> {
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|result| {
            // The spot and derivative `book_ticker` streams carry the best
            // bid/ask with sizes (`b`/`B`/`a`/`A`), doubling as the L1
            // top-of-book channel (same event as the BBO Ticker source).
            Some(L1Book {
                exchange: ExchangeId::Gateio,
                symbol: resolved_or_parsed_symbol(result.get("s")?.as_str()?, channel, instrument)?,
                bid: PriceLevel {
                    price: parse_decimal(result.get("b")?)?,
                    amount: parse_decimal(result.get("B")?)?,
                },
                ask: PriceLevel {
                    price: parse_decimal(result.get("a")?)?,
                    amount: parse_decimal(result.get("A")?)?,
                },
                exchange_ts: result
                    .get("t")
                    .and_then(parse_gateio_timestamp)
                    .unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
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
pub fn parse_trades_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<Trade> {
    parse_trades_with_symbol(message, received_ts, Some(instrument))
}

#[cfg(feature = "trade")]
fn parse_trades_with_symbol(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<Trade> {
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|row| parse_trade_row(row, received_ts, channel, instrument))
        .collect()
}

#[cfg(feature = "trade")]
fn parse_trade_row(
    first: &Value,
    received_ts: f64,
    channel: Option<&str>,
    instrument: Option<&GateioInstrument>,
) -> Option<Trade> {
    let futures = channel.is_some_and(|value| value.starts_with("futures."));
    let signed_size = futures.then(|| parse_decimal(first.get("size")?)).flatten();
    Some(Trade {
        exchange: ExchangeId::Gateio,
        symbol: resolved_or_parsed_symbol(
            first
                .get("currency_pair")
                .or_else(|| first.get("contract"))?
                .as_str()?,
            channel,
            instrument,
        )?,
        side: if futures {
            if signed_size? < Decimal::ZERO {
                Side::Sell
            } else {
                Side::Buy
            }
        } else {
            match first.get("side")?.as_str()? {
                "sell" => Side::Sell,
                _ => Side::Buy,
            }
        },
        amount: if futures {
            signed_size?.abs()
        } else {
            parse_decimal(first.get("amount")?)?
        },
        price: parse_decimal(first.get("price")?)?,
        exchange_ts: first
            .get("create_time_ms")
            .and_then(parse_millis)
            .or_else(|| first.get("create_time").and_then(parse_seconds))?,
        received_ts,
        id: first.get("id").and_then(value_to_string),
        implied_volatility: None,
    })
}

#[cfg(feature = "funding")]
pub fn parse_funding(message: &Value, received_ts: f64) -> Option<Funding> {
    parse_fundings(message, received_ts).into_iter().next()
}

#[cfg(feature = "funding")]
pub fn parse_fundings(message: &Value, received_ts: f64) -> Vec<Funding> {
    parse_fundings_with_instrument(message, received_ts, None)
}

#[cfg(feature = "funding")]
pub fn parse_fundings_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<Funding> {
    parse_fundings_with_instrument(message, received_ts, Some(instrument))
}

#[cfg(feature = "funding")]
fn parse_fundings_with_instrument(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<Funding> {
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|row| {
            // The derivative `tickers` stream carries `funding_rate` only on
            // contracts that have funding; delivery contracts do not. Gate on
            // the rate so non-funding rows never emit a funding event.
            let rate = parse_decimal(row.get("funding_rate")?)?;
            Some(Funding {
                exchange: ExchangeId::Gateio,
                symbol: resolved_or_parsed_symbol(
                    row.get("contract")?.as_str()?,
                    channel,
                    instrument,
                )?,
                mark_price: row.get("mark_price").and_then(parse_decimal),
                rate: Some(rate),
                // Gate.io exposes no funding-time field on the derivative
                // tickers stream; the normalized model keeps the applicable
                // time unset.
                next_funding_time: None,
                predicted_rate: None,
                exchange_ts: row
                    .get("time")
                    .and_then(parse_millis)
                    .unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interest(message: &Value, received_ts: f64) -> Option<OpenInterest> {
    parse_open_interests(message, received_ts)
        .into_iter()
        .next()
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interests(message: &Value, received_ts: f64) -> Vec<OpenInterest> {
    parse_open_interests_with_instrument(message, received_ts, None)
}

#[cfg(feature = "openinterest")]
pub fn parse_open_interests_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<OpenInterest> {
    parse_open_interests_with_instrument(message, received_ts, Some(instrument))
}

#[cfg(feature = "openinterest")]
fn parse_open_interests_with_instrument(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<OpenInterest> {
    // Gate.io has no standalone ticker-level open-interest channel: the
    // derivative `tickers` stream exposes current contract `total_size`.
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|row| {
            Some(OpenInterest {
                exchange: ExchangeId::Gateio,
                symbol: resolved_or_parsed_symbol(
                    row.get("contract")?.as_str()?,
                    channel,
                    instrument,
                )?,
                open_interest: parse_decimal(
                    row.get("total_size").or_else(|| row.get("open_interest"))?,
                )?,
                coin_quantity: None,
                value_usd: None,
                exchange_ts: row
                    .get("time")
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
    parse_index_prices_with_instrument(message, received_ts, None)
}

#[cfg(feature = "index")]
pub fn parse_index_prices_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<IndexPrice> {
    parse_index_prices_with_instrument(message, received_ts, Some(instrument))
}

#[cfg(feature = "index")]
fn parse_index_prices_with_instrument(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<IndexPrice> {
    // Gate.io carries the index price inside the derivative `tickers`
    // stream; there is no standalone index-price channel. The event symbol
    // is the derivative symbol itself (e.g. `BTC-USDT-PERP`), mirroring the
    // open-interest extraction convention.
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|row| {
            Some(IndexPrice {
                exchange: ExchangeId::Gateio,
                symbol: resolved_or_parsed_symbol(
                    row.get("contract")?.as_str()?,
                    channel,
                    instrument,
                )?,
                price: parse_decimal(row.get("index_price")?)?,
                open_24h: None,
                high_24h: None,
                low_24h: None,
                exchange_ts: row
                    .get("time")
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
    parse_mark_prices_with_instrument(message, received_ts, None)
}

#[cfg(feature = "markprice")]
pub fn parse_mark_prices_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<MarkPrice> {
    parse_mark_prices_with_instrument(message, received_ts, Some(instrument))
}

#[cfg(feature = "markprice")]
fn parse_mark_prices_with_instrument(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<MarkPrice> {
    // Gate.io carries the mark price inside the derivative `tickers`
    // stream; there is no standalone mark-price channel. The event symbol is
    // the derivative symbol itself, mirroring the funding extraction.
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|row| {
            Some(MarkPrice {
                exchange: ExchangeId::Gateio,
                symbol: resolved_or_parsed_symbol(
                    row.get("contract")?.as_str()?,
                    channel,
                    instrument,
                )?,
                price: parse_decimal(row.get("mark_price")?)?,
                // The derivative tickers stream carries no funding-time or
                // predicted-rate fields.
                next_funding_time: None,
                predicted_rate: None,
                exchange_ts: row
                    .get("time")
                    .and_then(parse_millis)
                    .unwrap_or(received_ts),
                received_ts,
            })
        })
        .collect()
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book(message: &Value, received_ts: f64) -> Option<L2Book> {
    parse_l2_books(message, received_ts).into_iter().next()
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_books(message: &Value, received_ts: f64) -> Vec<L2Book> {
    parse_l2_books_with_symbol(message, received_ts, None)
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_books_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<L2Book> {
    parse_l2_books_with_symbol(message, received_ts, Some(instrument))
}

#[cfg(feature = "orderbook")]
fn parse_l2_books_with_symbol(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<L2Book> {
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|result| parse_book_row(result, received_ts, channel, instrument))
        .collect()
}

#[cfg(feature = "orderbook")]
fn parse_book_row(
    result: &Value,
    received_ts: f64,
    channel: Option<&str>,
    instrument: Option<&GateioInstrument>,
) -> Option<L2Book> {
    let symbol = resolved_or_parsed_symbol(result.get("s")?.as_str()?, channel, instrument)?;
    let bids = parse_levels(result.get("b")?)?;
    let asks = parse_levels(result.get("a")?)?;
    let exchange_ts = result
        .get("t")
        .and_then(parse_gateio_book_ts)
        .unwrap_or(received_ts);
    if result.get("full").and_then(Value::as_bool) == Some(true) {
        return Some(L2Book::Snapshot(L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol,
            bids,
            asks,
            exchange_ts,
            received_ts,
        }));
    }
    Some(L2Book::Delta(L2BookDelta {
        exchange: ExchangeId::Gateio,
        symbol,
        bids,
        asks,
        exchange_ts,
        received_ts,
    }))
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update(message: &Value, received_ts: f64) -> Option<GateioBookUpdate> {
    parse_l2_book_update_inner(message, received_ts, None)
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_update_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Option<GateioBookUpdate> {
    parse_l2_book_update_inner(message, received_ts, Some(instrument))
}

#[cfg(feature = "orderbook")]
fn parse_l2_book_update_inner(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Option<GateioBookUpdate> {
    let result = message.get("result")?;
    let parsed = match instrument {
        Some(instrument) => parse_l2_books_for_instrument(message, received_ts, instrument),
        None => parse_l2_books(message, received_ts),
    };
    let book = parsed.into_iter().next()?;
    match book {
        L2Book::Delta(delta) => Some(GateioBookUpdate::Delta(GateioDepthDelta {
            first_update_id: parse_u64(result.get("U")?)?,
            last_update_id: parse_u64(result.get("u")?)?,
            ts: result.get("t").and_then(parse_gateio_timestamp)?,
            book: delta,
        })),
        // `full: true` pushes carry the complete book; the sequence anchor is
        // the push's own `u`.
        L2Book::Snapshot(snapshot) => Some(GateioBookUpdate::Full {
            snapshot,
            last_update_id: parse_u64(result.get("u")?)?,
        }),
    }
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_snapshot(
    message: &Value,
    exchange_symbol: &str,
    received_ts: f64,
) -> Option<GateioBookSnapshot> {
    let instrument = GateioInstrument::new(
        parse_symbol(exchange_symbol, None),
        exchange_symbol,
        super::adapter::GateioProduct::Spot,
    );
    let snapshot = parse_l2_book_snapshot_for_instrument(message, &instrument, received_ts)?;
    Some(snapshot)
}

#[cfg(feature = "orderbook")]
pub fn parse_l2_book_snapshot_for_instrument(
    message: &Value,
    instrument: &GateioInstrument,
    received_ts: f64,
) -> Option<GateioBookSnapshot> {
    let last_update_id = message.get("id").and_then(parse_u64);
    let generated_ts = message
        .get("current")
        .and_then(parse_gateio_timestamp)
        .unwrap_or(received_ts);
    let update_ts = message
        .get("update")
        .and_then(parse_gateio_timestamp)
        .unwrap_or(generated_ts);
    Some(GateioBookSnapshot {
        last_update_id,
        generated_ts,
        update_ts,
        book: L2BookSnapshot {
            exchange: ExchangeId::Gateio,
            symbol: instrument.symbol.clone(),
            bids: parse_levels(message.get("bids")?)?,
            asks: parse_levels(message.get("asks")?)?,
            exchange_ts: update_ts,
            received_ts,
        },
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
pub fn parse_candles_for_instrument(
    message: &Value,
    received_ts: f64,
    instrument: &GateioInstrument,
) -> Vec<Candle> {
    parse_candles_with_symbol(message, received_ts, Some(instrument))
}

#[cfg(feature = "candles")]
fn parse_candles_with_symbol(
    message: &Value,
    received_ts: f64,
    instrument: Option<&GateioInstrument>,
) -> Vec<Candle> {
    let channel = message.get("channel").and_then(Value::as_str);
    result_rows(message)
        .into_iter()
        .filter_map(|result| parse_candle_row(result, received_ts, channel, instrument))
        .collect()
}

#[cfg(feature = "candles")]
fn parse_candle_row(
    result: &Value,
    received_ts: f64,
    channel: Option<&str>,
    instrument: Option<&GateioInstrument>,
) -> Option<Candle> {
    let name = result.get("n")?.as_str()?;
    let (interval, symbol) = name.split_once('_')?;
    let start = result.get("t").and_then(parse_seconds)?;

    Some(Candle {
        exchange: ExchangeId::Gateio,
        symbol: resolved_or_parsed_symbol(symbol, channel, instrument)?,
        start,
        end: start + candle_duration_seconds(interval),
        interval: interval.to_owned(),
        trades: None,
        open: parse_decimal(result.get("o")?)?,
        close: parse_decimal(result.get("c")?)?,
        high: parse_decimal(result.get("h")?)?,
        low: parse_decimal(result.get("l")?)?,
        volume: parse_decimal(result.get("v")?)?,
        closed: result.get("w").and_then(|v| v.as_bool()),
        exchange_ts: start,
        received_ts,
    })
}

/// Bar duration in seconds for the Gate.io wire interval vocabulary (`10s`,
/// `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `8h`, `24h`/`1d`, `7d`/`1w`,
/// `30d`/`1M`). Unknown intervals fall back to one minute so a malformed
/// name still yields a closed range.
#[cfg(feature = "candles")]
fn candle_duration_seconds(interval: &str) -> f64 {
    if let Some(seconds) = interval
        .strip_suffix('s')
        .and_then(|value| value.parse::<f64>().ok())
    {
        return seconds;
    }
    match interval {
        "1m" => 60.0,
        "5m" => 300.0,
        "15m" => 900.0,
        "30m" => 1800.0,
        "1h" => 3600.0,
        "4h" => 14_400.0,
        "8h" => 28_800.0,
        "24h" | "1d" => 86_400.0,
        "7d" | "1w" => 604_800.0,
        "30d" | "1M" => 2_592_000.0,
        _ => 60.0,
    }
}

/// Parses a native Gate.io symbol (`BTC_USDT`, `BTC_USDT_20260925`, ...)
/// into a normalized symbol. Malformed dash-less input yields an `Unknown`
/// symbol instead of panicking on array indexing.
fn parse_symbol(raw: &str, channel: Option<&str>) -> Symbol {
    let normalized = raw.replace('_', "-");
    let parts: Vec<_> = normalized.split('-').collect();
    let Some(base) = parts.first().filter(|part| !part.is_empty()) else {
        return Symbol::from_input(&normalized);
    };
    let Some(quote) = parts.get(1) else {
        return Symbol::from_input(&normalized);
    };
    if channel.is_some_and(|value| value.starts_with("futures.")) {
        if let Some(expiry) = parts.get(2) {
            Symbol::futures(base, quote, expiry)
        } else {
            Symbol::perpetual(base, quote)
        }
    } else {
        Symbol::spot(base, quote)
    }
}

fn resolved_or_parsed_symbol(
    raw: &str,
    channel: Option<&str>,
    instrument: Option<&GateioInstrument>,
) -> Option<Symbol> {
    if let Some(instrument) = instrument {
        if !instrument.exchange_symbol.eq_ignore_ascii_case(raw) {
            return None;
        }
        Some(instrument.symbol.clone())
    } else {
        Some(parse_symbol(raw, channel))
    }
}

fn parse_decimal(value: &Value) -> Option<Decimal> {
    if let Some(value) = value.as_str() {
        Decimal::from_str_exact(value).ok()
    } else {
        Decimal::from_str_exact(&value.to_string()).ok()
    }
}

#[cfg(feature = "orderbook")]
fn parse_levels(value: &Value) -> Option<Vec<PriceLevel>> {
    value
        .as_array()?
        .iter()
        .map(|level| {
            if let Some(pair) = level.as_array() {
                return Some(PriceLevel {
                    price: parse_decimal(pair.first()?)?,
                    amount: parse_decimal(pair.get(1)?)?,
                });
            }
            Some(PriceLevel {
                price: parse_decimal(level.get("p")?)?,
                amount: parse_decimal(level.get("s")?)?,
            })
        })
        .collect()
}

fn result_rows(message: &Value) -> Vec<&Value> {
    match message.get("result") {
        Some(Value::Array(rows)) => rows.iter().collect(),
        Some(Value::Object(_)) => vec![&message["result"]],
        _ => Vec::new(),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    value.as_str().map(ToOwned::to_owned).or_else(|| {
        if value.is_number() {
            Some(value.to_string())
        } else {
            None
        }
    })
}

fn parse_millis(value: &Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|v| v.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .or_else(|| value.as_i64().map(|v| v as f64))
        .map(|v| v / 1000.0)
}

fn parse_seconds(value: &Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|v| v.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .or_else(|| value.as_i64().map(|v| v as f64))
}

fn parse_gateio_timestamp(value: &Value) -> Option<f64> {
    let raw = parse_seconds(value)?;
    if raw >= 10_000_000_000.0 {
        Some(raw / 1000.0)
    } else {
        Some(raw)
    }
}

#[cfg(feature = "orderbook")]
fn parse_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|v| v.parse().ok()))
}

#[cfg(feature = "orderbook")]
fn parse_gateio_book_ts(value: &Value) -> Option<f64> {
    parse_gateio_timestamp(value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[cfg(feature = "candles")]
    use super::parse_candle;
    #[cfg(feature = "funding")]
    use super::parse_funding;
    #[cfg(feature = "index")]
    use super::parse_index_price;
    #[cfg(feature = "orderbook")]
    use super::parse_l1_book;
    #[cfg(feature = "orderbook")]
    use super::parse_l2_book_snapshot_for_instrument;
    #[cfg(feature = "markprice")]
    use super::parse_mark_price;
    #[cfg(feature = "openinterest")]
    use super::parse_open_interest;
    #[cfg(feature = "ticker")]
    use super::parse_ticker;
    #[cfg(feature = "trade")]
    use super::parse_trade;
    #[cfg(feature = "orderbook")]
    use super::{parse_l2_book, parse_l2_book_snapshot, parse_l2_book_update};
    use crate::exchange::gateio::adapter::{GateioInstrument, GateioProduct};
    #[cfg(feature = "orderbook")]
    use crate::exchange::gateio::book_sync::GateioBookUpdate;
    #[cfg(feature = "orderbook")]
    use cryptofeed_orderbook::L2Book;

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_gateio_ticker_message() {
        let message = json!({
            "channel": "spot.book_ticker",
            "event": "update",
            "result": {"s": "BTC_USDT", "b": "64999.10", "a": "65000.20", "t": 1710000000}
        });
        let ticker = parse_ticker(&message, 1710000001.5).expect("ticker");
        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_gateio_trade_message() {
        let message = json!({
            "channel": "spot.trades",
            "event": "update",
            "result": {
                "id": 1,
                "id_market": 99,
                "currency_pair": "BTC_USDT",
                "price": "65000.50",
                "amount": "0.0100",
                "side": "buy",
                "create_time_ms": "1710000000123.456"
            }
        });
        let trade = parse_trade(&message, 1710000001.5).expect("trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
        assert_eq!(trade.id.as_deref(), Some("1"));
        assert_eq!(trade.exchange_ts, 1710000000.123456);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_gateio_l2_book_message() {
        let message = json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "s": "BTC_USDT",
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "t": 1710000000,
                "full": true
            }
        });
        let book = parse_l2_book(&message, 1710000001.5).expect("book");
        match book {
            L2Book::Snapshot(snapshot) => assert_eq!(snapshot.symbol.as_str(), "BTC-USDT"),
            L2Book::Delta(_) => panic!("expected snapshot event model"),
        }
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_gateio_futures_trade_shape() {
        let message = json!({
            "channel": "futures.trades",
            "event": "update",
            "result": [{
                "id": 27753479,
                "contract": "BTC_USDT",
                "price": "96.4",
                "size": -108,
                "create_time_ms": 1545136464123u64
            }]
        });

        let trade = parse_trade(&message, 1545136465.0).expect("futures trade");
        assert_eq!(trade.symbol.as_str(), "BTC-USDT-PERP");
        assert!(matches!(trade.side, cryptofeed_trade::model::Side::Sell));
        assert_eq!(trade.amount.to_string(), "108");
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_gateio_l2_book_update_ids() {
        let message = json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "s": "BTC_USDT",
                "U": 100u64,
                "u": 101u64,
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "t": 1710000000456u64
            }
        });

        let update = parse_l2_book_update(&message, 1710000001.5).expect("book update");
        let delta = match update {
            GateioBookUpdate::Delta(delta) => delta,
            _ => panic!("expected delta update"),
        };

        assert_eq!(delta.first_update_id, 100);
        assert_eq!(delta.last_update_id, 101);
        assert_eq!(delta.book.symbol.as_str(), "BTC-USDT");
        assert_eq!(delta.book.exchange_ts, 1710000000.456);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_gateio_full_push_as_replacement_snapshot() {
        let message = json!({
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "t": 1710000000456u64,
                "U": 100u64,
                "u": 120u64,
                "s": "BTC_USDT",
                "b": [["64999.10", "1.25"]],
                "a": [["65000.20", "0.75"]],
                "full": true
            }
        });

        let update = parse_l2_book_update(&message, 1710000001.5).expect("full push");
        let (snapshot, last_update_id) = match update {
            GateioBookUpdate::Full {
                snapshot,
                last_update_id,
            } => (snapshot, last_update_id),
            _ => panic!("expected full push"),
        };

        assert_eq!(last_update_id, 120);
        assert_eq!(snapshot.symbol.as_str(), "BTC-USDT");
        assert_eq!(snapshot.bids[0].price.to_string(), "64999.10");
        assert_eq!(snapshot.asks[0].amount.to_string(), "0.75");
        assert_eq!(snapshot.exchange_ts, 1710000000.456);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_gateio_l2_book_snapshot_response() {
        let message = json!({
            "id": 100u64,
            "current": 1710000000.456,
            "bids": [["64999.10", "1.25"]],
            "asks": [["65000.20", "0.75"]]
        });

        let snapshot =
            parse_l2_book_snapshot(&message, "BTC_USDT", 1710000001.5).expect("snapshot");

        assert_eq!(snapshot.last_update_id, Some(100));
        assert_eq!(snapshot.book.symbol.as_str(), "BTC-USDT");
        assert_eq!(snapshot.book.bids.len(), 1);
        assert_eq!(snapshot.book.asks.len(), 1);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_perpetual_snapshot_metadata_with_resolved_identity() {
        let message = json!({
            "id": 100u64,
            "current": 1710000000.456,
            "update": 1710000000.123,
            "bids": [{"p": "64999.10", "s": "2"}],
            "asks": [{"p": "65000.20", "s": "3"}]
        });
        let instrument = GateioInstrument::new(
            cryptofeed_core::symbol::Symbol::perpetual("btc", "usdt"),
            "BTC_USDT",
            GateioProduct::UsdtPerpetual,
        );

        let snapshot = parse_l2_book_snapshot_for_instrument(&message, &instrument, 1710000001.5)
            .expect("snapshot");

        assert_eq!(snapshot.last_update_id, Some(100));
        assert_eq!(snapshot.generated_ts, 1710000000.456);
        assert_eq!(snapshot.update_ts, 1710000000.123);
        assert_eq!(snapshot.book.symbol.as_str(), "BTC-USDT-PERP");
    }

    #[cfg(feature = "trade")]
    #[test]
    fn resolved_delivery_instrument_preserves_expiry() {
        let message = json!({
            "channel": "futures.trades",
            "event": "update",
            "result": [{
                "id": 1,
                "contract": "BTC_USDT_20260925",
                "price": "65000",
                "size": 2,
                "create_time_ms": 1710000000123u64
            }]
        });
        let instrument = GateioInstrument::new(
            cryptofeed_core::symbol::Symbol::futures("btc", "usdt", "20260925"),
            "BTC_USDT_20260925",
            GateioProduct::UsdtDelivery,
        );

        let trades = super::parse_trades_for_instrument(&message, 1710000001.5, &instrument);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].symbol.as_str(), "BTC-USDT-20260925");
    }

    #[cfg(feature = "candles")]
    #[test]
    fn parses_gateio_candle_message() {
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
        let candle = parse_candle(&message, 1710000061.0).expect("candle");
        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
    }

    #[cfg(feature = "funding")]
    #[test]
    fn parses_gateio_derivative_ticker_funding() {
        let message = json!({
            "channel": "futures.tickers",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "last": "65102.1",
                "change_percentage": "0.21",
                "mark_price": "65103.4",
                "funding_rate": "0.000061",
                "index_price": "65103.1",
                "total_size": "2334747.0",
                "time": 1710000000000u64
            }]
        });

        let funding = parse_funding(&message, 1710000001.0).expect("funding");

        assert_eq!(funding.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(
            funding.rate,
            Some(rust_decimal::Decimal::from_str_exact("0.000061").unwrap())
        );
        assert_eq!(
            funding.mark_price,
            Some(rust_decimal::Decimal::from_str_exact("65103.4").unwrap())
        );
        assert_eq!(funding.next_funding_time, None);
        assert_eq!(funding.exchange_ts, 1710000000.0);
    }

    #[cfg(feature = "funding")]
    #[test]
    fn funding_row_without_rate_emits_nothing() {
        let message = json!({
            "channel": "futures.tickers",
            "event": "update",
            "result": [{"contract": "BTC_USDT_20260925", "last": "65000", "time": 1710000000000u64}]
        });

        assert!(parse_funding(&message, 1710000001.0).is_none());
    }

    #[cfg(feature = "openinterest")]
    #[test]
    fn parses_gateio_derivative_ticker_open_interest() {
        let message = json!({
            "channel": "futures.tickers",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "last": "65102.1",
                "total_size": "2334747.0",
                "time": 1710000000000u64
            }]
        });

        let oi = parse_open_interest(&message, 1710000001.0).expect("open interest");

        assert_eq!(oi.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(
            oi.open_interest,
            rust_decimal::Decimal::from_str_exact("2334747.0").unwrap()
        );
        assert_eq!(oi.value_usd, None);
        assert_eq!(oi.exchange_ts, 1710000000.0);
    }

    #[cfg(feature = "index")]
    #[test]
    fn parses_gateio_derivative_ticker_index_price() {
        let message = json!({
            "channel": "futures.tickers",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "last": "65102.1",
                "index_price": "65103.1",
                "time": 1710000000000u64
            }]
        });

        let index = parse_index_price(&message, 1710000001.0).expect("index price");

        assert_eq!(index.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(
            index.price,
            rust_decimal::Decimal::from_str_exact("65103.1").unwrap()
        );
        assert_eq!(index.exchange_ts, 1710000000.0);
    }

    #[cfg(feature = "markprice")]
    #[test]
    fn parses_gateio_derivative_ticker_mark_price() {
        let message = json!({
            "channel": "futures.tickers",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "last": "65102.1",
                "mark_price": "65103.4",
                "time": 1710000000000u64
            }]
        });

        let mark = parse_mark_price(&message, 1710000001.0).expect("mark price");

        assert_eq!(mark.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(
            mark.price,
            rust_decimal::Decimal::from_str_exact("65103.4").unwrap()
        );
        assert_eq!(mark.next_funding_time, None);
        assert_eq!(mark.predicted_rate, None);
        assert_eq!(mark.exchange_ts, 1710000000.0);
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_gateio_book_ticker_l1_book() {
        let message = json!({
            "channel": "spot.book_ticker",
            "event": "update",
            "result": {
                "t": 1710000000123u64,
                "u": 48733182u64,
                "s": "ETH_BTC",
                "b": "0.03400",
                "B": "1.25",
                "a": "0.03410",
                "A": "0.75"
            }
        });

        let book = parse_l1_book(&message, 1710000001.5).expect("l1 book");

        assert_eq!(book.symbol.as_str(), "ETH-BTC");
        assert_eq!(
            book.bid.price,
            rust_decimal::Decimal::from_str_exact("0.03400").unwrap()
        );
        assert_eq!(
            book.bid.amount,
            rust_decimal::Decimal::from_str_exact("1.25").unwrap()
        );
        assert_eq!(
            book.ask.price,
            rust_decimal::Decimal::from_str_exact("0.03410").unwrap()
        );
        assert_eq!(book.exchange_ts, 1710000000.123);
    }
}
