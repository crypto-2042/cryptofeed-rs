use super::parser;
use crate::exchange::ExchangeFeed;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
    symbol::{InstrumentKind, Symbol},
};
use serde_json::Value;

#[cfg(feature = "candles")]
use cryptofeed_candles::Candle;
#[cfg(feature = "funding")]
use cryptofeed_funding::Funding;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPrice;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::Liquidation;
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPrice;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterest;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L1Book, L2Book};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::Ticker;
#[cfg(feature = "trade")]
use cryptofeed_trade::Trade;

pub struct BinanceAdapter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceProduct {
    Spot,
    UsdM,
    CoinM,
    Option,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceInstrument {
    pub symbol: Symbol,
    pub exchange_symbol: String,
    pub product: BinanceProduct,
}

impl BinanceInstrument {
    pub fn new(symbol: Symbol, exchange_symbol: &str, product: BinanceProduct) -> Self {
        Self {
            symbol,
            exchange_symbol: exchange_symbol.to_ascii_uppercase(),
            product,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinanceConnectionPlan {
    pub product: BinanceProduct,
    pub websocket_url: String,
    pub streams: Vec<String>,
    /// REST URLs for L2-subscribed instruments only, in their instrument order.
    pub snapshot_urls: Vec<String>,
    /// Partial-depth width (5/10/20) when the feed requested a bounded L2
    /// book; `None` means the full-depth `@depth` stream with a 1000-level
    /// snapshot. Partial-depth pushes carry the complete top-N book every
    /// interval, so book sync must treat them as replacement snapshots.
    pub l2_book_depth: Option<u16>,
    pub instruments: Vec<BinanceInstrument>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BinanceAdapterMetadata {
    pub product: BinanceProduct,
    pub websocket_url: &'static str,
    pub snapshot_endpoint: &'static str,
    pub supported_channels: &'static [Channel],
}

const SPOT_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
];
const OPTION_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::Candles,
];
const DERIVATIVE_CHANNELS: &[Channel] = &[
    Channel::Ticker,
    Channel::Trade,
    Channel::L2Book,
    Channel::L1Book,
    Channel::Candles,
    Channel::Funding,
    Channel::Liquidations,
    Channel::MarkPrice,
    Channel::OpenInterest,
    Channel::Index,
];

pub enum BinanceEvent {
    #[cfg(feature = "candles")]
    Candle(Candle),
    #[cfg(feature = "funding")]
    Funding(Funding),
    #[cfg(feature = "index")]
    IndexPrice(IndexPrice),
    #[cfg(feature = "liquidations")]
    Liquidation(Liquidation),
    #[cfg(feature = "orderbook")]
    L1Book(L1Book),
    #[cfg(feature = "markprice")]
    MarkPrice(MarkPrice),
    #[cfg(feature = "orderbook")]
    L2Book(L2Book),
    #[cfg(feature = "openinterest")]
    OpenInterest(OpenInterest),
    #[cfg(feature = "ticker")]
    Ticker(Ticker),
    #[cfg(feature = "trade")]
    Trade(Trade),
}

impl BinanceAdapter {
    pub fn websocket_url() -> &'static str {
        "wss://stream.binance.com:9443/stream?streams="
    }

    /// Builds the initial explicit subscription on a combined-stream endpoint.
    /// The response must be matched to id 1; callers handle control responses
    /// separately from market data. URL-based planning remains available.
    pub fn explicit_subscription(plan: &BinanceConnectionPlan) -> Result<(String, String)> {
        let mut url = url::Url::parse(&plan.websocket_url)
            .map_err(|error| Error::InvalidConfiguration(error.to_string()))?;
        let streams: Vec<_> = url
            .query_pairs()
            .find(|(key, _)| key == "streams")
            .map(|(_, streams)| streams.split('/').map(str::to_owned).collect())
            .ok_or_else(|| {
                Error::InvalidConfiguration("Binance stream plan is missing topics".to_owned())
            })?;
        url.set_query(None);
        Ok((
            url.to_string(),
            serde_json::json!({"method":"SUBSCRIBE","params":streams,"id":1}).to_string(),
        ))
    }

    pub fn connection_plans(feed: &ExchangeFeed) -> Result<Vec<BinanceConnectionPlan>> {
        let instruments = Self::instruments(feed)?;
        if feed.channels.contains(&Channel::Candles)
            && candle_interval_wire(&feed.candle_interval).is_none()
        {
            return Err(Error::UnsupportedCapability(format!(
                "binance candle interval {:?}",
                feed.candle_interval
            )));
        }
        let l2_book_interval = feed.l2_book_interval.as_deref().unwrap_or("100ms");
        let mut plans = Vec::new();

        for product in [
            BinanceProduct::Spot,
            BinanceProduct::UsdM,
            BinanceProduct::CoinM,
            BinanceProduct::Option,
        ] {
            let product_instruments: Vec<_> = instruments
                .iter()
                .filter(|instrument| instrument.product == product)
                .cloned()
                .collect();
            if product_instruments.is_empty() {
                continue;
            }
            Self::validate_channels(product, &feed.channels)?;
            // Option depth streams (`@depth1000`) are not interval-
            // parameterized, so the interval check applies to every product
            // except options.
            if product != BinanceProduct::Option
                && feed.channels.contains(&Channel::L2Book)
                && !l2_book_interval_supported(l2_book_interval, product)
            {
                return Err(Error::UnsupportedCapability(format!(
                    "binance {product:?} l2 book interval {l2_book_interval}"
                )));
            }
            if product == BinanceProduct::UsdM {
                let public_channels: Vec<_> = feed
                    .channels
                    .iter()
                    .copied()
                    .filter(|channel| {
                        matches!(channel, Channel::Ticker | Channel::L2Book | Channel::L1Book)
                    })
                    .collect();
                let market_channels: Vec<_> = feed
                    .channels
                    .iter()
                    .copied()
                    .filter(|channel| {
                        matches!(
                            channel,
                            Channel::Trade
                                | Channel::Candles
                                | Channel::Funding
                                | Channel::Liquidations
                                | Channel::MarkPrice
                                | Channel::OpenInterest
                                | Channel::Index
                        )
                    })
                    .collect();
                if !public_channels.is_empty() {
                    plans.push(Self::connection_plan(
                        product,
                        &product_instruments,
                        &public_channels,
                        "wss://fstream.binance.com/public/stream?streams=",
                        feed,
                    ));
                }
                if !market_channels.is_empty() {
                    plans.push(Self::connection_plan(
                        product,
                        &product_instruments,
                        &market_channels,
                        "wss://fstream.binance.com/market/stream?streams=",
                        feed,
                    ));
                }
            } else {
                plans.push(Self::connection_plan(
                    product,
                    &product_instruments,
                    &feed.channels,
                    Self::websocket_url_for(product),
                    feed,
                ));
            }
        }

        plans.retain(|plan| !plan.streams.is_empty());
        Ok(plans)
    }

    pub fn validate_channels(product: BinanceProduct, channels: &[Channel]) -> Result<()> {
        let metadata = Self::metadata(product);
        for channel in channels {
            if !metadata.supported_channels.contains(channel) {
                return Err(Error::UnsupportedChannel(channel_name(*channel).to_owned()));
            }
        }
        Ok(())
    }

    pub fn metadata(product: BinanceProduct) -> BinanceAdapterMetadata {
        match product {
            BinanceProduct::Spot => BinanceAdapterMetadata {
                product,
                websocket_url: "wss://stream.binance.com:9443/stream?streams=",
                snapshot_endpoint: "https://api.binance.com/api/v3/depth",
                supported_channels: SPOT_CHANNELS,
            },
            BinanceProduct::UsdM => BinanceAdapterMetadata {
                product,
                websocket_url: "wss://fstream.binance.com/public/stream?streams=",
                snapshot_endpoint: "https://fapi.binance.com/fapi/v1/depth",
                supported_channels: DERIVATIVE_CHANNELS,
            },
            BinanceProduct::Option => BinanceAdapterMetadata {
                product,
                websocket_url: "wss://nbstream.binance.com/eoptions/stream?streams=",
                snapshot_endpoint: "https://eapi.binance.com/eapi/v1/depth",
                supported_channels: OPTION_CHANNELS,
            },
            BinanceProduct::CoinM => BinanceAdapterMetadata {
                product,
                websocket_url: "wss://dstream.binance.com/stream?streams=",
                snapshot_endpoint: "https://dapi.binance.com/dapi/v1/depth",
                supported_channels: DERIVATIVE_CHANNELS,
            },
        }
    }

    pub fn websocket_url_for(product: BinanceProduct) -> &'static str {
        Self::metadata(product).websocket_url
    }

    pub fn snapshot_url(instrument: &BinanceInstrument, limit: u16) -> String {
        let endpoint = Self::metadata(instrument.product).snapshot_endpoint;
        format!(
            "{endpoint}?symbol={}&limit={limit}",
            instrument.exchange_symbol
        )
    }

    pub fn instruments(feed: &ExchangeFeed) -> Result<Vec<BinanceInstrument>> {
        if !feed.exchange_symbols.is_empty() && feed.exchange_symbols.len() != feed.symbols.len() {
            return Err(Error::UnsupportedSymbol(
                "binance normalized and exchange symbol counts differ".to_owned(),
            ));
        }

        feed.symbols
            .iter()
            .enumerate()
            .map(|(index, symbol)| {
                let product = product_from_normalized(symbol)?;
                let exchange_symbol = feed
                    .exchange_symbols
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| native_symbol(symbol, product));
                Ok(BinanceInstrument::new(
                    symbol.clone(),
                    &exchange_symbol,
                    product,
                ))
            })
            .collect()
    }

    fn streams_for_instruments(
        instruments: &[BinanceInstrument],
        channels: &[Channel],
        feed: &ExchangeFeed,
    ) -> Vec<String> {
        let candle_interval = feed.candle_interval.as_str();
        let l2_book_depth = feed.l2_book_depth;
        let l2_book_interval = feed.l2_book_interval.as_deref().unwrap_or("100ms");
        let mut streams = Vec::new();
        for instrument in instruments {
            let exchange_symbol = instrument.exchange_symbol.to_ascii_lowercase();
            for channel in channels {
                if !feed.subscribes(*channel, &instrument.symbol) {
                    continue;
                }
                let mut stream = if instrument.product == BinanceProduct::Option {
                    option_stream_name(&exchange_symbol, *channel, candle_interval)
                } else {
                    stream_name(
                        &exchange_symbol,
                        *channel,
                        candle_interval,
                        l2_book_depth,
                        l2_book_interval,
                    )
                };
                // The documented mark-price payload carries the per-contract
                // index price for both USD-M and COIN-M. Share its 1s stream
                // when Index is requested, avoiding guessed native index topics.
                if channels.contains(&Channel::Index)
                    && feed.subscribes(Channel::Index, &instrument.symbol)
                    && stream.ends_with("@markPrice")
                {
                    stream.push_str("@1s");
                }
                if !stream.is_empty() && !streams.contains(&stream) {
                    streams.push(stream);
                }
            }
        }
        streams
    }

    fn connection_plan(
        product: BinanceProduct,
        instruments: &[BinanceInstrument],
        channels: &[Channel],
        websocket_base: &str,
        feed: &ExchangeFeed,
    ) -> BinanceConnectionPlan {
        let instruments: Vec<_> = instruments
            .iter()
            .filter(|instrument| {
                channels
                    .iter()
                    .any(|channel| feed.subscribes(*channel, &instrument.symbol))
            })
            .cloned()
            .collect();
        let streams = Self::streams_for_instruments(&instruments, channels, feed);
        let l2_book_depth = feed.l2_book_depth;
        let snapshot_urls = if channels.contains(&Channel::L2Book) {
            // Partial depth streams (`@depth5@100ms` etc.) must be bootstrapped
            // from a snapshot of the same width; the full-depth default uses
            // the 1000-level snapshot.
            let limit = l2_book_depth.unwrap_or(1000);
            instruments
                .iter()
                .filter(|instrument| feed.subscribes(Channel::L2Book, &instrument.symbol))
                .map(|instrument| Self::snapshot_url(instrument, limit))
                .collect()
        } else {
            Vec::new()
        };
        BinanceConnectionPlan {
            product,
            websocket_url: format!("{websocket_base}{}", streams.join("/")),
            streams,
            snapshot_urls,
            l2_book_depth,
            instruments,
        }
    }

    /// Spot partial-depth pushes (`@depth5@100ms` etc.) carry
    /// `{"lastUpdateId": ..., "bids": [...], "asks": [...]}` with no `e`, no
    /// `s`, and no `U`/`u`; the instrument is only in the combined-stream
    /// `stream` field. Full-depth and derivative payloads always carry `e`.
    pub fn is_partial_depth_payload(payload: &Value) -> bool {
        payload.get("e").is_none()
            && payload.get("lastUpdateId").is_some()
            && payload.get("bids").is_some()
            && payload.get("asks").is_some()
    }

    /// Resolve the instrument for a combined-stream `stream` name like
    /// `btcusdt@depth5@100ms` (stream symbols are lowercase).
    pub fn instrument_for_stream<'a>(
        plan: &'a BinanceConnectionPlan,
        stream: &str,
    ) -> Option<&'a BinanceInstrument> {
        let stream_symbol = stream.split('@').next()?;
        plan.instruments.iter().find(|instrument| {
            instrument
                .exchange_symbol
                .eq_ignore_ascii_case(stream_symbol)
        })
    }

    pub fn unwrap_combined_message(message: &Value) -> Option<&Value> {
        message.get("data").or(Some(message))
    }
    pub fn parse_message(message: &Value, received_ts: f64) -> Option<BinanceEvent> {
        Self::parse_messages(message, received_ts)
            .into_iter()
            .next()
    }

    pub fn parse_messages(message: &Value, received_ts: f64) -> Vec<BinanceEvent> {
        let Some(payload) = Self::unwrap_combined_message(message) else {
            return Vec::new();
        };
        let Some(event) = message_event(message, payload) else {
            return Vec::new();
        };

        match event {
            #[cfg(feature = "candles")]
            "kline" => parser::parse_candle(payload, received_ts)
                .map(BinanceEvent::Candle)
                .into_iter()
                .collect(),
            #[cfg(any(feature = "funding", feature = "markprice", feature = "index"))]
            "markPriceUpdate" => {
                let mut events = Vec::new();
                #[cfg(feature = "funding")]
                if let Some(funding) = parser::parse_funding(payload, received_ts) {
                    events.push(BinanceEvent::Funding(funding));
                }
                #[cfg(feature = "markprice")]
                if let Some(mark_price) = parser::parse_mark_price(payload, received_ts) {
                    events.push(BinanceEvent::MarkPrice(mark_price));
                }
                #[cfg(feature = "index")]
                if let Some(index) = parser::parse_index_price(payload, received_ts) {
                    events.push(BinanceEvent::IndexPrice(index));
                }
                events
            }
            #[cfg(feature = "liquidations")]
            "forceOrder" => parser::parse_liquidation(payload, received_ts)
                .map(BinanceEvent::Liquidation)
                .into_iter()
                .collect(),
            #[cfg(feature = "orderbook")]
            "depthUpdate" => parser::parse_l2_book(payload, received_ts)
                .map(BinanceEvent::L2Book)
                .into_iter()
                .collect(),
            #[cfg(feature = "trade")]
            "aggTrade" => parser::parse_trade(payload, received_ts)
                .map(BinanceEvent::Trade)
                .into_iter()
                .collect(),
            #[cfg(feature = "trade")]
            "trade" => parser::parse_option_trade(payload, received_ts)
                .map(BinanceEvent::Trade)
                .into_iter()
                .collect(),
            #[cfg(any(feature = "ticker", feature = "orderbook"))]
            "bookTicker" => {
                // The bookTicker stream doubles as the BBO Ticker source and,
                // when subscribed, as the L1 top-of-book channel.
                let mut events = Vec::new();
                #[cfg(feature = "ticker")]
                if let Some(ticker) = parser::parse_ticker(payload, received_ts) {
                    events.push(BinanceEvent::Ticker(ticker));
                }
                #[cfg(feature = "orderbook")]
                if let Some(book) = parser::parse_l1_book(payload, received_ts) {
                    events.push(BinanceEvent::L1Book(book));
                }
                events
            }
            #[cfg(feature = "ticker")]
            "24hrTicker" => parser::parse_option_ticker(payload, received_ts)
                .map(BinanceEvent::Ticker)
                .into_iter()
                .collect(),
            #[cfg(feature = "index")]
            "IndexUpdate" | "indexPriceUpdate" => parser::parse_index_price(payload, received_ts)
                .map(BinanceEvent::IndexPrice)
                .into_iter()
                .collect(),
            #[cfg(feature = "openinterest")]
            "openInterest" => parser::parse_open_interest(payload, received_ts)
                .map(BinanceEvent::OpenInterest)
                .into_iter()
                .collect(),
            _ => Vec::new(),
        }
    }

    pub fn parse_message_for_instrument(
        message: &Value,
        received_ts: f64,
        instrument: &BinanceInstrument,
    ) -> Option<BinanceEvent> {
        Self::parse_messages_for_instrument(message, received_ts, instrument)
            .into_iter()
            .next()
    }

    pub fn parse_messages_for_instrument(
        message: &Value,
        received_ts: f64,
        instrument: &BinanceInstrument,
    ) -> Vec<BinanceEvent> {
        let Some(payload) = Self::unwrap_combined_message(message) else {
            return Vec::new();
        };
        let Some(event) = message_event(message, payload) else {
            return Vec::new();
        };

        match event {
            #[cfg(feature = "candles")]
            "kline" => parser::parse_candle_for_instrument(payload, received_ts, instrument)
                .map(BinanceEvent::Candle)
                .into_iter()
                .collect(),
            #[cfg(any(feature = "funding", feature = "markprice", feature = "index"))]
            "markPriceUpdate" => {
                let mut events = Vec::new();
                #[cfg(feature = "funding")]
                if let Some(funding) =
                    parser::parse_funding_for_instrument(payload, received_ts, instrument)
                {
                    events.push(BinanceEvent::Funding(funding));
                }
                #[cfg(feature = "markprice")]
                if let Some(mark_price) =
                    parser::parse_mark_price_for_instrument(payload, received_ts, instrument)
                {
                    events.push(BinanceEvent::MarkPrice(mark_price));
                }
                #[cfg(feature = "index")]
                if let Some(index) =
                    parser::parse_index_price_for_instrument(payload, received_ts, instrument)
                {
                    events.push(BinanceEvent::IndexPrice(index));
                }
                events
            }
            #[cfg(feature = "liquidations")]
            "forceOrder" => {
                parser::parse_liquidation_for_instrument(payload, received_ts, instrument)
                    .map(BinanceEvent::Liquidation)
                    .into_iter()
                    .collect()
            }
            #[cfg(feature = "orderbook")]
            "depthUpdate" => parser::parse_l2_book_for_instrument(payload, received_ts, instrument)
                .map(BinanceEvent::L2Book)
                .into_iter()
                .collect(),
            #[cfg(feature = "trade")]
            "aggTrade" => parser::parse_trade_for_instrument(payload, received_ts, instrument)
                .map(BinanceEvent::Trade)
                .into_iter()
                .collect(),
            #[cfg(feature = "trade")]
            "trade" => parser::parse_option_trade_for_instrument(payload, received_ts, instrument)
                .map(BinanceEvent::Trade)
                .into_iter()
                .collect(),
            #[cfg(any(feature = "ticker", feature = "orderbook"))]
            "bookTicker" => {
                let mut events = Vec::new();
                #[cfg(feature = "ticker")]
                if let Some(ticker) =
                    parser::parse_ticker_for_instrument(payload, received_ts, instrument)
                {
                    events.push(BinanceEvent::Ticker(ticker));
                }
                #[cfg(feature = "orderbook")]
                if let Some(book) =
                    parser::parse_l1_book_for_instrument(payload, received_ts, instrument)
                {
                    events.push(BinanceEvent::L1Book(book));
                }
                events
            }
            #[cfg(feature = "ticker")]
            "24hrTicker" => {
                parser::parse_option_ticker_for_instrument(payload, received_ts, instrument)
                    .map(BinanceEvent::Ticker)
                    .into_iter()
                    .collect()
            }
            #[cfg(feature = "index")]
            "IndexUpdate" | "indexPriceUpdate" => {
                parser::parse_index_price_for_instrument(payload, received_ts, instrument)
                    .map(BinanceEvent::IndexPrice)
                    .into_iter()
                    .collect()
            }
            #[cfg(feature = "openinterest")]
            "openInterest" => {
                parser::parse_open_interest_for_instrument(payload, received_ts, instrument)
                    .map(BinanceEvent::OpenInterest)
                    .into_iter()
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}

fn message_event<'a>(message: &'a Value, payload: &'a Value) -> Option<&'a str> {
    if let Some(event) = payload.get("e").and_then(Value::as_str) {
        return Some(event);
    }
    if let Some(stream) = message.get("stream").and_then(Value::as_str) {
        let channel = stream.split('@').nth(1)?;
        return match channel.to_ascii_lowercase().as_str() {
            "bookticker" => Some("bookTicker"),
            "aggtrade" => Some("aggTrade"),
            _ => None,
        };
    }
    (payload.get("s").is_some()
        && payload.get("u").is_some()
        && payload.get("b").and_then(Value::as_str).is_some()
        && payload.get("a").and_then(Value::as_str).is_some())
    .then_some("bookTicker")
}

fn option_stream_name(exchange_symbol: &str, channel: Channel, candle_interval: &str) -> String {
    match channel {
        Channel::Ticker => format!("{exchange_symbol}@ticker"),
        Channel::Trade => format!("{exchange_symbol}@trade"),
        Channel::L2Book => format!("{exchange_symbol}@depth1000"),
        Channel::Candles => format!(
            "{exchange_symbol}@kline_{}",
            candle_interval_wire(candle_interval).unwrap_or("1m")
        ),
        Channel::Funding
        | Channel::Liquidations
        | Channel::MarkPrice
        | Channel::OpenInterest
        | Channel::Index
        | Channel::L1Book => String::new(),
        _ => String::new(),
    }
}

fn stream_name(
    exchange_symbol: &str,
    channel: Channel,
    candle_interval: &str,
    l2_book_depth: Option<u16>,
    l2_book_interval: &str,
) -> String {
    match channel {
        Channel::Ticker => format!("{exchange_symbol}@bookTicker"),
        Channel::Trade => format!("{exchange_symbol}@aggTrade"),
        Channel::L2Book => match l2_book_depth {
            Some(5) => format!("{exchange_symbol}@depth5@{l2_book_interval}"),
            Some(10) => format!("{exchange_symbol}@depth10@{l2_book_interval}"),
            Some(20) => format!("{exchange_symbol}@depth20@{l2_book_interval}"),
            _ => format!("{exchange_symbol}@depth@{l2_book_interval}"),
        },
        Channel::Candles => format!(
            "{exchange_symbol}@kline_{}",
            candle_interval_wire(candle_interval).unwrap_or("1m")
        ),
        Channel::Funding => format!("{exchange_symbol}@markPrice"),
        Channel::MarkPrice => format!("{exchange_symbol}@markPrice"),
        Channel::Liquidations => format!("{exchange_symbol}@forceOrder"),
        Channel::OpenInterest => format!("{exchange_symbol}@openInterest"),
        Channel::Index => format!("{exchange_symbol}@markPrice"),
        Channel::L1Book => format!("{exchange_symbol}@bookTicker"),
        _ => String::new(),
    }
}

/// Normalized L2 book update interval support per Binance product. Official
/// intervals: spot `100ms`/`1000ms`, USD-M/COIN-M `100ms`/`250ms`/`500ms`
/// (both partial `@depth{N}` and full `@depth` streams). Options streams are
/// not interval-parameterized.
pub fn l2_book_interval_supported(interval: &str, product: BinanceProduct) -> bool {
    match product {
        BinanceProduct::Spot => matches!(interval, "100ms" | "1000ms"),
        BinanceProduct::UsdM | BinanceProduct::CoinM => {
            matches!(interval, "100ms" | "250ms" | "500ms")
        }
        BinanceProduct::Option => false,
    }
}

/// Normalized candle interval vocabulary used by the feed builder, mapped to
/// the Binance wire form. Follows the stable Binance interval naming
/// (`1m`/`3m`/`5m`/`15m`/`30m`/`1h`/`2h`/`4h`/`6h`/`8h`/`12h`/`1d`/`3d`/
/// `1w`/`1M`) shared by spot, USD-M, and COIN-M streams.
pub fn candle_interval_wire(interval: &str) -> Option<&str> {
    if matches!(
        interval,
        "1m" | "3m"
            | "5m"
            | "15m"
            | "30m"
            | "1h"
            | "2h"
            | "4h"
            | "6h"
            | "8h"
            | "12h"
            | "1d"
            | "3d"
            | "1w"
            | "1M"
    ) {
        Some(interval)
    } else {
        None
    }
}

fn channel_name(channel: Channel) -> &'static str {
    match channel {
        Channel::Candles => "candles",
        Channel::Funding => "funding",
        Channel::Liquidations => "liquidations",
        Channel::Ticker => "ticker",
        Channel::Trade => "trade",
        Channel::L2Book => "l2_book",
        Channel::OpenInterest => "open_interest",
        Channel::Index => "index",
        Channel::MarkPrice => "mark_price",
        Channel::L1Book => "l1_book",
        _ => "unknown",
    }
}

fn product_from_normalized(symbol: &Symbol) -> Result<BinanceProduct> {
    if symbol.kind() == InstrumentKind::Option {
        return Ok(BinanceProduct::Option);
    }
    let parts: Vec<_> = symbol.as_str().split('-').collect();
    match parts.as_slice() {
        [_, _] => Ok(BinanceProduct::Spot),
        [_, quote, suffix] if *suffix == "PERP" || suffix.chars().all(|ch| ch.is_ascii_digit()) => {
            if matches!(*quote, "USDT" | "USDC") {
                Ok(BinanceProduct::UsdM)
            } else {
                Ok(BinanceProduct::CoinM)
            }
        }
        _ => Err(Error::UnsupportedSymbol(symbol.as_str().to_owned())),
    }
}

fn native_symbol(symbol: &Symbol, product: BinanceProduct) -> String {
    let parts: Vec<_> = symbol.as_str().split('-').collect();
    match (product, parts.as_slice()) {
        (BinanceProduct::Spot, [base, quote]) => format!("{base}{quote}"),
        (_, [base, quote, "PERP"]) if product == BinanceProduct::UsdM => format!("{base}{quote}"),
        (_, [base, quote, "PERP"]) => format!("{base}{quote}_PERP"),
        (_, [base, quote, expiry]) => format!("{base}{quote}_{expiry}"),
        _ => symbol.as_str().replace('-', ""),
    }
}

#[cfg(test)]
mod tests {
    use super::{BinanceAdapter, BinanceEvent, BinanceProduct};
    use crate::exchange::binance::Binance;

    #[test]
    fn candle_interval_and_l2_depth_reach_connection_plans() {
        let feed = Binance::new()
            .candles()
            .candles_interval("1h")
            .l2_book()
            .l2_book_depth(5)
            .instrument(cryptofeed_core::symbol::Symbol::perpetual("BTC", "USDT"))
            .exchange_symbol("BTCUSDT")
            .build();
        let plans = BinanceAdapter::connection_plans(&feed).expect("plans");
        // USD-M splits into public (depth) and market (kline) connections.
        assert_eq!(plans.len(), 2);
        assert!(
            plans
                .iter()
                .any(|plan| plan.websocket_url.contains("btcusdt@kline_1h"))
        );
        assert!(
            plans
                .iter()
                .any(|plan| plan.websocket_url.contains("btcusdt@depth5@100ms"))
        );
        assert!(plans.iter().any(|plan| plan.snapshot_urls
            == vec!["https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=5"]));
    }

    #[test]
    fn l2_book_interval_reaches_connection_plans_and_wire() {
        let feed = Binance::new()
            .l2_book()
            .l2_book_depth(20)
            .l2_book_interval("250ms")
            .instrument(cryptofeed_core::symbol::Symbol::perpetual("BTC", "USDT"))
            .exchange_symbol("BTCUSDT")
            .build();
        let plans = BinanceAdapter::connection_plans(&feed).expect("plans");
        assert!(
            plans
                .iter()
                .any(|plan| plan.websocket_url.contains("btcusdt@depth20@250ms"))
        );

        let spot_feed = Binance::new()
            .l2_book()
            .l2_book_interval("1000ms")
            .instrument(cryptofeed_core::symbol::Symbol::spot("BTC", "USDT"))
            .build();
        let spot_plan = BinanceAdapter::connection_plans(&spot_feed)
            .expect("spot plan")
            .remove(0);
        assert!(spot_plan.websocket_url.contains("btcusdt@depth@1000ms"));
    }

    #[test]
    fn l2_book_interval_supported_matches_official_sets() {
        use super::BinanceProduct;
        assert!(super::l2_book_interval_supported(
            "100ms",
            BinanceProduct::Spot
        ));
        assert!(super::l2_book_interval_supported(
            "1000ms",
            BinanceProduct::Spot
        ));
        assert!(!super::l2_book_interval_supported(
            "250ms",
            BinanceProduct::Spot
        ));
        assert!(super::l2_book_interval_supported(
            "100ms",
            BinanceProduct::UsdM
        ));
        assert!(super::l2_book_interval_supported(
            "250ms",
            BinanceProduct::UsdM
        ));
        assert!(super::l2_book_interval_supported(
            "500ms",
            BinanceProduct::CoinM
        ));
        assert!(!super::l2_book_interval_supported(
            "1000ms",
            BinanceProduct::UsdM
        ));
        assert!(!super::l2_book_interval_supported(
            "250ms",
            BinanceProduct::Option
        ));
    }

    #[test]
    fn rejects_unsupported_l2_book_interval_in_plans() {
        let feed = Binance::new()
            .l2_book()
            .l2_book_interval("250ms")
            .instrument(cryptofeed_core::symbol::Symbol::spot("BTC", "USDT"))
            .build();
        assert!(BinanceAdapter::connection_plans(&feed).is_err());
    }

    #[test]
    fn option_l2_book_plans_without_interval_validation() {
        // Options depth streams (`@depth1000`) are not interval-parameterized;
        // the default interval must not reject an option L2 plan.
        let feed = Binance::new()
            .l2_book()
            .instrument(cryptofeed_core::symbol::Symbol::option(
                "BTC", "USDT", "26JUN26", "70000", "C",
            ))
            .exchange_symbol("BTC-26JUN26-70000-C")
            .build();

        let plans = BinanceAdapter::connection_plans(&feed).expect("option l2 plan");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].product, BinanceProduct::Option);
        assert!(plans[0].websocket_url.contains("@depth1000"));
        assert_eq!(plans[0].snapshot_urls.len(), 1);
    }

    #[test]
    fn candle_interval_wire_maps_official_binance_intervals() {
        for interval in [
            "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
            "1M",
        ] {
            assert_eq!(
                super::candle_interval_wire(interval),
                Some(interval),
                "binance interval {interval}"
            );
        }
        assert_eq!(super::candle_interval_wire("7d"), None);
    }

    #[test]
    fn builds_binance_stream_names() {
        let feed = Binance::new().ticker().trade().symbol("BTC-USDT").build();
        let plan = BinanceAdapter::connection_plans(&feed)
            .expect("plan")
            .remove(0);

        assert_eq!(
            plan.streams,
            vec![
                "btcusdt@bookTicker".to_owned(),
                "btcusdt@aggTrade".to_owned(),
            ]
        );
    }

    #[cfg(feature = "candles")]
    #[test]
    fn builds_binance_candle_stream_name() {
        let feed = Binance::new().candles().symbol("BTC-USDT").build();
        let plan = BinanceAdapter::connection_plans(&feed)
            .expect("plan")
            .remove(0);
        assert_eq!(plan.streams, vec!["btcusdt@kline_1m".to_owned()]);
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parse_message_for_instrument_resolves_through_instrument() {
        use super::BinanceInstrument;
        use crate::exchange::binance::adapter::BinanceProduct;
        use cryptofeed_core::symbol::Symbol;
        let instrument =
            BinanceInstrument::new(Symbol::spot("BTC", "USDT"), "BTCUSDT", BinanceProduct::Spot);
        let message = serde_json::json!({
            "u": 400900217u64,
            "s": "BTCUSDT",
            "b": "64999.10",
            "B": "1.25",
            "a": "65000.20",
            "A": "0.75"
        });
        let event =
            BinanceAdapter::parse_message_for_instrument(&message, 1710000001.5, &instrument)
                .expect("event");
        match event {
            BinanceEvent::Ticker(ticker) => assert_eq!(ticker.symbol.as_str(), "BTC-USDT"),
            _ => panic!("expected ticker event"),
        }
    }

    #[test]
    fn builds_binance_subscription_url() {
        let feed = Binance::new().ticker().trade().symbol("BTC-USDT").build();

        let plan = BinanceAdapter::connection_plans(&feed)
            .expect("plan")
            .remove(0);
        assert_eq!(
            plan.websocket_url,
            "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade"
        );
    }

    #[test]
    fn plans_product_specific_connections_and_snapshots() {
        let feed = Binance::new()
            .ticker()
            .l2_book()
            .symbol("ETH-BTC")
            .exchange_symbol("ETHBTC")
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .symbol("BTC-USD-240927")
            .exchange_symbol("BTCUSD_240927")
            .build();

        let plans = BinanceAdapter::connection_plans(&feed).expect("plans");
        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].product, BinanceProduct::Spot);
        assert_eq!(
            plans[0].websocket_url,
            "wss://stream.binance.com:9443/stream?streams=ethbtc@bookTicker/ethbtc@depth@100ms"
        );
        assert_eq!(
            plans[0].snapshot_urls,
            vec!["https://api.binance.com/api/v3/depth?symbol=ETHBTC&limit=1000"]
        );
        assert_eq!(plans[1].product, BinanceProduct::UsdM);
        assert_eq!(
            plans[1].websocket_url,
            "wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms"
        );
        assert_eq!(
            plans[1].snapshot_urls,
            vec!["https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"]
        );
        assert_eq!(plans[2].product, BinanceProduct::CoinM);
        assert_eq!(
            plans[2].websocket_url,
            "wss://dstream.binance.com/stream?streams=btcusd_240927@bookTicker/btcusd_240927@depth@100ms"
        );
        assert_eq!(
            plans[2].snapshot_urls,
            vec!["https://dapi.binance.com/dapi/v1/depth?symbol=BTCUSD_240927&limit=1000"]
        );
    }

    #[test]
    fn splits_usd_m_public_and_market_streams() {
        let feed = Binance::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .funding()
            .liquidations()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build();

        let plans = BinanceAdapter::connection_plans(&feed).expect("plans");
        assert_eq!(plans.len(), 2);
        assert_eq!(
            plans[0].websocket_url,
            "wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms"
        );
        assert_eq!(plans[0].snapshot_urls.len(), 1);
        assert_eq!(
            plans[1].websocket_url,
            "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m/btcusdt@markPrice/btcusdt@forceOrder"
        );
        assert!(plans[1].snapshot_urls.is_empty());
    }

    #[test]
    fn rejects_derivatives_only_channels_for_spot() {
        let feed = Binance::new()
            .funding()
            .symbol("ETH-BTC")
            .exchange_symbol("ETHBTC")
            .build();

        let error = BinanceAdapter::connection_plans(&feed).expect_err("unsupported channel");
        assert!(error.to_string().contains("funding"));
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_trade_event_message() {
        let message = serde_json::json!({
            "e": "aggTrade",
            "s": "BTCUSDT",
            "a": 12345,
            "p": "65000.50",
            "q": "0.01000000",
            "T": 1710000000123u64,
            "m": false
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5);
        assert!(matches!(event, Some(BinanceEvent::Trade(_))));
    }

    #[cfg(feature = "ticker")]
    #[test]
    fn parses_ticker_event_message() {
        let message = serde_json::json!({
            "stream": "btcusdt@bookTicker",
            "data": {
                "u": 400900217,
                "s": "BTCUSDT",
                "b": "64999.10",
                "B": "1.25",
                "a": "65000.20",
                "A": "0.75"
            }
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5);
        assert!(matches!(event, Some(BinanceEvent::Ticker(_))));
    }

    #[cfg(feature = "trade")]
    #[test]
    fn parses_trade_event_from_combined_stream_wrapper() {
        let message = serde_json::json!({
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
        assert!(matches!(event, Some(BinanceEvent::Trade(_))));
    }

    #[cfg(feature = "orderbook")]
    #[test]
    fn parses_l2_book_event_message() {
        let message = serde_json::json!({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "E": 1710000000456u64,
            "b": [["64999.10", "1.25"]],
            "a": [["65000.20", "0.75"]]
        });

        let event = BinanceAdapter::parse_message(&message, 1710000001.5);
        assert!(matches!(event, Some(BinanceEvent::L2Book(_))));
    }
}
