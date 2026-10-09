pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod coinbase;
pub mod gateio;
pub mod kraken;
pub mod okx;

use std::collections::{HashMap, HashSet};
#[cfg(any(
    feature = "ticker",
    feature = "trade",
    feature = "orderbook",
    feature = "candles",
    feature = "funding",
    feature = "liquidations",
    feature = "markprice",
    feature = "openinterest",
    feature = "index"
))]
use std::sync::Arc;
#[cfg(feature = "orderbook")]
use std::sync::Mutex;

#[cfg(feature = "orderbook")]
use crate::exchange::binance::book_sync::BinanceBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::bitget::book_sync::BitgetBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::bybit::book_sync::BybitBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::gateio::book_sync::GateioBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::okx::book_sync::OkxBookSync;
#[cfg(feature = "candles")]
use cryptofeed_candles::CandleHandler;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::{Channel, ExchangeId},
    symbol::{InstrumentKind, Symbol},
};
#[cfg(feature = "funding")]
use cryptofeed_funding::FundingHandler;
#[cfg(feature = "index")]
use cryptofeed_index::IndexPriceHandler;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::LiquidationHandler;
#[cfg(feature = "markprice")]
use cryptofeed_markprice::MarkPriceHandler;
#[cfg(feature = "openinterest")]
use cryptofeed_openinterest::OpenInterestHandler;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::{L2BookState, OrderBookHandler};
#[cfg(feature = "ticker")]
use cryptofeed_ticker::TickerHandler;
#[cfg(feature = "trade")]
use cryptofeed_trade::TradeHandler;

#[derive(Clone)]
pub struct ExchangeFeed {
    pub exchange: ExchangeId,
    pub channels: Vec<Channel>,
    pub symbols: Vec<Symbol>,
    pub exchange_symbols: Vec<String>,
    pub(crate) channel_subscriptions: Vec<(Channel, Vec<Symbol>)>,
    pub(crate) subscription_mode_conflict: bool,
    /// Normalized candle interval (default `"1m"`); each adapter maps it to
    /// its own wire form. `Channel::Candles` uses a single interval per feed.
    pub candle_interval: String,
    /// Requested L2 depth level; `None` means the exchange default (Bybit 50,
    /// OKX/Bitget full book, Binance full depth).
    pub l2_book_depth: Option<u16>,
    /// Requested L2 book update interval (Binance only; other exchanges use
    /// the exchange default). `None` means the fastest `100ms` stream.
    pub l2_book_interval: Option<String>,
    #[cfg(feature = "ticker")]
    pub ticker_handler: Option<Arc<dyn TickerHandler>>,
    #[cfg(feature = "candles")]
    pub candle_handler: Option<Arc<dyn CandleHandler>>,
    #[cfg(feature = "funding")]
    pub funding_handler: Option<Arc<dyn FundingHandler>>,
    #[cfg(feature = "liquidations")]
    pub liquidation_handler: Option<Arc<dyn LiquidationHandler>>,
    #[cfg(feature = "markprice")]
    pub mark_price_handler: Option<Arc<dyn MarkPriceHandler>>,
    #[cfg(feature = "trade")]
    pub trade_handler: Option<Arc<dyn TradeHandler>>,
    #[cfg(feature = "openinterest")]
    pub open_interest_handler: Option<Arc<dyn OpenInterestHandler>>,
    #[cfg(feature = "index")]
    pub index_price_handler: Option<Arc<dyn IndexPriceHandler>>,
    #[cfg(feature = "orderbook")]
    pub orderbook_handler: Option<Arc<dyn OrderBookHandler>>,
    #[cfg(feature = "orderbook")]
    pub(crate) orderbook_states: Arc<Mutex<HashMap<String, L2BookState>>>,
    #[cfg(feature = "orderbook")]
    pub(crate) binance_book_syncs: Arc<Mutex<HashMap<String, BinanceBookSync>>>,
    #[cfg(feature = "orderbook")]
    pub(crate) bitget_book_syncs: Arc<Mutex<HashMap<String, BitgetBookSync>>>,
    #[cfg(feature = "orderbook")]
    pub(crate) bybit_book_syncs: Arc<Mutex<HashMap<String, BybitBookSync>>>,
    #[cfg(feature = "orderbook")]
    pub(crate) gateio_book_syncs: Arc<Mutex<HashMap<String, GateioBookSync>>>,
    #[cfg(feature = "orderbook")]
    pub(crate) okx_book_syncs: Arc<Mutex<HashMap<String, OkxBookSync>>>,
    /// Event-stream sender injected by `FeedHandler::add_feed` when the user
    /// subscribed via `FeedHandler::subscribe()`.
    pub(crate) event_sender: Option<tokio::sync::broadcast::Sender<crate::feed::FeedEvent>>,
    /// Lifecycle sender injected by `FeedHandler::add_feed`.
    pub(crate) status_sender: Option<tokio::sync::broadcast::Sender<crate::feed::FeedStatus>>,
    /// Per-channel counters injected by `FeedHandler::add_feed`.
    pub(crate) event_counts: Option<std::sync::Arc<crate::feed::EventCounters>>,
}

impl ExchangeFeed {
    /// Whether this feed requests the exact normalized channel/symbol pair.
    pub fn subscribes(&self, channel: Channel, symbol: &Symbol) -> bool {
        self.channels.contains(&channel)
            && self.symbols.contains(symbol)
            && (self.channel_subscriptions.is_empty()
                || self
                    .channel_subscriptions
                    .iter()
                    .any(|(entry, symbols)| *entry == channel && symbols.contains(symbol)))
    }

    pub(crate) fn validate_subscription_configuration(&self) -> Result<()> {
        if self.subscription_mode_conflict {
            return Err(Error::InvalidConfiguration(
                "use per-channel subscriptions or shared channels/symbols, not both".to_owned(),
            ));
        }
        if self
            .channel_subscriptions
            .iter()
            .any(|(_, symbols)| symbols.is_empty())
        {
            return Err(Error::InvalidConfiguration(
                "each channel subscription must contain at least one symbol".to_owned(),
            ));
        }
        Ok(())
    }

    /// Compiles per-channel subscriptions into concrete feeds for adapter
    /// planning. Channels with identical symbol sets share a group. Different
    /// sets may use separate connections; this is not capacity-based sharding.
    /// Explicit native names map to the first-seen union in `self.symbols`.
    pub fn connection_feeds(&self) -> Result<Vec<Self>> {
        self.validate_subscription_configuration()?;
        if self.channel_subscriptions.is_empty() {
            return Ok(vec![self.clone()]);
        }
        self.product_kind()?;
        if !self.exchange_symbols.is_empty() {
            self.ensure_symbol_mapping_is_complete()?;
        }
        let native: HashMap<_, _> = self.symbols.iter().zip(&self.exchange_symbols).collect();
        let mut groups: Vec<Self> = Vec::new();
        for (channel, symbols) in &self.channel_subscriptions {
            let mut symbols = symbols.clone();
            symbols.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            if let Some(group) = groups.iter_mut().find(|group| group.symbols == symbols) {
                group.channels.push(*channel);
                continue;
            }
            let mut group = self.clone();
            group.channels = vec![*channel];
            group.exchange_symbols = if self.exchange_symbols.is_empty() {
                Vec::new()
            } else {
                symbols
                    .iter()
                    .map(|symbol| native[symbol].clone())
                    .collect()
            };
            group.symbols = symbols;
            group.channel_subscriptions.clear();
            group.subscription_mode_conflict = false;
            groups.push(group);
        }
        Ok(groups)
    }

    /// Publishes a normalized event to the `FeedHandler` event stream (when
    /// subscribed) and bumps the per-channel counter. Drop failures are
    /// expected: the broadcast channel is bounded and lagging subscribers
    /// lose the oldest events by design.
    pub(crate) fn publish_event(&self, event: crate::feed::FeedEvent) {
        if let Some(sender) = &self.event_sender {
            let _ = sender.send(event.clone());
        }
        if let Some(counts) = &self.event_counts {
            counts.bump(event.channel());
        }
    }

    pub fn product_kind(&self) -> Result<InstrumentKind> {
        let first = self.symbols.first().ok_or_else(|| {
            Error::InvalidConfiguration(
                "a feed must contain at least one normalized symbol".to_owned(),
            )
        })?;
        let product = first.kind();
        if product == InstrumentKind::Unknown {
            return Err(Error::UnsupportedSymbol(first.as_str().to_owned()));
        }
        for symbol in &self.symbols[1..] {
            if symbol.kind() == InstrumentKind::Unknown {
                return Err(Error::UnsupportedSymbol(symbol.as_str().to_owned()));
            }
            if symbol.kind() != product {
                return Err(Error::InvalidConfiguration(format!(
                    "one feed cannot mix {:?} and {:?} products",
                    product,
                    symbol.kind()
                )));
            }
        }
        Ok(product)
    }

    pub fn exchange_symbol_for(&self, symbol: &Symbol) -> Result<&str> {
        self.ensure_symbol_mapping_is_complete()?;
        self.symbols
            .iter()
            .position(|candidate| candidate == symbol)
            .map(|index| self.exchange_symbols[index].as_str())
            .ok_or_else(|| Error::UnsupportedSymbol(symbol.as_str().to_owned()))
    }

    pub fn normalized_symbol_for(
        &self,
        exchange_symbol: &str,
        product: InstrumentKind,
    ) -> Result<&Symbol> {
        self.ensure_symbol_mapping_is_complete()?;
        let mut matches =
            self.symbols
                .iter()
                .zip(&self.exchange_symbols)
                .filter(|(symbol, candidate)| {
                    symbol.kind() == product && candidate.eq_ignore_ascii_case(exchange_symbol)
                });
        let Some((symbol, _)) = matches.next() else {
            return Err(Error::UnsupportedSymbol(format!(
                "{} ({product:?})",
                exchange_symbol.to_ascii_uppercase()
            )));
        };
        if matches.next().is_some() {
            return Err(Error::AmbiguousSymbol(format!(
                "{} ({product:?})",
                exchange_symbol.to_ascii_uppercase()
            )));
        }
        Ok(symbol)
    }

    fn ensure_symbol_mapping_is_complete(&self) -> Result<()> {
        if self.symbols.len() != self.exchange_symbols.len() {
            return Err(Error::InvalidConfiguration(
                "normalized and exchange symbol counts must match".to_owned(),
            ));
        }
        Ok(())
    }
}

/// The user-facing handler name for a channel, or `None` when the channel is
/// not subscribed or its handler is registered. Used by the builder to warn
/// about subscriptions that would silently drop events.
fn handler_name_for_channel(
    channel: Channel,
    builder: &ExchangeFeedBuilder,
) -> Option<&'static str> {
    if builder.has_handler_for(channel) {
        None
    } else {
        Some(channel_name_for_warning(channel))
    }
}

impl ExchangeFeedBuilder {
    /// Whether a handler trait is registered for the channel. Channels whose
    /// data feature is disabled report `false` so the builder can warn that
    /// the subscription would be dropped.
    pub(crate) fn has_handler_for(&self, channel: Channel) -> bool {
        match channel {
            #[cfg(feature = "ticker")]
            Channel::Ticker => self.ticker_handler.is_some(),
            #[cfg(not(feature = "ticker"))]
            Channel::Ticker => false,
            #[cfg(feature = "candles")]
            Channel::Candles => self.candle_handler.is_some(),
            #[cfg(not(feature = "candles"))]
            Channel::Candles => false,
            #[cfg(feature = "funding")]
            Channel::Funding => self.funding_handler.is_some(),
            #[cfg(not(feature = "funding"))]
            Channel::Funding => false,
            #[cfg(feature = "liquidations")]
            Channel::Liquidations => self.liquidation_handler.is_some(),
            #[cfg(not(feature = "liquidations"))]
            Channel::Liquidations => false,
            #[cfg(feature = "markprice")]
            Channel::MarkPrice => self.mark_price_handler.is_some(),
            #[cfg(not(feature = "markprice"))]
            Channel::MarkPrice => false,
            #[cfg(feature = "trade")]
            Channel::Trade => self.trade_handler.is_some(),
            #[cfg(not(feature = "trade"))]
            Channel::Trade => false,
            #[cfg(feature = "orderbook")]
            Channel::L2Book => self.orderbook_handler.is_some(),
            #[cfg(not(feature = "orderbook"))]
            Channel::L2Book => false,
            #[cfg(feature = "orderbook")]
            Channel::L1Book => self.orderbook_handler.is_some(),
            #[cfg(not(feature = "orderbook"))]
            Channel::L1Book => false,
            #[cfg(feature = "openinterest")]
            Channel::OpenInterest => self.open_interest_handler.is_some(),
            #[cfg(not(feature = "openinterest"))]
            Channel::OpenInterest => false,
            #[cfg(feature = "index")]
            Channel::Index => self.index_price_handler.is_some(),
            #[cfg(not(feature = "index"))]
            Channel::Index => false,
            // A future channel without a handler must not silently suppress
            // the capability warning.
            _ => false,
        }
    }
}

fn channel_name_for_warning(channel: Channel) -> &'static str {
    match channel {
        Channel::Candles => "candles",
        Channel::Funding => "funding",
        Channel::Liquidations => "liquidations",
        Channel::Ticker => "ticker",
        Channel::Trade => "trade",
        Channel::L2Book => "l2_book",
        Channel::L1Book => "l1_book",
        Channel::OpenInterest => "open_interest",
        Channel::Index => "index",
        Channel::MarkPrice => "mark_price",
        _ => "unknown",
    }
}

pub struct ExchangeFeedBuilder {
    exchange: ExchangeId,
    channels: Vec<Channel>,
    symbols: Vec<Symbol>,
    exchange_symbols: Vec<String>,
    channel_subscriptions: Vec<(Channel, Vec<Symbol>)>,
    candle_interval: String,
    l2_book_depth: Option<u16>,
    l2_book_interval: Option<String>,
    #[cfg(feature = "ticker")]
    ticker_handler: Option<Arc<dyn TickerHandler>>,
    #[cfg(feature = "candles")]
    candle_handler: Option<Arc<dyn CandleHandler>>,
    #[cfg(feature = "funding")]
    funding_handler: Option<Arc<dyn FundingHandler>>,
    #[cfg(feature = "liquidations")]
    liquidation_handler: Option<Arc<dyn LiquidationHandler>>,
    #[cfg(feature = "markprice")]
    mark_price_handler: Option<Arc<dyn MarkPriceHandler>>,
    #[cfg(feature = "trade")]
    trade_handler: Option<Arc<dyn TradeHandler>>,
    #[cfg(feature = "openinterest")]
    open_interest_handler: Option<Arc<dyn OpenInterestHandler>>,
    #[cfg(feature = "index")]
    index_price_handler: Option<Arc<dyn IndexPriceHandler>>,
    #[cfg(feature = "orderbook")]
    orderbook_handler: Option<Arc<dyn OrderBookHandler>>,
    #[cfg(feature = "orderbook")]
    orderbook_states: Arc<Mutex<HashMap<String, L2BookState>>>,
    #[cfg(feature = "orderbook")]
    binance_book_syncs: Arc<Mutex<HashMap<String, BinanceBookSync>>>,
    #[cfg(feature = "orderbook")]
    bitget_book_syncs: Arc<Mutex<HashMap<String, BitgetBookSync>>>,
    #[cfg(feature = "orderbook")]
    bybit_book_syncs: Arc<Mutex<HashMap<String, BybitBookSync>>>,
    #[cfg(feature = "orderbook")]
    gateio_book_syncs: Arc<Mutex<HashMap<String, GateioBookSync>>>,
    #[cfg(feature = "orderbook")]
    okx_book_syncs: Arc<Mutex<HashMap<String, OkxBookSync>>>,
}

impl ExchangeFeedBuilder {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            channels: Vec::new(),
            symbols: Vec::new(),
            exchange_symbols: Vec::new(),
            channel_subscriptions: Vec::new(),
            candle_interval: "1m".to_owned(),
            l2_book_depth: None,
            l2_book_interval: None,
            #[cfg(feature = "ticker")]
            ticker_handler: None,
            #[cfg(feature = "candles")]
            candle_handler: None,
            #[cfg(feature = "funding")]
            funding_handler: None,
            #[cfg(feature = "liquidations")]
            liquidation_handler: None,
            #[cfg(feature = "markprice")]
            mark_price_handler: None,
            #[cfg(feature = "trade")]
            trade_handler: None,
            #[cfg(feature = "openinterest")]
            open_interest_handler: None,
            #[cfg(feature = "index")]
            index_price_handler: None,
            #[cfg(feature = "orderbook")]
            orderbook_handler: None,
            #[cfg(feature = "orderbook")]
            orderbook_states: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "orderbook")]
            binance_book_syncs: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "orderbook")]
            bitget_book_syncs: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "orderbook")]
            bybit_book_syncs: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "orderbook")]
            gateio_book_syncs: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "orderbook")]
            okx_book_syncs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn ticker(mut self) -> Self {
        self.channels.push(Channel::Ticker);
        self
    }

    pub fn trade(mut self) -> Self {
        self.channels.push(Channel::Trade);
        self
    }

    pub fn l2_book(mut self) -> Self {
        self.channels.push(Channel::L2Book);
        self
    }

    pub fn l1_book(mut self) -> Self {
        self.channels.push(Channel::L1Book);
        self
    }

    pub fn open_interest(mut self) -> Self {
        self.channels.push(Channel::OpenInterest);
        self
    }

    pub fn index(mut self) -> Self {
        self.channels.push(Channel::Index);
        self
    }

    pub fn candles(mut self) -> Self {
        self.channels.push(Channel::Candles);
        self
    }

    /// Sets the normalized candle interval (default `"1m"`). The interval is
    /// validated against the exchange's official interval set during feed
    /// preflight; unsupported intervals fail before connecting.
    pub fn candles_interval(mut self, interval: impl Into<String>) -> Self {
        self.candle_interval = interval.into();
        self
    }

    /// Requests a specific L2 depth level. `None` (the default) keeps the
    /// exchange default (Bybit 50, OKX/Bitget full book, Binance full depth).
    pub fn l2_book_depth(mut self, level: u16) -> Self {
        self.l2_book_depth = Some(level);
        self
    }

    /// Requests a specific L2 book update interval (Binance only). Official
    /// intervals: spot `100ms`/`1000ms`, USD-M/COIN-M `100ms`/`250ms`/`500ms`.
    /// The interval is validated during feed preflight; `None` (the default)
    /// keeps the fastest `100ms` stream.
    pub fn l2_book_interval(mut self, interval: impl Into<String>) -> Self {
        self.l2_book_interval = Some(interval.into());
        self
    }

    pub fn funding(mut self) -> Self {
        self.channels.push(Channel::Funding);
        self
    }

    pub fn liquidations(mut self) -> Self {
        self.channels.push(Channel::Liquidations);
        self
    }

    pub fn mark_price(mut self) -> Self {
        self.channels.push(Channel::MarkPrice);
        self
    }

    #[cfg(feature = "candles")]
    pub fn candle_handler(mut self, handler: Arc<dyn CandleHandler>) -> Self {
        self.candle_handler = Some(handler);
        self
    }

    #[cfg(feature = "funding")]
    pub fn funding_handler(mut self, handler: Arc<dyn FundingHandler>) -> Self {
        self.funding_handler = Some(handler);
        self
    }

    #[cfg(feature = "liquidations")]
    pub fn liquidation_handler(mut self, handler: Arc<dyn LiquidationHandler>) -> Self {
        self.liquidation_handler = Some(handler);
        self
    }

    #[cfg(feature = "markprice")]
    pub fn mark_price_handler(mut self, handler: Arc<dyn MarkPriceHandler>) -> Self {
        self.mark_price_handler = Some(handler);
        self
    }

    #[cfg(feature = "ticker")]
    pub fn ticker_handler(mut self, handler: Arc<dyn TickerHandler>) -> Self {
        self.ticker_handler = Some(handler);
        self
    }

    #[cfg(feature = "trade")]
    pub fn trade_handler(mut self, handler: Arc<dyn TradeHandler>) -> Self {
        self.trade_handler = Some(handler);
        self
    }

    #[cfg(feature = "orderbook")]
    pub fn orderbook_handler(mut self, handler: Arc<dyn OrderBookHandler>) -> Self {
        self.orderbook_handler = Some(handler);
        self
    }

    #[cfg(feature = "openinterest")]
    pub fn open_interest_handler(mut self, handler: Arc<dyn OpenInterestHandler>) -> Self {
        self.open_interest_handler = Some(handler);
        self
    }

    #[cfg(feature = "index")]
    pub fn index_price_handler(mut self, handler: Arc<dyn IndexPriceHandler>) -> Self {
        self.index_price_handler = Some(handler);
        self
    }

    /// Adds normalized symbols for one channel. Repeated entries merge.
    /// Cannot be mixed with the channel shortcuts or shared symbol methods.
    pub fn subscription<I, S>(self, channel: Channel, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.subscription_instruments(
            channel,
            symbols
                .into_iter()
                .map(|symbol| Symbol::from_input(symbol.as_ref())),
        )
    }

    /// Adds typed instruments for one channel, preserving product identity.
    pub fn subscription_instruments(
        mut self,
        channel: Channel,
        symbols: impl IntoIterator<Item = Symbol>,
    ) -> Self {
        if let Some((_, existing)) = self
            .channel_subscriptions
            .iter_mut()
            .find(|(entry, _)| *entry == channel)
        {
            existing.extend(symbols);
        } else {
            self.channel_subscriptions
                .push((channel, symbols.into_iter().collect()));
        }
        self
    }

    pub fn symbol(mut self, symbol: &str) -> Self {
        self.symbols.push(Symbol::from_input(symbol));
        self
    }

    /// Appends normalized symbol names, just like repeated `symbol` calls.
    pub fn symbols<I, S>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.symbols.extend(
            symbols
                .into_iter()
                .map(|symbol| Symbol::from_input(symbol.as_ref())),
        );
        self
    }

    /// Appends typed instruments, preserving product identity from a catalog.
    pub fn instruments(mut self, symbols: impl IntoIterator<Item = Symbol>) -> Self {
        self.symbols.extend(symbols);
        self
    }

    pub fn instrument(mut self, symbol: Symbol) -> Self {
        self.symbols.push(symbol);
        self
    }

    pub fn exchange_symbol(mut self, exchange_symbol: &str) -> Self {
        self.exchange_symbols
            .push(exchange_symbol.to_ascii_uppercase());
        self
    }

    pub fn build(mut self) -> ExchangeFeed {
        let subscription_mode_conflict = !self.channel_subscriptions.is_empty()
            && (!self.channels.is_empty() || !self.symbols.is_empty());
        if !self.channel_subscriptions.is_empty() {
            let mut all_symbols: HashSet<Symbol> = self.symbols.iter().cloned().collect();
            for (channel, symbols) in &mut self.channel_subscriptions {
                let mut seen = HashSet::new();
                symbols.retain(|symbol| seen.insert(symbol.clone()));
                if !self.channels.contains(channel) {
                    self.channels.push(*channel);
                }
                for symbol in symbols {
                    if all_symbols.insert(symbol.clone()) {
                        self.symbols.push(symbol.clone());
                    }
                }
            }
        }
        let channels = &self.channels;
        let handler_names: Vec<&'static str> = channels
            .iter()
            .filter_map(|channel| handler_name_for_channel(*channel, &self))
            .collect();
        if !handler_names.is_empty() {
            tracing::warn!(
                exchange = ?self.exchange,
                channels = ?channels.iter().map(|channel| format!("{channel:?}")).collect::<Vec<_>>(),
                "feed subscribes channels without a registered handler; register handlers or consume FeedHandler::subscribe(): {handler_names:?}"
            );
        }
        ExchangeFeed {
            exchange: self.exchange,
            channels: self.channels,
            symbols: self.symbols,
            exchange_symbols: self.exchange_symbols,
            channel_subscriptions: self.channel_subscriptions,
            subscription_mode_conflict,
            candle_interval: self.candle_interval,
            l2_book_depth: self.l2_book_depth,
            l2_book_interval: self.l2_book_interval,
            #[cfg(feature = "ticker")]
            ticker_handler: self.ticker_handler,
            #[cfg(feature = "candles")]
            candle_handler: self.candle_handler,
            #[cfg(feature = "funding")]
            funding_handler: self.funding_handler,
            #[cfg(feature = "liquidations")]
            liquidation_handler: self.liquidation_handler,
            #[cfg(feature = "markprice")]
            mark_price_handler: self.mark_price_handler,
            #[cfg(feature = "trade")]
            trade_handler: self.trade_handler,
            #[cfg(feature = "orderbook")]
            orderbook_handler: self.orderbook_handler,
            #[cfg(feature = "openinterest")]
            open_interest_handler: self.open_interest_handler,
            #[cfg(feature = "index")]
            index_price_handler: self.index_price_handler,
            #[cfg(feature = "orderbook")]
            orderbook_states: self.orderbook_states,
            #[cfg(feature = "orderbook")]
            binance_book_syncs: self.binance_book_syncs,
            #[cfg(feature = "orderbook")]
            bitget_book_syncs: self.bitget_book_syncs,
            #[cfg(feature = "orderbook")]
            bybit_book_syncs: self.bybit_book_syncs,
            #[cfg(feature = "orderbook")]
            gateio_book_syncs: self.gateio_book_syncs,
            #[cfg(feature = "orderbook")]
            okx_book_syncs: self.okx_book_syncs,
            event_sender: None,
            status_sender: None,
            event_counts: None,
        }
    }
}

#[cfg(all(test, feature = "ticker"))]
mod tests {
    use super::binance::Binance;
    use cryptofeed_core::symbol::{InstrumentKind, Symbol};

    #[test]
    fn binance_builder_collects_symbols() {
        let feed = Binance::new().symbol("BTC-USDT").build();
        assert_eq!(feed.symbols.len(), 1);
    }

    #[test]
    fn builder_collects_channels() {
        let feed = Binance::new().ticker().trade().l2_book().build();
        assert_eq!(feed.channels.len(), 3);
    }

    #[test]
    fn builder_collects_explicit_exchange_symbols() {
        let feed = Binance::new()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build();
        assert_eq!(feed.exchange_symbols, vec!["BTCUSDT".to_owned()]);
    }

    #[test]
    fn builder_preserves_typed_product_identity() {
        let feed = Binance::new()
            .instrument(Symbol::perpetual("BTC", "USDT"))
            .build();

        assert_eq!(feed.symbols[0].kind(), InstrumentKind::Perpetual);
        assert_eq!(feed.product_kind().unwrap(), InstrumentKind::Perpetual);
    }

    #[test]
    fn hydrated_feed_resolves_symbols_in_both_directions() {
        let symbol = Symbol::spot("BTC", "USDT");
        let feed = Binance::new()
            .instrument(symbol.clone())
            .exchange_symbol("BTCUSDT")
            .build();

        assert_eq!(feed.exchange_symbol_for(&symbol).unwrap(), "BTCUSDT");
        assert_eq!(
            feed.normalized_symbol_for("btcusdt", InstrumentKind::Spot)
                .unwrap(),
            &symbol
        );
    }

    #[test]
    fn builder_flags_channels_without_handlers() {
        use super::handler_name_for_channel;
        use cryptofeed_core::exchange::Channel;
        let builder = Binance::new()
            .ticker()
            .trade()
            .ticker_handler(std::sync::Arc::new(NoopTickerHandler));
        assert!(handler_name_for_channel(Channel::Ticker, &builder).is_none());
        assert_eq!(
            handler_name_for_channel(Channel::Trade, &builder),
            Some("trade")
        );
    }

    struct NoopTickerHandler;
    #[async_trait::async_trait]
    impl cryptofeed_ticker::TickerHandler for NoopTickerHandler {
        async fn on_ticker(&self, _ticker: cryptofeed_ticker::Ticker) {}
    }
}
