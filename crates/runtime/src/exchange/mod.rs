pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod coinbase;
pub mod gateio;
pub mod kraken;
pub mod okx;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[cfg(feature = "orderbook")]
use crate::exchange::binance::book_sync::BinanceBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::bitget::book_sync::BitgetBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::bybit::book_sync::BybitBookSync;
#[cfg(feature = "orderbook")]
use crate::exchange::okx::book_sync::OkxBookSync;
#[cfg(feature = "candles")]
use cryptofeed_candles::CandleHandler;
use cryptofeed_core::{
    exchange::{Channel, ExchangeId},
    symbol::Symbol,
};
#[cfg(feature = "funding")]
use cryptofeed_funding::FundingHandler;
#[cfg(feature = "liquidations")]
use cryptofeed_liquidations::LiquidationHandler;
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
    #[cfg(feature = "ticker")]
    pub ticker_handler: Option<Arc<dyn TickerHandler>>,
    #[cfg(feature = "candles")]
    pub candle_handler: Option<Arc<dyn CandleHandler>>,
    #[cfg(feature = "funding")]
    pub funding_handler: Option<Arc<dyn FundingHandler>>,
    #[cfg(feature = "liquidations")]
    pub liquidation_handler: Option<Arc<dyn LiquidationHandler>>,
    #[cfg(feature = "trade")]
    pub trade_handler: Option<Arc<dyn TradeHandler>>,
    #[cfg(feature = "orderbook")]
    pub orderbook_handler: Option<Arc<dyn OrderBookHandler>>,
    #[cfg(feature = "orderbook")]
    pub orderbook_states: Arc<Mutex<HashMap<String, L2BookState>>>,
    #[cfg(feature = "orderbook")]
    pub binance_book_syncs: Arc<Mutex<HashMap<String, BinanceBookSync>>>,
    #[cfg(feature = "orderbook")]
    pub bitget_book_syncs: Arc<Mutex<HashMap<String, BitgetBookSync>>>,
    #[cfg(feature = "orderbook")]
    pub bybit_book_syncs: Arc<Mutex<HashMap<String, BybitBookSync>>>,
    #[cfg(feature = "orderbook")]
    pub okx_book_syncs: Arc<Mutex<HashMap<String, OkxBookSync>>>,
}

pub struct ExchangeFeedBuilder {
    exchange: ExchangeId,
    channels: Vec<Channel>,
    symbols: Vec<Symbol>,
    #[cfg(feature = "ticker")]
    ticker_handler: Option<Arc<dyn TickerHandler>>,
    #[cfg(feature = "candles")]
    candle_handler: Option<Arc<dyn CandleHandler>>,
    #[cfg(feature = "funding")]
    funding_handler: Option<Arc<dyn FundingHandler>>,
    #[cfg(feature = "liquidations")]
    liquidation_handler: Option<Arc<dyn LiquidationHandler>>,
    #[cfg(feature = "trade")]
    trade_handler: Option<Arc<dyn TradeHandler>>,
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
    okx_book_syncs: Arc<Mutex<HashMap<String, OkxBookSync>>>,
}

impl ExchangeFeedBuilder {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            channels: Vec::new(),
            symbols: Vec::new(),
            #[cfg(feature = "ticker")]
            ticker_handler: None,
            #[cfg(feature = "candles")]
            candle_handler: None,
            #[cfg(feature = "funding")]
            funding_handler: None,
            #[cfg(feature = "liquidations")]
            liquidation_handler: None,
            #[cfg(feature = "trade")]
            trade_handler: None,
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

    pub fn candles(mut self) -> Self {
        self.channels.push(Channel::Candles);
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

    pub fn symbol(mut self, symbol: &str) -> Self {
        let parts: Vec<_> = symbol.split('-').collect();
        let normalized = Symbol::spot(parts[0], parts[1]);
        self.symbols.push(normalized);
        self
    }

    pub fn build(self) -> ExchangeFeed {
        ExchangeFeed {
            exchange: self.exchange,
            channels: self.channels,
            symbols: self.symbols,
            #[cfg(feature = "ticker")]
            ticker_handler: self.ticker_handler,
            #[cfg(feature = "candles")]
            candle_handler: self.candle_handler,
            #[cfg(feature = "funding")]
            funding_handler: self.funding_handler,
            #[cfg(feature = "liquidations")]
            liquidation_handler: self.liquidation_handler,
            #[cfg(feature = "trade")]
            trade_handler: self.trade_handler,
            #[cfg(feature = "orderbook")]
            orderbook_handler: self.orderbook_handler,
            #[cfg(feature = "orderbook")]
            orderbook_states: self.orderbook_states,
            #[cfg(feature = "orderbook")]
            binance_book_syncs: self.binance_book_syncs,
            #[cfg(feature = "orderbook")]
            bitget_book_syncs: self.bitget_book_syncs,
            #[cfg(feature = "orderbook")]
            bybit_book_syncs: self.bybit_book_syncs,
            #[cfg(feature = "orderbook")]
            okx_book_syncs: self.okx_book_syncs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::binance::Binance;

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
}
