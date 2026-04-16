pub mod binance;
pub mod bitget;
pub mod coinbase;
pub mod kraken;

use std::sync::Arc;

use cryptofeed_core::{
    exchange::{Channel, ExchangeId},
    symbol::Symbol,
};
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::OrderBookHandler;
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
    #[cfg(feature = "trade")]
    pub trade_handler: Option<Arc<dyn TradeHandler>>,
    #[cfg(feature = "orderbook")]
    pub orderbook_handler: Option<Arc<dyn OrderBookHandler>>,
}

pub struct ExchangeFeedBuilder {
    exchange: ExchangeId,
    channels: Vec<Channel>,
    symbols: Vec<Symbol>,
    #[cfg(feature = "ticker")]
    ticker_handler: Option<Arc<dyn TickerHandler>>,
    #[cfg(feature = "trade")]
    trade_handler: Option<Arc<dyn TradeHandler>>,
    #[cfg(feature = "orderbook")]
    orderbook_handler: Option<Arc<dyn OrderBookHandler>>,
}

impl ExchangeFeedBuilder {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            channels: Vec::new(),
            symbols: Vec::new(),
            #[cfg(feature = "ticker")]
            ticker_handler: None,
            #[cfg(feature = "trade")]
            trade_handler: None,
            #[cfg(feature = "orderbook")]
            orderbook_handler: None,
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
            #[cfg(feature = "trade")]
            trade_handler: self.trade_handler,
            #[cfg(feature = "orderbook")]
            orderbook_handler: self.orderbook_handler,
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
