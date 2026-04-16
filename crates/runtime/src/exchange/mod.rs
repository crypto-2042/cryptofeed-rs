pub mod binance;
pub mod coinbase;
pub mod kraken;

use cryptofeed_core::{
    exchange::{Channel, ExchangeId},
    symbol::Symbol,
};

pub struct ExchangeFeed {
    pub exchange: ExchangeId,
    pub channels: Vec<Channel>,
    pub symbols: Vec<Symbol>,
}

pub struct ExchangeFeedBuilder {
    exchange: ExchangeId,
    channels: Vec<Channel>,
    symbols: Vec<Symbol>,
}

impl ExchangeFeedBuilder {
    pub fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            channels: Vec::new(),
            symbols: Vec::new(),
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
