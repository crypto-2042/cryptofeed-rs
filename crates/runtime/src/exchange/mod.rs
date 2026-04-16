pub mod binance;
pub mod coinbase;
pub mod kraken;

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};

pub struct ExchangeFeed {
    pub exchange: ExchangeId,
    pub symbols: Vec<Symbol>,
}

pub struct ExchangeFeedBuilder {
    exchange: ExchangeId,
    symbols: Vec<Symbol>,
}

impl ExchangeFeedBuilder {
    pub fn new(exchange: ExchangeId) -> Self {
        Self { exchange, symbols: Vec::new() }
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
}
