//! Product-qualified symbol discovery and local pattern selection.

use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::{InstrumentKind, Symbol},
};

/// A snapshot of normalized symbols from the exchange's instrument catalog.
/// Loading uses the existing 24-hour HTTP cache. It does not refresh running
/// subscriptions or automatically discover new listings in the background.
#[derive(Debug)]
pub struct MarketCatalog {
    symbols: Vec<Symbol>,
}

impl MarketCatalog {
    /// Loads one enabled exchange/product catalog, without opening WebSockets.
    /// Unsupported products are rejected before any HTTP request.
    pub async fn load(exchange: ExchangeId, product: InstrumentKind) -> Result<Self> {
        if !crate::markets::capability_matrix()
            .iter()
            .any(|entry| entry.exchange == exchange && entry.product == product)
        {
            return Err(Error::UnsupportedCapability(format!(
                "{exchange:?}/{product:?} catalog"
            )));
        }
        let registry = crate::markets::fetch_symbol_registry(exchange, product).await?;
        Ok(Self {
            symbols: registry.into_symbols(),
        })
    }

    /// All discovered symbols, sorted by normalized name.
    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    /// Selects a sorted, deduplicated union of normalized-name patterns.
    /// Matching is case-insensitive: `*` matches zero or more characters and
    /// `?` matches one character. Other characters are literal; character
    /// classes and escaping are not supported. Each pattern must match at
    /// least one symbol, and an empty pattern list is an error.
    pub fn select(&self, patterns: &[&str]) -> Result<Vec<Symbol>> {
        if patterns.is_empty() {
            return Err(Error::InvalidConfiguration(
                "at least one symbol pattern is required".to_owned(),
            ));
        }
        let mut selected = vec![false; self.symbols.len()];
        for pattern in patterns {
            let pattern = pattern.to_ascii_uppercase();
            let mut matched = false;
            for (index, symbol) in self.symbols.iter().enumerate() {
                if matches_pattern(&pattern, symbol.as_str()) {
                    selected[index] = true;
                    matched = true;
                }
            }
            if !matched {
                return Err(Error::UnsupportedSymbol(pattern));
            }
        }
        Ok(self
            .symbols
            .iter()
            .zip(selected)
            .filter(|(_, selected)| *selected)
            .map(|(symbol, _)| symbol.clone())
            .collect())
    }
}

fn matches_pattern(pattern: &str, value: &str) -> bool {
    let pattern: Vec<_> = pattern.chars().collect();
    let value: Vec<_> = value.chars().collect();
    let (mut p, mut v) = (0, 0);
    let mut star = None;
    let mut retry = 0;
    while v < value.len() {
        if p < pattern.len() && pattern[p] == '*' {
            star = Some(p);
            p += 1;
            retry = v;
        } else if p < pattern.len() && (pattern[p] == '?' || pattern[p] == value[v]) {
            p += 1;
            v += 1;
        } else if let Some(index) = star {
            retry += 1;
            v = retry;
            p = index + 1;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::markets::SymbolRegistry;

    fn catalog() -> MarketCatalog {
        let mut registry = SymbolRegistry::default();
        for (base, quote) in [
            ("SOL", "USDT"),
            ("BTC", "USDC"),
            ("BTC", "USDT"),
            ("ETH", "USDT"),
        ] {
            registry
                .insert(Symbol::spot(base, quote), &format!("{base}{quote}"))
                .unwrap();
        }
        MarketCatalog {
            symbols: registry.into_symbols(),
        }
    }

    #[test]
    fn selection_sorts_deduplicates_and_normalizes_patterns() {
        let catalog = catalog();
        let selected = catalog.select(&["btc-*", "*-usdt", "BTC-USDT"]).unwrap();
        assert_eq!(selected, catalog.symbols());
        assert_eq!(
            selected.iter().map(Symbol::as_str).collect::<Vec<_>>(),
            ["BTC-USDC", "BTC-USDT", "ETH-USDT", "SOL-USDT"]
        );
        assert_eq!(
            catalog.select(&["?TC-USDT"]).unwrap(),
            [Symbol::spot("BTC", "USDT")]
        );
    }

    #[test]
    fn missing_pattern_never_silently_yields_a_partial_subscription() {
        let catalog = catalog();
        assert!(matches!(
            catalog.select(&["BTC-USDT", "DOGE-*"]),
            Err(Error::UnsupportedSymbol(_))
        ));
        assert!(matches!(
            catalog.select(&[]),
            Err(Error::InvalidConfiguration(_))
        ));
        assert!(catalog.select(&[""]).is_err());
    }

    #[test]
    fn patterns_are_anchored_and_backtrack_over_stars() {
        for (pattern, value, expected) in [
            ("*", "", true),
            ("?", "", false),
            ("BTC", "BTC-USDT", false),
            ("*BTC", "BTC-USDT", false),
            ("B**-U*?T", "BTC-USDT", true),
            ("*A?B", "AAAB", true),
            ("[BE]TC-*", "BTC-USDT", false),
            ("?", "币", true),
        ] {
            assert_eq!(
                matches_pattern(pattern, value),
                expected,
                "{pattern}/{value}"
            );
        }
    }
}
