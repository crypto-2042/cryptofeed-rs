use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum InstrumentKind {
    Spot,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Symbol {
    value: String,
    kind: InstrumentKind,
}

impl Symbol {
    pub fn spot(base: &str, quote: &str) -> Self {
        Self {
            value: format!("{}-{}", base.to_ascii_uppercase(), quote.to_ascii_uppercase()),
            kind: InstrumentKind::Spot,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::Symbol;

    #[test]
    fn normalizes_spot_symbol() {
        let symbol = Symbol::spot("btc", "usdt");
        assert_eq!(symbol.as_str(), "BTC-USDT");
    }
}
