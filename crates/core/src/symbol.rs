use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum InstrumentKind {
    Spot,
    Margin,
    Perpetual,
    Futures,
    Option,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize)]
pub struct Symbol {
    value: String,
    kind: InstrumentKind,
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawSymbol {
            value: String,
            kind: InstrumentKind,
        }

        let raw = RawSymbol::deserialize(deserializer)?;
        let parsed = Symbol::from_input(&raw.value);
        let valid = if raw.kind == InstrumentKind::Margin {
            raw.value == raw.value.to_ascii_uppercase()
                && raw.value.split('-').count() == 2
                && raw.value.split('-').all(|part| !part.is_empty())
        } else {
            parsed.kind == raw.kind && parsed.value == raw.value
        };
        if !valid {
            return Err(serde::de::Error::custom(format!(
                "symbol value {} does not match kind {:?}",
                raw.value, raw.kind
            )));
        }
        Ok(Self {
            value: raw.value,
            kind: raw.kind,
        })
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.value)
    }
}

impl std::str::FromStr for Symbol {
    type Err = std::convert::Infallible;

    /// Parses a normalized symbol string (`BTC-USDT`, `BTC-USDT-PERP`,
    /// `BTC-USD-240628`, `BTC-USDT-30JUN26-70000-C`). Unrecognized shapes
    /// parse into an [`InstrumentKind::Unknown`] symbol rather than failing,
    /// mirroring [`Symbol::from_input`].
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Symbol::from_input(input))
    }
}

impl Symbol {
    /// Constructs a spot symbol. Panics if `base` or `quote` is empty —
    /// an empty component would silently poison downstream symbol matching;
    /// use [`Symbol::from_input`] for untrusted input.
    pub fn spot(base: &str, quote: &str) -> Self {
        assert!(!base.is_empty(), "Symbol::spot requires a non-empty base");
        assert!(!quote.is_empty(), "Symbol::spot requires a non-empty quote");
        Self {
            value: format!(
                "{}-{}",
                base.to_ascii_uppercase(),
                quote.to_ascii_uppercase()
            ),
            kind: InstrumentKind::Spot,
        }
    }

    /// Constructs a perpetual symbol; panics on empty components (see
    /// [`Symbol::spot`]).
    pub fn perpetual(base: &str, quote: &str) -> Self {
        assert!(
            !base.is_empty(),
            "Symbol::perpetual requires a non-empty base"
        );
        assert!(
            !quote.is_empty(),
            "Symbol::perpetual requires a non-empty quote"
        );
        Self {
            value: format!(
                "{}-{}-PERP",
                base.to_ascii_uppercase(),
                quote.to_ascii_uppercase()
            ),
            kind: InstrumentKind::Perpetual,
        }
    }

    /// Constructs a dated-futures symbol; panics on empty components (see
    /// [`Symbol::spot`]).
    pub fn futures(base: &str, quote: &str, expiry: &str) -> Self {
        assert!(
            !base.is_empty(),
            "Symbol::futures requires a non-empty base"
        );
        assert!(
            !quote.is_empty(),
            "Symbol::futures requires a non-empty quote"
        );
        assert!(
            !expiry.is_empty(),
            "Symbol::futures requires a non-empty expiry"
        );
        Self {
            value: format!(
                "{}-{}-{}",
                base.to_ascii_uppercase(),
                quote.to_ascii_uppercase(),
                expiry.to_ascii_uppercase()
            ),
            kind: InstrumentKind::Futures,
        }
    }

    /// Constructs a margin symbol. Margin instruments share the
    /// exchange-native form of their spot counterpart (e.g. OKX `BTC-USDT`
    /// under `instType=MARGIN`), so a margin symbol can never be inferred
    /// from input text — it must be constructed explicitly with the product
    /// kind. Panics on empty components (see [`Symbol::spot`]).
    pub fn margin(base: &str, quote: &str) -> Self {
        assert!(!base.is_empty(), "Symbol::margin requires a non-empty base");
        assert!(
            !quote.is_empty(),
            "Symbol::margin requires a non-empty quote"
        );
        Self {
            value: format!(
                "{}-{}",
                base.to_ascii_uppercase(),
                quote.to_ascii_uppercase()
            ),
            kind: InstrumentKind::Margin,
        }
    }

    /// Constructs an option symbol; panics on empty components (see
    /// [`Symbol::spot`]).
    pub fn option(base: &str, quote: &str, expiry: &str, strike: &str, option_type: &str) -> Self {
        assert!(!base.is_empty(), "Symbol::option requires a non-empty base");
        assert!(
            !quote.is_empty(),
            "Symbol::option requires a non-empty quote"
        );
        assert!(
            !expiry.is_empty(),
            "Symbol::option requires a non-empty expiry"
        );
        assert!(
            !strike.is_empty(),
            "Symbol::option requires a non-empty strike"
        );
        assert!(
            !option_type.is_empty(),
            "Symbol::option requires a non-empty option type"
        );
        Self {
            value: format!(
                "{}-{}-{}-{}-{}",
                base.to_ascii_uppercase(),
                quote.to_ascii_uppercase(),
                expiry.to_ascii_uppercase(),
                strike.to_ascii_uppercase(),
                option_type.to_ascii_uppercase()
            ),
            kind: InstrumentKind::Option,
        }
    }

    pub fn from_input(input: &str) -> Self {
        let normalized = input.to_ascii_uppercase().replace('_', "-");
        let parts: Vec<_> = normalized.split('-').collect();
        match parts.as_slice() {
            [_, "PERP"] | [_, "SWAP"] => Self {
                value: normalized,
                kind: InstrumentKind::Unknown,
            },
            [base, quote] if !base.is_empty() && !quote.is_empty() => Self::spot(base, quote),
            [base, quote, "PERP"] | [base, quote, "SWAP"]
                if !base.is_empty() && !quote.is_empty() =>
            {
                Self::perpetual(base, quote)
            }
            [base, quote, expiry]
                if !base.is_empty()
                    && !quote.is_empty()
                    && !expiry.is_empty()
                    && expiry.chars().all(|ch| ch.is_ascii_alphanumeric()) =>
            {
                Self::futures(base, quote, expiry)
            }
            [base, quote, expiry, strike, option_type]
                if !base.is_empty()
                    && !quote.is_empty()
                    && !expiry.is_empty()
                    && !strike.is_empty()
                    && matches!(*option_type, "C" | "P") =>
            {
                Self::option(base, quote, expiry, strike, option_type)
            }
            _ => Self {
                value: normalized,
                kind: InstrumentKind::Unknown,
            },
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub fn kind(&self) -> InstrumentKind {
        self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::{InstrumentKind, Symbol};
    use std::str::FromStr;

    #[test]
    fn displays_and_parses_as_string() {
        let symbol = Symbol::perpetual("btc", "usdt");
        assert_eq!(symbol.to_string(), "BTC-USDT-PERP");

        let parsed = Symbol::from_str("BTC-USD-240628").expect("parse");
        assert_eq!(parsed.kind(), InstrumentKind::Futures);
        assert_eq!(parsed.as_str(), "BTC-USD-240628");
        assert_eq!(format!("{parsed}"), "BTC-USD-240628");

        // Unrecognized shapes parse without failing, like `from_input`.
        let unknown = Symbol::from_str("weird").expect("lenient parse");
        assert_eq!(unknown.kind(), InstrumentKind::Unknown);
    }

    #[test]
    fn normalizes_spot_symbol() {
        let symbol = Symbol::spot("btc", "usdt");
        assert_eq!(symbol.as_str(), "BTC-USDT");
    }

    #[test]
    #[should_panic(expected = "non-empty base")]
    fn constructors_reject_empty_components() {
        let _ = Symbol::spot("", "usdt");
    }

    #[test]
    fn parses_perpetual_symbol_input() {
        let symbol = Symbol::from_input("btc-usdt-perp");
        assert_eq!(symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(symbol.kind(), InstrumentKind::Perpetual);
    }

    #[test]
    fn parses_futures_symbol_input() {
        let symbol = Symbol::from_input("btc-usdt-240628");
        assert_eq!(symbol.as_str(), "BTC-USDT-240628");
        assert_eq!(symbol.kind(), InstrumentKind::Futures);
    }

    #[test]
    fn keeps_unknown_symbol_input() {
        let symbol = Symbol::from_input("btcusd_perp");
        assert_eq!(symbol.as_str(), "BTCUSD-PERP");
        assert!(matches!(symbol.kind, InstrumentKind::Unknown));
    }

    #[test]
    fn builds_margin_symbol_with_explicit_kind() {
        let symbol = Symbol::margin("btc", "usdt");
        assert_eq!(symbol.as_str(), "BTC-USDT");
        assert_eq!(symbol.kind(), InstrumentKind::Margin);
        assert_ne!(
            Symbol::from_input("BTC-USDT").kind(),
            InstrumentKind::Margin
        );
    }

    #[test]
    fn parses_option_symbol_input() {
        let symbol = Symbol::from_input("btc-usdc-30dec22-18000-c");
        assert_eq!(symbol.as_str(), "BTC-USDC-30DEC22-18000-C");
        assert_eq!(symbol.kind(), InstrumentKind::Option);
    }

    #[test]
    fn builds_option_symbol_with_put_type() {
        let symbol = Symbol::option("btc", "usdt", "27MAR26", "70000", "P");
        assert_eq!(symbol.as_str(), "BTC-USDT-27MAR26-70000-P");
        assert_eq!(symbol.kind(), InstrumentKind::Option);
    }

    #[test]
    fn rejects_missing_symbol_component() {
        assert_eq!(Symbol::from_input("BTC-").kind(), InstrumentKind::Unknown);
    }

    #[test]
    fn serde_rejects_mismatched_value_and_kind() {
        let invalid = r#"{"value":"BTC-USDT","kind":"Option"}"#;
        assert!(serde_json::from_str::<Symbol>(invalid).is_err());

        let margin = Symbol::margin("BTC", "USDT");
        let encoded = serde_json::to_string(&margin).expect("serialize");
        assert_eq!(
            serde_json::from_str::<Symbol>(&encoded).expect("deserialize"),
            margin
        );
    }
}
