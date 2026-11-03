//! KuCoin futures quantity conversion.
//!
//! UTA V2 futures public feeds report trade `q`, `obu` level sizes and the
//! `mark-price` `oi` value in contract units (`UNIT`), not base-asset
//! quantity. The catalog supplies the per-contract face value (`unitSize`) and
//! whether the contract is linear or inverse (`isInverse`); this module applies
//! that documented metadata explicitly instead of hard-coding venue constants.
//! Spot feeds already report base quantity and must not use this conversion.

use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::InstrumentKind,
};
use rust_decimal::Decimal;

use crate::MarketInfo;

/// Converts a KuCoin futures contract quantity (trade `q`, `obu` level size or
/// `mark-price` open interest) to base-asset quantity.
///
/// - Linear contract (face value denominated in the base asset):
///   `contracts * contract_value`.
/// - Inverse coin-swap contract (face value denominated in the quote asset):
///   `contracts * contract_value / price`, where `price` is quote per base.
///
/// The conversion fails closed instead of guessing: a non-KuCoin/non-contract
/// market, missing/zero/negative or undetermined contract value metadata, a
/// face-value currency that is neither the normalized base nor quote asset, or
/// a non-positive price for an inverse contract is an error. KuCoin futures
/// report a single `unitSize` face value and no separate multiplier, so no
/// other factor is applied.
pub fn contracts_to_base(info: &MarketInfo, contracts: Decimal, price: Decimal) -> Result<Decimal> {
    if info.exchange != ExchangeId::Kucoin {
        return Err(Error::UnsupportedCapability(format!(
            "KuCoin contract quantity conversion for {:?} market",
            info.exchange
        )));
    }
    if !matches!(
        info.symbol.kind(),
        InstrumentKind::Perpetual | InstrumentKind::Futures
    ) {
        return Err(Error::UnsupportedCapability(format!(
            "KuCoin contract quantity conversion for {:?} symbol",
            info.symbol.kind()
        )));
    }
    let contract_value = info
        .contract_value
        .filter(|value| *value > Decimal::ZERO)
        .ok_or_else(|| {
            Error::UnsupportedCapability(format!(
                "KuCoin {} has no positive contract value metadata",
                info.exchange_symbol
            ))
        })?;
    let currency = info.contract_value_currency.as_deref().ok_or_else(|| {
        Error::UnsupportedCapability(format!(
            "KuCoin {} has no contract value currency metadata",
            info.exchange_symbol
        ))
    })?;
    let (base, quote) = crate::market_info::base_quote(&info.symbol).ok_or_else(|| {
        Error::MalformedData(format!(
            "KuCoin {} has no base/quote identity",
            info.exchange_symbol
        ))
    })?;
    let overflow = || {
        Error::MalformedData(format!(
            "KuCoin {} contract quantity overflow",
            info.exchange_symbol
        ))
    };
    if currency.eq_ignore_ascii_case(base) {
        contracts.checked_mul(contract_value).ok_or_else(overflow)
    } else if currency.eq_ignore_ascii_case(quote) {
        if price <= Decimal::ZERO {
            return Err(Error::InvalidConfiguration(format!(
                "KuCoin {} inverse contract conversion needs a positive price",
                info.exchange_symbol
            )));
        }
        contracts
            .checked_mul(contract_value)
            .and_then(|amount| amount.checked_div(price))
            .ok_or_else(overflow)
    } else {
        Err(Error::UnsupportedCapability(format!(
            "KuCoin {} contract value currency {currency} is neither {base} nor {quote}",
            info.exchange_symbol
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_core::symbol::Symbol;
    use rust_decimal::Decimal;

    fn info(
        exchange: ExchangeId,
        symbol: Symbol,
        native: &str,
        value: Option<Decimal>,
        currency: Option<&str>,
    ) -> MarketInfo {
        let mut info = MarketInfo::new(exchange, symbol, native);
        info.contract_value = value;
        info.contract_value_currency = currency.map(ToOwned::to_owned);
        info
    }

    fn linear(value: &str) -> MarketInfo {
        info(
            ExchangeId::Kucoin,
            Symbol::perpetual("BTC", "USDT"),
            "XBTUSDTM",
            Some(Decimal::from_str_exact(value).unwrap()),
            Some("BTC"),
        )
    }

    #[test]
    fn linear_contracts_multiply_by_the_reported_face_value() {
        // 12 XBTUSDTM contracts at the documented 0.001 BTC face value.
        assert_eq!(
            contracts_to_base(&linear("0.001"), Decimal::from(12), Decimal::ZERO).unwrap(),
            Decimal::from_str_exact("0.012").unwrap()
        );
        // ETHUSDTM has a different face value; it is never inferred from XBT.
        let eth = info(
            ExchangeId::Kucoin,
            Symbol::perpetual("ETH", "USDT"),
            "ETHUSDTM",
            Some(Decimal::from_str_exact("0.01").unwrap()),
            Some("ETH"),
        );
        assert_eq!(
            contracts_to_base(&eth, Decimal::from(3), Decimal::ZERO).unwrap(),
            Decimal::from_str_exact("0.03").unwrap()
        );
        // High-precision face values stay exact (1e-18 per contract).
        assert_eq!(
            contracts_to_base(
                &linear("0.000000000000000001"),
                Decimal::from(7),
                Decimal::ZERO
            )
            .unwrap(),
            Decimal::from_str_exact("0.000000000000000007").unwrap()
        );
        // The price is ignored for linear contracts (0 is only invalid for
        // inverse conversion).
        assert!(contracts_to_base(&linear("0.001"), Decimal::from(1), Decimal::ZERO).is_ok());
    }

    #[test]
    fn inverse_contracts_divide_by_price_and_never_use_settlement_currency() {
        let inverse = info(
            ExchangeId::Kucoin,
            Symbol::perpetual("BTC", "USD"),
            "XBTUSDM",
            Some(Decimal::ONE),
            Some("USD"),
        );
        // 3 XBTUSDM contracts are 3 USD; at 80 000 USD/BTC that is 0.0000375 BTC.
        assert_eq!(
            contracts_to_base(&inverse, Decimal::from(3), Decimal::from(80_000)).unwrap(),
            Decimal::from_str_exact("0.0000375").unwrap()
        );
        // A missing/zero price cannot produce base quantity.
        assert!(matches!(
            contracts_to_base(&inverse, Decimal::from(3), Decimal::ZERO),
            Err(Error::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn conversion_fails_closed_when_metadata_is_missing_or_unusable() {
        assert!(matches!(
            contracts_to_base(&linear("0"), Decimal::ONE, Decimal::ZERO),
            Err(Error::UnsupportedCapability(_))
        ));
        let missing_value = info(
            ExchangeId::Kucoin,
            Symbol::perpetual("BTC", "USDT"),
            "XBTUSDTM",
            None,
            Some("BTC"),
        );
        assert!(matches!(
            contracts_to_base(&missing_value, Decimal::ONE, Decimal::ZERO),
            Err(Error::UnsupportedCapability(_))
        ));
        let missing_currency = info(
            ExchangeId::Kucoin,
            Symbol::perpetual("BTC", "USDT"),
            "XBTUSDTM",
            Some(Decimal::new(1, 3)),
            None,
        );
        assert!(matches!(
            contracts_to_base(&missing_currency, Decimal::ONE, Decimal::ZERO),
            Err(Error::UnsupportedCapability(_))
        ));
        let foreign_currency = info(
            ExchangeId::Kucoin,
            Symbol::perpetual("BTC", "USDT"),
            "XBTUSDTM",
            Some(Decimal::new(1, 3)),
            Some("EUR"),
        );
        assert!(matches!(
            contracts_to_base(&foreign_currency, Decimal::ONE, Decimal::ZERO),
            Err(Error::UnsupportedCapability(_))
        ));
        let negative = info(
            ExchangeId::Kucoin,
            Symbol::perpetual("BTC", "USDT"),
            "XBTUSDTM",
            Some(Decimal::NEGATIVE_ONE),
            Some("BTC"),
        );
        assert!(matches!(
            contracts_to_base(&negative, Decimal::ONE, Decimal::ZERO),
            Err(Error::UnsupportedCapability(_))
        ));
        let spot = info(
            ExchangeId::Kucoin,
            Symbol::spot("BTC", "USDT"),
            "BTC-USDT",
            Some(Decimal::new(1, 3)),
            Some("BTC"),
        );
        assert!(matches!(
            contracts_to_base(&spot, Decimal::ONE, Decimal::ZERO),
            Err(Error::UnsupportedCapability(_))
        ));
        let other_exchange = info(
            ExchangeId::Okx,
            Symbol::perpetual("BTC", "USDT"),
            "BTC-USDT-SWAP",
            Some(Decimal::new(1, 3)),
            Some("BTC"),
        );
        assert!(matches!(
            contracts_to_base(&other_exchange, Decimal::ONE, Decimal::ZERO),
            Err(Error::UnsupportedCapability(_))
        ));
    }

    #[test]
    fn conversion_rejects_arithmetic_overflow() {
        let huge = info(
            ExchangeId::Kucoin,
            Symbol::perpetual("BTC", "USDT"),
            "XBTUSDTM",
            Some(Decimal::MAX),
            Some("BTC"),
        );
        assert!(matches!(
            contracts_to_base(&huge, Decimal::from(2), Decimal::ZERO),
            Err(Error::MalformedData(_))
        ));
    }
}
