//! Kucoin catalog fetch and parse support.
//!
//! Owns the `kucoin` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

/// KuCoin base aliases used before product qualification (`XBT` -> `BTC`).
pub(super) fn kucoin_alias_asset(asset: &str) -> &str {
    if asset.eq_ignore_ascii_case("XBT") {
        "BTC"
    } else {
        asset
    }
}

pub(super) fn add_kucoin_spot_catalog(
    registry: &mut SymbolRegistry,
    payload: &Value,
) -> Result<()> {
    for item in array_at(payload, &["data", "list"])? {
        if required_str(item, "tradingStatus")? != "TradingEnabled" {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "baseCurrency")?;
        let quote = required_str(item, "quoteCurrency")?;
        let expected = format!("{base}-{quote}");
        if !exchange_symbol.eq_ignore_ascii_case(&expected) {
            return Err(Error::MalformedData(format!(
                "KuCoin spot instrument {exchange_symbol} disagrees with {expected}"
            )));
        }
        registry.insert_market(
            Symbol::spot(kucoin_alias_asset(base), quote),
            exchange_symbol,
            ExchangeId::Kucoin,
            item,
            None,
        )?;
    }
    Ok(())
}

/// KuCoin `tradeType=FUTURES` catalog. `contractType: "0"` with a null
/// `expiryTime` is a perpetual; `contractType: "1"` with an expiry is a dated
/// contract. `marketStage` other than `NORMAL` (pre-market) is skipped: the
/// protocol baseline only covers the normal market.
pub(super) fn add_kucoin_futures_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["data", "list"])? {
        if required_str(item, "tradingStatus")? != "Open" {
            continue;
        }
        if required_str(item, "marketStage")? != "NORMAL" {
            continue;
        }
        let contract_type = required_str(item, "contractType")?;
        let expiry = str_at(item, &["expiryTime"]).filter(|value| !value.is_empty());
        let item_product = match contract_type {
            "0" if expiry.is_none() => InstrumentKind::Perpetual,
            "1" if expiry.is_some() => InstrumentKind::Futures,
            _ => {
                return Err(Error::MalformedData(format!(
                    "KuCoin futures contract {} has contractType {contract_type} with expiry {expiry:?}",
                    required_str(item, "symbol")?
                )));
            }
        };
        if item_product != product {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "baseCurrency")?;
        let quote = required_str(item, "quoteCurrency")?;
        let symbol = match item_product {
            InstrumentKind::Perpetual => {
                let expected = format!("{base}{quote}M");
                if !exchange_symbol.eq_ignore_ascii_case(&expected) {
                    return Err(Error::MalformedData(format!(
                        "KuCoin perpetual {exchange_symbol} disagrees with {expected}"
                    )));
                }
                Symbol::perpetual(kucoin_alias_asset(base), quote)
            }
            InstrumentKind::Futures => {
                let expiry = expiry.ok_or_else(|| {
                    Error::MalformedData(format!(
                        "KuCoin dated contract {exchange_symbol} has no expiryTime"
                    ))
                })?;
                Symbol::futures(
                    kucoin_alias_asset(base),
                    quote,
                    &expiry_from_millis(expiry)?,
                )
            }
            _ => continue,
        };
        registry.insert_market(symbol, exchange_symbol, ExchangeId::Kucoin, item, None)?;
    }
    Ok(())
}

/// Fetches and registers this venue's instruments for `product`.
pub(super) async fn fetch_registry(
    registry: &mut SymbolRegistry,
    product: InstrumentKind,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let exchange = ExchangeId::Kucoin;
    match product {
        InstrumentKind::Spot => {
            let payload = fetch_json(
                "https://api.kucoin.com/api/ua/v2/market/instrument?tradeType=SPOT",
                refresh,
                transport,
                exchange,
            )
            .await?;
            add_kucoin_spot_catalog(registry, &payload)?;
        }
        InstrumentKind::Perpetual | InstrumentKind::Futures => {
            let payload = fetch_json(
                "https://api.kucoin.com/api/ua/v2/market/instrument?tradeType=FUTURES",
                refresh,
                transport,
                exchange,
            )
            .await?;
            add_kucoin_futures_markets(registry, &payload, product)?;
        }
        InstrumentKind::Unknown | InstrumentKind::Option | InstrumentKind::Margin => {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?} symbol discovery",
                exchange, product
            )));
        }
        _ => {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?} symbol discovery",
                exchange, product
            )));
        }
    }
    Ok(())
}
