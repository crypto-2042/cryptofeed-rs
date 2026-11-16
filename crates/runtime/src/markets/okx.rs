//! Okx catalog fetch and parse support.
//!
//! Owns the `okx` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) fn add_okx_spot_markets(registry: &mut SymbolRegistry, payload: &Value) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        if str_at(item, &["state"]).is_some_and(|state| state != "live") {
            continue;
        }
        add_spot_market(
            registry,
            ExchangeId::Okx,
            item,
            str_at(item, &["instId"]),
            str_at(item, &["baseCcy"]),
            str_at(item, &["quoteCcy"]),
        )?;
    }
    Ok(())
}

pub(super) fn add_okx_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        if str_at(item, &["state"]).is_some_and(|state| state != "live") {
            continue;
        }
        let inst_type = required_str(item, "instType")?;
        let expected_type = match product {
            InstrumentKind::Perpetual => "SWAP",
            InstrumentKind::Futures => "FUTURES",
            InstrumentKind::Option => "OPTION",
            _ => continue,
        };
        if inst_type != expected_type {
            continue;
        }
        if product == InstrumentKind::Futures
            && str_at(item, &["ruleType"]).is_some_and(|rule| rule != "normal")
        {
            continue;
        }

        let exchange_symbol = required_str(item, "instId")?;
        let mut parts = exchange_symbol.split('-');
        let base = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or_else(|| {
                Error::MalformedData(format!("invalid OKX instrument {exchange_symbol}"))
            })?;
        let quote = parts
            .next()
            .filter(|part| !part.is_empty())
            .ok_or_else(|| {
                Error::MalformedData(format!("invalid OKX instrument {exchange_symbol}"))
            })?;
        let symbol = match product {
            InstrumentKind::Perpetual => Symbol::perpetual(base, quote),
            InstrumentKind::Futures => dated_symbol(
                base,
                quote,
                exchange_symbol,
                str_at(item, &["expTime"]),
                "OKX",
            )?,
            InstrumentKind::Option => {
                // OKX option instIds are {BASE}-{QUOTE}-{EXPIRY}-{STRIKE}-{C|P}.
                let parts: Vec<_> = exchange_symbol.split('-').collect();
                let [base, quote, expiry, strike, option_type] = parts.as_slice() else {
                    return Err(Error::MalformedData(format!(
                        "invalid OKX option instrument {exchange_symbol}"
                    )));
                };
                if !matches!(*option_type, "C" | "P") {
                    return Err(Error::MalformedData(format!(
                        "invalid OKX option type in {exchange_symbol}"
                    )));
                }
                Symbol::option(base, quote, expiry, strike, option_type)
            }
            _ => continue,
        };
        registry.insert_market(symbol, exchange_symbol, ExchangeId::Okx, item, None)?;
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
    let exchange = ExchangeId::Okx;
    let inst_type = match product {
        InstrumentKind::Spot => "SPOT",
        InstrumentKind::Perpetual => "SWAP",
        InstrumentKind::Futures => "FUTURES",
        InstrumentKind::Option => "OPTION",
        InstrumentKind::Margin | InstrumentKind::Unknown => {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?} symbol discovery; margin instruments share the spot instId form and require explicit exchange_symbol pairs",
                exchange, product
            )));
        }
        _ => {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?} symbol discovery",
                exchange, product
            )));
        }
    };
    let url = format!("https://openapi.okx.com/api/v5/public/instruments?instType={inst_type}");
    let payload = fetch_json(&url, refresh, transport, exchange).await?;
    if product == InstrumentKind::Spot {
        add_okx_spot_markets(registry, &payload)?;
    } else {
        add_okx_markets(registry, &payload, product)?;
    }
    Ok(())
}
