//! Deribit catalog fetch and parse support.
//!
//! Owns the `deribit` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) async fn fetch_deribit_catalog(
    registry: &mut SymbolRegistry,
    product: InstrumentKind,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    // Public JSON-RPC method over the documented REST gateway; `kind=future`
    // covers both perpetuals and dated futures, options are filtered out.
    let payload = fetch_json(
        "https://www.deribit.com/api/v2/public/get_instruments?currency=any&kind=future&expired=false",
        refresh,
        transport,
        ExchangeId::Deribit,
    )
    .await?;
    add_deribit_markets(registry, &payload, product)
}

pub(super) fn add_deribit_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["result"])? {
        // Options/combos are never registered.
        if required_str(item, "kind")? != "future" {
            continue;
        }
        if !catalog_status(item, "state", &["open"])? {
            continue;
        }
        if item.get("is_active").and_then(Value::as_bool) == Some(false) {
            continue;
        }
        let exchange_symbol = required_str(item, "instrument_name")?;
        let base = required_str(item, "base_currency")?;
        let quote = required_str(item, "quote_currency")?;
        let stem = exchange_symbol
            .split_once('-')
            .map_or(exchange_symbol, |(stem, _)| stem);
        let native_base = stem.split_once('_').map_or(stem, |(base, _)| base);
        if !native_base.eq_ignore_ascii_case(base) {
            return Err(Error::MalformedData(format!(
                "Deribit instrument {exchange_symbol} disagrees with base {base}"
            )));
        }
        let perpetual = str_at(item, &["settlement_period"]) == Some("perpetual");
        let symbol = match product {
            InstrumentKind::Perpetual => {
                if !perpetual {
                    continue;
                }
                Symbol::perpetual(base, quote)
            }
            InstrumentKind::Futures => {
                if perpetual {
                    continue;
                }
                let expires = item
                    .get("expiration_timestamp")
                    .and_then(Value::as_i64)
                    .filter(|expires| *expires > 0)
                    .ok_or_else(|| {
                        Error::MalformedData(format!(
                            "Deribit dated instrument {exchange_symbol} has no expiration"
                        ))
                    })?;
                Symbol::futures(base, quote, &expiry_from_millis(&expires.to_string())?)
            }
            _ => continue,
        };
        registry.insert_market(symbol, exchange_symbol, ExchangeId::Deribit, item, None)?;
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
    let exchange = ExchangeId::Deribit;
    if !matches!(product, InstrumentKind::Perpetual | InstrumentKind::Futures) {
        return Err(Error::UnsupportedCapability(format!(
            "{:?}/{:?} symbol discovery; Deribit derivatives are perpetual/dates futures only",
            exchange, product
        )));
    }
    fetch_deribit_catalog(registry, product, refresh, transport).await?;
    Ok(())
}
