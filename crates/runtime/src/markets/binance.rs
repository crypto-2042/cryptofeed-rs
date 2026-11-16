//! Binance catalog fetch and parse support.
//!
//! Owns the `binance` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) fn add_binance_option_market(registry: &mut SymbolRegistry, item: &Value) -> Result<()> {
    // Native form: {BASE}-{YYMMDD}-{STRIKE}-{C|P}. The current eapi
    // `optionSymbols` response carries `underlying` (e.g. `BTCUSDT`) and
    // `quoteAsset` instead of a separate `baseAsset` (verified 2026-08-06);
    // the base is derived by stripping the quote suffix from the underlying.
    let exchange_symbol = required_str(item, "symbol")?;
    let parts: Vec<_> = exchange_symbol.split('-').collect();
    let [base_part, expiry, strike, option_type] = parts.as_slice() else {
        return Err(Error::MalformedData(format!(
            "invalid Binance option symbol {exchange_symbol}"
        )));
    };
    if !matches!(*option_type, "C" | "P") {
        return Err(Error::MalformedData(format!(
            "invalid Binance option type in {exchange_symbol}"
        )));
    }
    let Some(quote) = str_at(item, &["quoteAsset"]) else {
        // Rows without a quote currency cannot be normalized faithfully;
        // skip them instead of failing the whole catalog.
        return Ok(());
    };
    let underlying = str_at(item, &["underlying"]).unwrap_or(base_part);
    let base = underlying
        .strip_suffix(quote)
        .filter(|base| !base.is_empty())
        .unwrap_or(underlying);
    if *base_part != base {
        return Err(Error::MalformedData(format!(
            "Binance option {exchange_symbol} disagrees with underlying {underlying}"
        )));
    }
    registry.insert(
        Symbol::option(base, quote, expiry, strike, option_type),
        exchange_symbol,
    )
}

pub(super) fn add_binance_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["symbols"])? {
        let status = if item.get("contractStatus").is_some() {
            "contractStatus"
        } else {
            "status"
        };
        if !catalog_status(item, status, &["TRADING"])? {
            continue;
        }
        let contract_type = required_str(item, "contractType")?;
        let item_product = if contract_type == "PERPETUAL" {
            InstrumentKind::Perpetual
        } else {
            InstrumentKind::Futures
        };
        if item_product != product {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "baseAsset")?;
        let quote = required_str(item, "quoteAsset")?;
        let symbol = match product {
            InstrumentKind::Perpetual => Symbol::perpetual(base, quote),
            InstrumentKind::Futures => dated_symbol(
                base,
                quote,
                exchange_symbol,
                str_at(item, &["deliveryDate"]),
                "Binance",
            )?,
            _ => continue,
        };
        registry.insert_market(symbol, exchange_symbol, ExchangeId::Binance, item, None)?;
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
    let exchange = ExchangeId::Binance;
    if product == InstrumentKind::Spot {
        let payload = fetch_json(
            "https://api.binance.com/api/v3/exchangeInfo",
            refresh,
            transport,
            exchange,
        )
        .await?;
        add_spot_catalog(registry, &payload, ExchangeId::Binance)?;
    } else if product == InstrumentKind::Option {
        let payload = fetch_json(
            "https://eapi.binance.com/eapi/v1/exchangeInfo",
            refresh,
            transport,
            exchange,
        )
        .await?;
        for item in array_at(&payload, &["optionSymbols"])? {
            add_binance_option_market(registry, item)?;
        }
    } else {
        for url in [
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "https://dapi.binance.com/dapi/v1/exchangeInfo",
        ] {
            let payload = fetch_json(url, refresh, transport, exchange).await?;
            add_binance_markets(registry, &payload, product)?;
        }
    }
    Ok(())
}
