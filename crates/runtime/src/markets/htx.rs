//! Htx catalog fetch and parse support.
//!
//! Owns the `htx` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) async fn fetch_htx_spot_catalog(
    registry: &mut SymbolRegistry,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let payload = fetch_json(
        "https://api.huobi.pro/v1/common/symbols",
        refresh,
        transport,
        ExchangeId::Htx,
    )
    .await?;
    add_htx_spot_catalog(registry, &payload)
}

pub(super) fn add_htx_spot_catalog(registry: &mut SymbolRegistry, payload: &Value) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        if !catalog_status(item, "state", &["online"])? {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "base-currency")?;
        let quote = required_str(item, "quote-currency")?;
        let expected = format!(
            "{}{}",
            base.to_ascii_lowercase(),
            quote.to_ascii_lowercase()
        );
        if !exchange_symbol.eq_ignore_ascii_case(&expected) {
            return Err(Error::MalformedData(format!(
                "HTX spot symbol {exchange_symbol} disagrees with {expected}"
            )));
        }
        registry.insert_market(
            Symbol::spot(base, quote),
            exchange_symbol,
            ExchangeId::Htx,
            item,
            None,
        )?;
    }
    Ok(())
}

pub(super) async fn fetch_htx_swap_catalog(
    registry: &mut SymbolRegistry,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let payload = fetch_json(
        "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info",
        refresh,
        transport,
        ExchangeId::Htx,
    )
    .await?;
    add_htx_swap_markets(registry, &payload)
}

pub(super) fn add_htx_swap_markets(registry: &mut SymbolRegistry, payload: &Value) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        // The swap endpoint also serves delivery contracts; the feed is
        // perpetual-only and must not mix them.
        if str_at(item, &["business_type"]) != Some("swap")
            || str_at(item, &["contract_type"]) != Some("swap")
        {
            continue;
        }
        // contract_status 1 means settlement finished / trading resumed; the
        // documented settlement period codes (5..8) are not subscribable.
        let status = item.get("contract_status").and_then(Value::as_i64);
        if status != Some(1) {
            continue;
        }
        let exchange_symbol = required_str(item, "contract_code")?;
        let pair = required_str(item, "pair")?;
        let (base, quote) = pair
            .split_once('-')
            .filter(|(base, quote)| !base.is_empty() && !quote.is_empty() && !quote.contains('-'))
            .ok_or_else(|| {
                Error::MalformedData(format!("HTX swap pair {pair} is not BASE-QUOTE"))
            })?;
        let expected = format!("{base}-{quote}");
        if !exchange_symbol.eq_ignore_ascii_case(&expected) {
            return Err(Error::MalformedData(format!(
                "HTX swap contract {exchange_symbol} disagrees with {expected}"
            )));
        }
        registry.insert_market(
            Symbol::perpetual(base, quote),
            exchange_symbol,
            ExchangeId::Htx,
            item,
            None,
        )?;
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
    let exchange = ExchangeId::Htx;
    match product {
        InstrumentKind::Spot => {
            fetch_htx_spot_catalog(registry, refresh, transport).await?;
        }
        InstrumentKind::Perpetual => {
            fetch_htx_swap_catalog(registry, refresh, transport).await?;
        }
        _ => {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?} symbol discovery; HTX USDT-M delivery contracts are out of scope",
                exchange, product
            )));
        }
    }
    Ok(())
}
