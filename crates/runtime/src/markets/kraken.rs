//! Kraken catalog fetch and parse support.
//!
//! Owns the `kraken` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

/// Legacy Kraken asset codes that the WebSocket v2 symbol namespace spells
/// differently from the REST `AssetPairs` `wsname`. Verified live on
/// 2026-11-03: `XBT/USD` and `XDG/USD` are rejected as unsupported while
/// `BTC/USD` and `DOGE/USD` subscribe. The alias is applied to both sides of
/// the pair, so an `AAVE/XBT` pair subscribes as `AAVE/BTC`.
pub(super) fn kraken_wire_asset(asset: &str) -> &str {
    match asset {
        "XBT" => "BTC",
        "XDG" => "DOGE",
        other => other,
    }
}

/// Kraken `AssetPairs` spot catalog. `wsname` is the canonical pair form used
/// by WebSocket v2 after the legacy `XBT`/`XDG` aliases; `altname` is the
/// slash-less legacy form and is cross-checked so a catalog contradiction is
/// explicit instead of being guessed around. Only `status: online` pairs are
/// eligible; `cancel_only`/`post_only` are not tradable market-data
/// subscriptions and are skipped.
pub(super) fn add_kraken_spot_catalog(
    registry: &mut SymbolRegistry,
    payload: &Value,
) -> Result<()> {
    let rows = payload
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::MalformedData("Kraken AssetPairs is missing result".to_owned()))?;
    for (native_key, item) in rows {
        if required_str(item, "status")? != "online" {
            continue;
        }
        let wsname = required_str(item, "wsname")?;
        let altname = required_str(item, "altname")?;
        if altname != wsname.replace('/', "") {
            return Err(Error::MalformedData(format!(
                "Kraken pair {native_key} altname {altname} disagrees with wsname {wsname}"
            )));
        }
        let (base, quote) = wsname.split_once('/').ok_or_else(|| {
            Error::MalformedData(format!(
                "Kraken pair {native_key} has invalid wsname {wsname}"
            ))
        })?;
        if base.is_empty() || quote.is_empty() || quote.contains('/') {
            return Err(Error::MalformedData(format!(
                "Kraken pair {native_key} has invalid wsname {wsname}"
            )));
        }
        let base = kraken_wire_asset(base);
        let quote = kraken_wire_asset(quote);
        registry.insert_market(
            Symbol::spot(base, quote),
            &format!("{base}/{quote}"),
            ExchangeId::Kraken,
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
    let exchange = ExchangeId::Kraken;
    if product != InstrumentKind::Spot {
        return Err(Error::UnsupportedCapability(format!(
            "{:?}/{:?} symbol discovery; Kraken derivatives live on a separate platform",
            exchange, product
        )));
    }
    let payload = fetch_json(
        "https://api.kraken.com/0/public/AssetPairs",
        refresh,
        transport,
        exchange,
    )
    .await?;
    add_kraken_spot_catalog(registry, &payload)?;
    Ok(())
}
