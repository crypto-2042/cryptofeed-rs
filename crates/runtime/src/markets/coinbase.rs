//! Coinbase catalog fetch and parse support.
//!
//! Owns the `coinbase` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) async fn fetch_coinbase_spot_catalog(
    registry: &mut SymbolRegistry,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    // Public Advanced Trade catalog; SPOT only. Cursor pagination is documented
    // as a base64 cursor with `pagination.has_next` / `next_cursor`.
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    loop {
        let mut url = url::Url::parse("https://api.coinbase.com/api/v3/brokerage/market/products")
            .map_err(|error| Error::Transport(error.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("product_type", "SPOT");
            query.append_pair("limit", "250");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let payload = fetch_json(url.as_str(), refresh, transport, ExchangeId::Coinbase).await?;
        add_coinbase_spot_page(registry, &payload)?;
        let has_next = payload
            .pointer("/pagination/has_next")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !has_next {
            return Ok(());
        }
        let next = str_at(&payload, &["pagination", "next_cursor"])
            .filter(|next| !next.is_empty())
            .ok_or_else(|| {
                Error::MalformedData("Coinbase catalog page is missing next_cursor".to_owned())
            })?;
        if !seen.insert(next.to_owned()) {
            return Err(Error::MalformedData(
                "Coinbase catalog cursor loop".to_owned(),
            ));
        }
        cursor = Some(next.to_owned());
    }
}

pub(super) fn add_coinbase_spot_page(registry: &mut SymbolRegistry, payload: &Value) -> Result<()> {
    for item in array_at(payload, &["products"])? {
        // The request already filters SPOT; keep the explicit check so a
        // derivative row can never be registered as a spot symbol.
        if str_at(item, &["product_type"]) != Some("SPOT") {
            continue;
        }
        if !catalog_status(item, "status", &["online"])? {
            continue;
        }
        let exchange_symbol = required_str(item, "product_id")?;
        let base = required_str(item, "base_currency_id")?;
        let quote = required_str(item, "quote_currency_id")?;
        let expected = format!(
            "{}-{}",
            base.to_ascii_uppercase(),
            quote.to_ascii_uppercase()
        );
        if !exchange_symbol.eq_ignore_ascii_case(&expected) {
            return Err(Error::MalformedData(format!(
                "Coinbase product {exchange_symbol} disagrees with {expected}"
            )));
        }
        registry.insert_market(
            Symbol::spot(base, quote),
            exchange_symbol,
            ExchangeId::Coinbase,
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
    let exchange = ExchangeId::Coinbase;
    if product != InstrumentKind::Spot {
        return Err(Error::UnsupportedCapability(format!(
            "{:?}/{:?} symbol discovery; the public Advanced Trade feed is spot-only",
            exchange, product
        )));
    }
    fetch_coinbase_spot_catalog(registry, refresh, transport).await?;
    Ok(())
}
