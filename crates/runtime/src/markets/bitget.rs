//! Bitget catalog fetch and parse support.
//!
//! Owns the `bitget` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) fn add_bitget_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<()> {
    for item in array_at(payload, &["data"])? {
        if !catalog_status(item, "status", &["online", "limit_open", "limit_close"])? {
            continue;
        }
        let contract_type = required_str(item, "type")?;
        let item_product = match contract_type {
            "perpetual" => InstrumentKind::Perpetual,
            "delivery" => InstrumentKind::Futures,
            other => {
                return Err(Error::MalformedData(format!(
                    "unknown Bitget futures type {other}"
                )));
            }
        };
        if item_product != product {
            continue;
        }
        let exchange_symbol = required_str(item, "symbol")?;
        let base = required_str(item, "baseCoin")?;
        let quote = required_str(item, "quoteCoin")?;
        let symbol = match product {
            InstrumentKind::Perpetual => Symbol::perpetual(base, quote),
            InstrumentKind::Futures => {
                let expiry = expiry_from_millis(required_str(item, "deliveryTime")?)?;
                Symbol::futures(base, quote, &expiry)
            }
            _ => continue,
        };
        registry.insert_market(symbol, exchange_symbol, ExchangeId::Bitget, item, None)?;
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
    let exchange = ExchangeId::Bitget;
    let categories: &[&str] = if product == InstrumentKind::Spot {
        &["SPOT"]
    } else {
        &["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]
    };
    for category in categories {
        let url = format!("https://api.bitget.com/api/v3/market/instruments?category={category}");
        let payload = fetch_json(&url, refresh, transport, exchange).await?;
        if product == InstrumentKind::Spot {
            add_spot_catalog(registry, &payload, ExchangeId::Bitget)?;
        } else {
            add_bitget_markets(registry, &payload, product)?;
        }
    }
    Ok(())
}
