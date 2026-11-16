//! Upbit catalog fetch and parse support.
//!
//! Owns the `upbit` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

/// Fetches and registers this venue's instruments for `product`.
pub(super) async fn fetch_registry(
    registry: &mut SymbolRegistry,
    product: InstrumentKind,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let exchange = ExchangeId::Upbit;
    if product != InstrumentKind::Spot {
        return Err(Error::UnsupportedCapability(format!(
            "{:?}/{:?} symbol discovery; Upbit exposes Korean spot quotation data only",
            exchange, product
        )));
    }
    let payload = fetch_json(
        "https://api.upbit.com/v1/market/all?is_details=true",
        refresh,
        transport,
        exchange,
    )
    .await?;
    add_korean_spot_catalog(registry, &payload, ExchangeId::Upbit, "market")?;
    Ok(())
}
