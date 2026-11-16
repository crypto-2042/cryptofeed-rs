//! Gateio catalog fetch and parse support.
//!
//! Owns the `gateio` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) fn add_gateio_perpetual_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
    settle: &str,
) -> Result<()> {
    let (expected_type, expected_quote) = match settle {
        "usdt" => ("direct", "USDT"),
        "btc" => ("inverse", "USD"),
        other => {
            return Err(Error::InvalidConfiguration(format!(
                "unsupported Gateio futures settlement {other}"
            )));
        }
    };

    for item in gateio_contracts(payload)? {
        if item
            .get("in_delisting")
            .and_then(Value::as_bool)
            .is_some_and(|value| value)
        {
            continue;
        }
        let exchange_symbol = required_str(item, "name")?;
        let contract_type = required_str(item, "type")?;
        let (base, quote) = gateio_pair(exchange_symbol)?;
        if contract_type != expected_type || quote != expected_quote {
            return Err(Error::MalformedData(format!(
                "Gateio {settle} contract {exchange_symbol} has type {contract_type} and quote {quote}"
            )));
        }
        registry.insert_market(
            Symbol::perpetual(base, quote),
            exchange_symbol,
            ExchangeId::Gateio,
            item,
            Some(settle),
        )?;
    }
    Ok(())
}

pub(super) fn add_gateio_delivery_markets(
    registry: &mut SymbolRegistry,
    payload: &Value,
) -> Result<()> {
    for item in gateio_contracts(payload)? {
        if item
            .get("in_delisting")
            .and_then(Value::as_bool)
            .is_some_and(|value| value)
        {
            continue;
        }
        let exchange_symbol = required_str(item, "name")?;
        let underlying = required_str(item, "underlying")?;
        if required_str(item, "type")? != "direct" {
            return Err(Error::MalformedData(format!(
                "Gateio USDT delivery contract {exchange_symbol} is not direct"
            )));
        }

        let (base, quote) = gateio_pair(underlying)?;
        if quote != "USDT" {
            return Err(Error::MalformedData(format!(
                "Gateio USDT delivery contract {exchange_symbol} has quote {quote}"
            )));
        }
        let prefix = exchange_symbol
            .strip_suffix(expiry_from_native(exchange_symbol)?)
            .and_then(|value| value.strip_suffix('_'));
        if prefix != Some(underlying) {
            return Err(Error::MalformedData(format!(
                "Gateio delivery contract {exchange_symbol} does not match underlying {underlying}"
            )));
        }
        let expiry = expiry_from_native(exchange_symbol)?;
        let expire_time = required_i64(item, "expire_time")?;
        let timestamp_expiry = expiry_from_seconds(expire_time)?;
        if !expiry.ends_with(&timestamp_expiry) {
            return Err(Error::MalformedData(format!(
                "Gateio delivery contract {exchange_symbol} disagrees with expire_time {expire_time}"
            )));
        }
        registry.insert_market(
            Symbol::futures(base, quote, expiry),
            exchange_symbol,
            ExchangeId::Gateio,
            item,
            Some("USDT"),
        )?;
    }
    Ok(())
}

pub(super) fn gateio_pair(value: &str) -> Result<(&str, &str)> {
    let mut parts = value.split('_');
    let base = parts.next().filter(|part| !part.is_empty());
    let quote = parts.next().filter(|part| !part.is_empty());
    if let (Some(base), Some(quote), None) = (base, quote, parts.next()) {
        return Ok((base, quote));
    }
    Err(Error::MalformedData(format!(
        "invalid Gateio contract pair {value}"
    )))
}

pub(super) fn required_i64(value: &Value, field: &str) -> Result<i64> {
    let field_value = value
        .get(field)
        .ok_or_else(|| Error::MalformedData(format!("instrument is missing {field}")))?;
    field_value
        .as_i64()
        .or_else(|| field_value.as_str()?.parse().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::MalformedData(format!("instrument has invalid {field}")))
}

pub(super) fn expiry_from_seconds(seconds: i64) -> Result<String> {
    let millis = seconds
        .checked_mul(1_000)
        .ok_or_else(|| Error::MalformedData(format!("invalid delivery timestamp {seconds}")))?;
    expiry_from_millis(&millis.to_string())
}

/// Fetches and registers this venue's instruments for `product`.
pub(super) async fn fetch_registry(
    registry: &mut SymbolRegistry,
    product: InstrumentKind,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let exchange = ExchangeId::Gateio;
    match product {
        InstrumentKind::Spot => {
            let payload = fetch_json(
                "https://api.gateio.ws/api/v4/spot/currency_pairs",
                refresh,
                transport,
                exchange,
            )
            .await?;
            add_spot_catalog(registry, &payload, ExchangeId::Gateio)?;
        }
        InstrumentKind::Perpetual => {
            for settle in ["usdt", "btc"] {
                let url = format!("https://api.gateio.ws/api/v4/futures/{settle}/contracts");
                let payload = fetch_json(&url, refresh, transport, exchange).await?;
                add_gateio_perpetual_markets(registry, &payload, settle)?;
            }
        }
        InstrumentKind::Futures => {
            let payload = fetch_json(
                "https://api.gateio.ws/api/v4/delivery/usdt/contracts",
                refresh,
                transport,
                exchange,
            )
            .await?;
            add_gateio_delivery_markets(registry, &payload)?;
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
