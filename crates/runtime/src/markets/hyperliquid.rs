//! Hyperliquid catalog fetch and parse support.
//!
//! Owns the `hyperliquid` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

/// Documented public info endpoint. Both catalogs are POST requests to the
/// same URL with different JSON bodies.
pub(super) const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

pub(super) async fn fetch_hyperliquid_perpetual_catalog(
    registry: &mut SymbolRegistry,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let payload = fetch_hyperliquid_info("meta", refresh, transport).await?;
    add_hyperliquid_perpetual_catalog(registry, &payload)
}

pub(super) async fn fetch_hyperliquid_spot_catalog(
    registry: &mut SymbolRegistry,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let payload = fetch_hyperliquid_info("spotMeta", refresh, transport).await?;
    add_hyperliquid_spot_catalog(registry, &payload)
}

/// `POST /info` with `{"type": <kind>}`. The synthetic `?type=` query is only
/// the cache/recording/replay key that distinguishes the two POST bodies; the
/// request itself goes to the documented base URL.
pub(super) async fn fetch_hyperliquid_info(
    kind: &str,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<Value> {
    let key_url = format!("{HYPERLIQUID_INFO_URL}?type={kind}");
    let key = transport.cache_key(&key_url);
    fetch_catalog_with(&key, refresh, || async {
        #[cfg(feature = "recording")]
        transport.check_replay_catalog_fetch(&key_url, ExchangeId::Hyperliquid, refresh)?;
        let body = serde_json::json!({ "type": kind });
        let result = fetch_catalog_json_post(
            HYPERLIQUID_INFO_URL,
            &key_url,
            &body,
            transport,
            ExchangeId::Hyperliquid,
        )
        .await;
        #[cfg(feature = "recording")]
        if result.is_ok() {
            transport.replay_catalog_loaded(&key_url, ExchangeId::Hyperliquid);
        }
        result
    })
    .await
}

/// `isDelisted` is the documented perpetual/spot removal flag; absent means
/// listed. A non-boolean value fails the catalog closed.
pub(super) fn hyperliquid_delisted(item: &Value) -> Result<bool> {
    match item.get("isDelisted") {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(Error::MalformedData(
            "Hyperliquid isDelisted must be a boolean".to_owned(),
        )),
    }
}

/// `{"type":"meta"}`: `universe[]` perpetual rows. The WS `coin` is
/// `universe[].name`; perpetuals are quoted in USD. Delisted rows are skipped.
pub(super) fn add_hyperliquid_perpetual_catalog(
    registry: &mut SymbolRegistry,
    payload: &Value,
) -> Result<()> {
    for item in array_at(payload, &["universe"])? {
        if hyperliquid_delisted(item)? {
            continue;
        }
        let coin = required_str(item, "name")?;
        if coin.contains(['-', '/']) {
            return Err(Error::MalformedData(format!(
                "Hyperliquid perpetual name {coin} is not a plain base symbol"
            )));
        }
        registry.insert_market(
            Symbol::perpetual(coin, "USD"),
            coin,
            ExchangeId::Hyperliquid,
            item,
            None,
        )?;
    }
    Ok(())
}

/// `{"type":"spotMeta"}`: `tokens[]` carries token names and `universe[]`
/// carries `tokens:[base,quote]` plus the WS `coin` (`PURR/USDC` for the
/// canonical pair or `@{index}` otherwise). The normalized symbol is built from
/// the token names, never from the UI name; arbitrary quote tokens (`USDT0`,
/// `USDH`, `USDE`) are preserved exactly. Removed pairs are absent from
/// `universe`; `isDelisted` is honored when present.
pub(super) fn add_hyperliquid_spot_catalog(
    registry: &mut SymbolRegistry,
    payload: &Value,
) -> Result<()> {
    let mut token_names = HashMap::new();
    for token in array_at(payload, &["tokens"])? {
        let index = token
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::MalformedData("Hyperliquid token is missing index".to_owned()))?;
        let name = required_str(token, "name")?;
        if token_names.insert(index, name.to_owned()).is_some() {
            return Err(Error::MalformedData(format!(
                "Hyperliquid token index {index} is duplicated"
            )));
        }
    }
    for item in array_at(payload, &["universe"])? {
        if hyperliquid_delisted(item)? {
            continue;
        }
        let coin = required_str(item, "name")?;
        let pair = item
            .get("tokens")
            .and_then(Value::as_array)
            .filter(|pair| pair.len() == 2)
            .ok_or_else(|| {
                Error::MalformedData(format!(
                    "Hyperliquid spot pair {coin} has no [base, quote] token indices"
                ))
            })?;
        let base_index = pair[0].as_u64().ok_or_else(|| {
            Error::MalformedData(format!(
                "Hyperliquid spot pair {coin} has an invalid base index"
            ))
        })?;
        let quote_index = pair[1].as_u64().ok_or_else(|| {
            Error::MalformedData(format!(
                "Hyperliquid spot pair {coin} has an invalid quote index"
            ))
        })?;
        let base = token_names.get(&base_index).ok_or_else(|| {
            Error::MalformedData(format!(
                "Hyperliquid spot pair {coin} references an unknown base"
            ))
        })?;
        let quote = token_names.get(&quote_index).ok_or_else(|| {
            Error::MalformedData(format!(
                "Hyperliquid spot pair {coin} references an unknown quote"
            ))
        })?;
        if base.contains(['-', '/']) || quote.contains(['-', '/']) {
            return Err(Error::MalformedData(format!(
                "Hyperliquid spot token names {base}/{quote} are not plain asset symbols"
            )));
        }
        match coin.strip_prefix('@') {
            Some(embedded) => {
                let index = item.get("index").and_then(Value::as_u64).ok_or_else(|| {
                    Error::MalformedData(format!(
                        "Hyperliquid spot pair {coin} is missing its pair index"
                    ))
                })?;
                if embedded.parse::<u64>().ok() != Some(index) {
                    return Err(Error::MalformedData(format!(
                        "Hyperliquid spot pair {coin} disagrees with index {index}"
                    )));
                }
            }
            None => {
                let expected = format!("{base}/{quote}");
                if coin != expected {
                    return Err(Error::MalformedData(format!(
                        "Hyperliquid spot pair {coin} disagrees with token pair {expected}"
                    )));
                }
            }
        }
        registry.insert_market(
            Symbol::spot(base, quote),
            coin,
            ExchangeId::Hyperliquid,
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
    let exchange = ExchangeId::Hyperliquid;
    match product {
        InstrumentKind::Spot => {
            fetch_hyperliquid_spot_catalog(registry, refresh, transport).await?;
        }
        InstrumentKind::Perpetual => {
            fetch_hyperliquid_perpetual_catalog(registry, refresh, transport).await?;
        }
        _ => {
            return Err(Error::UnsupportedCapability(format!(
                "{:?}/{:?} symbol discovery; Hyperliquid exposes public perpetuals and spot only",
                exchange, product
            )));
        }
    }
    Ok(())
}
