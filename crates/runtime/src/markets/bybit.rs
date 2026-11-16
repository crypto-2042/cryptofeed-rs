//! Bybit catalog fetch and parse support.
//!
//! Owns the `bybit` symbol-discovery endpoints and payload parsers. The
//! capability table, validation and shared catalog HTTP/cache client live in
//! `markets.rs`.

use super::*;

pub(super) async fn fetch_bybit_category(
    registry: &mut SymbolRegistry,
    category: &str,
    product: InstrumentKind,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    loop {
        let mut url = url::Url::parse("https://api.bybit.com/v5/market/instruments-info")
            .map_err(|error| Error::Transport(error.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("category", category);
            query.append_pair("status", "Trading");
            query.append_pair("limit", "1000");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let payload = fetch_json(url.as_str(), refresh, transport, ExchangeId::Bybit).await?;
        cursor = add_bybit_page(registry, &payload, product)?;
        let Some(next) = cursor.as_deref() else {
            return Ok(());
        };
        record_bybit_cursor(&mut seen, next)?;
    }
}

pub(super) async fn fetch_bybit_option_category(
    registry: &mut SymbolRegistry,
    refresh: bool,
    transport: &TransportConfig,
) -> Result<()> {
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    loop {
        let mut url = url::Url::parse("https://api.bybit.com/v5/market/instruments-info")
            .map_err(|error| Error::Transport(error.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("category", "option");
            query.append_pair("limit", "1000");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let payload = fetch_json(url.as_str(), refresh, transport, ExchangeId::Bybit).await?;
        cursor = add_bybit_option_page(registry, &payload)?;
        let Some(next) = cursor.as_deref() else {
            return Ok(());
        };
        record_bybit_cursor(&mut seen, next)?;
    }
}

pub(super) fn add_bybit_option_page(
    registry: &mut SymbolRegistry,
    payload: &Value,
) -> Result<Option<String>> {
    for item in array_at(payload, &["result", "list"])? {
        let exchange_symbol = required_str(item, "symbol")?;
        // The instruments-info contract carries `optionsType`
        // (`Call`/`Put`); cross-check it against the documented
        // `{C|P}` suffix so a catalog contradiction is explicit.
        if let Some(options_type) = str_at(item, &["optionsType"]) {
            let suffix = exchange_symbol
                .strip_suffix("-USDT")
                .or_else(|| exchange_symbol.strip_suffix("-USDC"))
                .and_then(|stripped| stripped.rsplit('-').next())
                .ok_or_else(|| {
                    Error::MalformedData(format!("invalid Bybit option symbol {exchange_symbol}"))
                })?;
            let expected = if matches!(suffix, "C" | "P") {
                suffix
            } else {
                ""
            };
            let expected = match expected {
                "C" => "Call",
                "P" => "Put",
                _ => {
                    return Err(Error::MalformedData(format!(
                        "invalid Bybit option type in {exchange_symbol}"
                    )));
                }
            };
            if options_type != expected {
                return Err(Error::MalformedData(format!(
                    "Bybit option {exchange_symbol} disagrees with optionsType {options_type}"
                )));
            }
        }
        add_bybit_option_market(registry, exchange_symbol)?;
    }

    let cursor = str_at(payload, &["result", "nextPageCursor"])
        .ok_or_else(|| {
            Error::MalformedData("Bybit options page is missing nextPageCursor".to_owned())
        })?
        .trim();
    Ok((!cursor.is_empty()).then(|| cursor.to_owned()))
}

pub(super) fn add_bybit_option_market(
    registry: &mut SymbolRegistry,
    exchange_symbol: &str,
) -> Result<()> {
    // Native form: {BASE}-{EXPIRY}-{STRIKE}-{C|P}, with a documented -USDT
    // suffix for USDT-settled options; otherwise USDC settlement.
    let settle = if exchange_symbol.ends_with("-USDT") {
        "USDT"
    } else {
        "USDC"
    };
    let stripped = exchange_symbol
        .strip_suffix("-USDT")
        .unwrap_or(exchange_symbol);
    let parts: Vec<_> = stripped.split('-').collect();
    let [base, expiry, strike, option_type] = parts.as_slice() else {
        return Err(Error::MalformedData(format!(
            "invalid Bybit option symbol {exchange_symbol}"
        )));
    };
    if !matches!(*option_type, "C" | "P") {
        return Err(Error::MalformedData(format!(
            "invalid Bybit option type in {exchange_symbol}"
        )));
    }
    registry.insert(
        Symbol::option(base, settle, expiry, strike, option_type),
        exchange_symbol,
    )
}

pub(super) fn add_bybit_page(
    registry: &mut SymbolRegistry,
    payload: &Value,
    product: InstrumentKind,
) -> Result<Option<String>> {
    for item in array_at(payload, &["result", "list"])? {
        if !catalog_status(item, "status", &["Trading"])? {
            continue;
        }
        let contract_type = required_str(item, "contractType")?;
        let item_product = if contract_type.ends_with("Perpetual") {
            InstrumentKind::Perpetual
        } else if contract_type.ends_with("Futures") {
            InstrumentKind::Futures
        } else {
            return Err(Error::MalformedData(format!(
                "unknown Bybit contract type {contract_type}"
            )));
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
        registry.insert_market(symbol, exchange_symbol, ExchangeId::Bybit, item, None)?;
    }

    let cursor = str_at(payload, &["result", "nextPageCursor"])
        .ok_or_else(|| {
            Error::MalformedData("Bybit instruments page is missing nextPageCursor".to_owned())
        })?
        .trim();
    Ok((!cursor.is_empty()).then(|| cursor.to_owned()))
}

pub(super) fn record_bybit_cursor(seen: &mut HashSet<String>, cursor: &str) -> Result<()> {
    if !seen.insert(cursor.to_owned()) {
        return Err(Error::Protocol(format!(
            "Bybit repeated instruments cursor {cursor}"
        )));
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
    let exchange = ExchangeId::Bybit;
    if product == InstrumentKind::Spot {
        let payload = fetch_json(
            "https://api.bybit.com/v5/market/instruments-info?category=spot&status=Trading",
            refresh,
            transport,
            exchange,
        )
        .await?;
        add_spot_catalog(registry, &payload, ExchangeId::Bybit)?;
    } else if product == InstrumentKind::Option {
        // Verified live 2026-08-07: `tickers?category=option`
        // requires a `baseCoin` parameter (PARAMS_ERROR otherwise);
        // the full option catalog pages from
        // `instruments-info?category=option` without a base coin.
        fetch_bybit_option_category(registry, refresh, transport).await?;
    } else {
        for category in ["linear", "inverse"] {
            fetch_bybit_category(registry, category, product, refresh, transport).await?;
        }
    }
    Ok(())
}
