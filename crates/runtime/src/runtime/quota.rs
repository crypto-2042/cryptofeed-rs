//! Official public REST rate codes, separate from HTTP status and trading limits.
use cryptofeed_core::exchange::ExchangeId;
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
/// Only documented representations/scopes. Do not inspect diagnostics or guess
/// generic 'too busy', private order limits, or a different exchange's code.
pub(crate) fn native_code(exchange: ExchangeId, value: &Value) -> Option<&'static str> {
    match exchange {
        ExchangeId::Binance if value.get("code").and_then(Value::as_i64) == Some(-1003) => {
            Some("-1003")
        }
        ExchangeId::Bybit if value.get("retCode").and_then(Value::as_i64) == Some(10006) => {
            Some("10006")
        }
        ExchangeId::Okx if value.get("code").and_then(Value::as_str) == Some("50011") => {
            Some("50011")
        }
        ExchangeId::Bitget if value.get("code").and_then(Value::as_str) == Some("429") => {
            Some("429")
        }
        _ => None,
    }
}
pub(super) fn reset_delay(millis: u64, now: SystemTime) -> Option<Duration> {
    let target = UNIX_EPOCH.checked_add(Duration::from_millis(millis))?;
    target
        .duration_since(now)
        .ok()
        .filter(|delay| !delay.is_zero())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn documented_public_codes_match_without_dynamic_conversion_or_private_scope_guessing() {
        for (exchange, value, code) in [
            (
                ExchangeId::Binance,
                serde_json::json!({"code":-1003}),
                "-1003",
            ),
            (
                ExchangeId::Bybit,
                serde_json::json!({"retCode":10006}),
                "10006",
            ),
            (
                ExchangeId::Okx,
                serde_json::json!({"code":"50011"}),
                "50011",
            ),
            (ExchangeId::Bitget, serde_json::json!({"code":"429"}), "429"),
        ] {
            assert_eq!(native_code(exchange, &value), Some(code));
        }
        for (exchange, value) in [
            (ExchangeId::Binance, serde_json::json!({"code":-1015})),
            (ExchangeId::Bybit, serde_json::json!({"retCode":10429})),
            (ExchangeId::Bybit, serde_json::json!({"retCode":"10006"})),
            (ExchangeId::Okx, serde_json::json!({"code":"50061"})),
            (ExchangeId::Bitget, serde_json::json!({"code":"25004"})),
            (ExchangeId::Bitget, serde_json::json!({"code":429})),
            (
                ExchangeId::Gateio,
                serde_json::json!({"label":"TOO_MANY_REQUESTS"}),
            ),
            (
                ExchangeId::Binance,
                serde_json::json!({"msg":"rate limit 10006"}),
            ),
        ] {
            assert_eq!(native_code(exchange, &value), None);
        }
    }
    #[test]
    fn reset_timestamp_is_epoch_milliseconds_and_invalid_past_values_do_not_disable_fallback() {
        let now = UNIX_EPOCH + Duration::from_secs(10);
        assert_eq!(reset_delay(12500, now), Some(Duration::from_millis(2500)));
        for millis in [10000, 9999] {
            assert_eq!(reset_delay(millis, now), None);
        }
    }
}
