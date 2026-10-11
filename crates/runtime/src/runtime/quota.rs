//! Verified public REST/WS codes, separate from trading limits.
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
/// Public WS error envelopes only; the delay is conservative SDK policy.
pub(crate) fn websocket_error(
    exchange: ExchangeId,
    value: &Value,
) -> Option<cryptofeed_core::error::Error> {
    if exchange == ExchangeId::Gateio
        && value["event"] == "subscribe"
        && value.pointer("/error/code").and_then(Value::as_i64) == Some(3)
    {
        return Some(cryptofeed_core::error::Error::Transport(
            "Gate WS service error 3".into(),
        ));
    }
    if value.get("event").and_then(Value::as_str) != Some("error") {
        return None;
    }
    let code = value.get("code").and_then(Value::as_str)?;
    let limited = match exchange {
        ExchangeId::Okx => matches!(code, "60014" | "50011"),
        ExchangeId::Bitget => matches!(code, "429" | "30006" | "30007"),
        _ => false,
    };
    if limited {
        return Some(cryptofeed_core::error::Error::RateLimited {
            code: code.into(),
            retry_after: Duration::from_secs(60),
        });
    }
    let transient = match exchange {
        ExchangeId::Okx => matches!(code, "64007" | "64008"),
        ExchangeId::Bitget => code == "30033",
        _ => false,
    };
    transient.then(|| {
        cryptofeed_core::error::Error::Transport(format!("{exchange:?} WS service error {code}"))
    })
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
    #[tokio::test]
    async fn ws_control_processing_without_readiness_preserves_native_error_class() {
        use crate::exchange::ExchangeFeedBuilder;
        use cryptofeed_core::error::Error;
        let bitget = ExchangeFeedBuilder::new(ExchangeId::Bitget).build();
        let okx = ExchangeFeedBuilder::new(ExchangeId::Okx).build();
        let gate = ExchangeFeedBuilder::new(ExchangeId::Gateio).build();
        assert!(matches!(
            super::super::process_bitget_text_message(
                &bitget,
                r#"{"event":"error","code":"30006"}"#,
                1.0
            )
            .await,
            Err(Error::RateLimited { .. })
        ));
        assert!(matches!(
            super::super::process_okx_text_message(
                &okx,
                r#"{"event":"error","code":"60014"}"#,
                1.0
            )
            .await,
            Err(Error::RateLimited { .. })
        ));
        assert!(matches!(
            super::super::process_gateio_text_message(
                &gate,
                r#"{"event":"subscribe","error":{"code":3}}"#,
                1.0
            )
            .await,
            Err(Error::Transport(_))
        ));
    }
    #[test]
    fn ws_quota_codes_are_typed_transient_public_errors() {
        use cryptofeed_core::error::Error;
        for (exchange, codes) in [
            (ExchangeId::Okx, vec!["60014", "50011"]),
            (ExchangeId::Bitget, vec!["429", "30006", "30007"]),
        ] {
            for code in codes {
                let error =
                    websocket_error(exchange, &serde_json::json!({"event":"error","code":code}))
                        .unwrap();
                assert!(
                    matches!(&error, Error::RateLimited { code: found, retry_after } if found == code && *retry_after == Duration::from_secs(60))
                );
                assert!(!crate::runtime::supervisor::is_permanent(&error));
            }
        }
    }
    #[test]
    fn ws_service_errors_reconnect_without_becoming_quota_or_parameter_errors() {
        use cryptofeed_core::error::Error;
        for (exchange, value) in [
            (
                ExchangeId::Okx,
                serde_json::json!({"event":"error","code":"64007"}),
            ),
            (
                ExchangeId::Okx,
                serde_json::json!({"event":"error","code":"64008"}),
            ),
            (
                ExchangeId::Bitget,
                serde_json::json!({"event":"error","code":"30033"}),
            ),
            (
                ExchangeId::Gateio,
                serde_json::json!({"event":"subscribe","error":{"code":3}}),
            ),
        ] {
            assert!(matches!(
                websocket_error(exchange, &value),
                Some(Error::Transport(_))
            ));
        }
    }
    #[test]
    fn ws_codes_do_not_coerce_types_or_promote_rest_private_and_unknown_envelopes() {
        for (exchange, value) in [
            (
                ExchangeId::Okx,
                serde_json::json!({"event":"error","code":60014}),
            ),
            (
                ExchangeId::Okx,
                serde_json::json!({"event":"subscribe","code":"60014"}),
            ),
            (ExchangeId::Okx, serde_json::json!({"code":"50011"})),
            (
                ExchangeId::Okx,
                serde_json::json!({"event":"error","code":"60023"}),
            ),
            (
                ExchangeId::Bitget,
                serde_json::json!({"event":"error","code":"30002","msg":"429 too frequent"}),
            ),
            (
                ExchangeId::Bybit,
                serde_json::json!({"op":"subscribe","retCode":10429}),
            ),
            (ExchangeId::Binance, serde_json::json!({"code":-1003})),
            (
                ExchangeId::Gateio,
                serde_json::json!({"event":"subscribe","error":{"code":"3"}}),
            ),
            (
                ExchangeId::Gateio,
                serde_json::json!({"event":"subscribe","error":{"code":2}}),
            ),
        ] {
            assert!(websocket_error(exchange, &value).is_none());
        }
    }
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
