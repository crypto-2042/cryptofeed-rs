//! Safe, bounded error classification. Never includes exchange or transport text.
use cryptofeed_core::error::Error;

/// Structured evidence for an SDK error, independent of its arbitrary message.
/// `permanent` describes retry policy, not whether retries have been exhausted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct FailureDiagnostic {
    pub category: &'static str,
    pub reason: &'static str,
    pub permanent: bool,
    pub http_status: Option<u16>,
    pub close_code: Option<u16>,
}

impl From<&Error> for FailureDiagnostic {
    fn from(error: &Error) -> Self {
        let category = match error {
            Error::UnsupportedExchange(_) => "unsupported_exchange",
            Error::UnsupportedChannel(_) => "unsupported_channel",
            Error::UnsupportedSymbol(_) => "unsupported_symbol",
            Error::AmbiguousSymbol(_) => "ambiguous_symbol",
            Error::UnsupportedCapability(_) => "unsupported_capability",
            Error::InvalidConfiguration(_) => "invalid_configuration",
            Error::Protocol(_) => "protocol",
            Error::Subscription(_) => "subscription",
            Error::RateLimited { .. } => "rate_limited",
            Error::WebSocketClosed { .. } => "websocket_closed",
            Error::MalformedData(_) => "malformed_data",
            Error::HttpStatus { .. } => "http_status",
            Error::Transport(_) => "transport",
            Error::Parse(_) => "parse",
            _ => "unknown",
        };
        let reason = match error {
            Error::Parse(text) if text == "bitget book sequence gap detected" => {
                "book_sequence_gap"
            }
            Error::Parse(text) if text == "bitget book sync not initialized" => {
                "book_not_initialized"
            }
            Error::Parse(text) if text == "missing bitget depth payload" => "book_payload_missing",
            Error::Parse(text) if text == "failed to parse bitget depth update" => {
                "book_payload_invalid"
            }
            Error::MalformedData(text) if text.starts_with("crossed L2 book: ") => "crossed_book",
            Error::MalformedData(text)
                if text
                    == "Bitget liquidation message matched the subscribed topic but produced no events" =>
            {
                "liquidation_payload_invalid"
            }
            Error::Transport(text) if text == "websocket idle timeout" => "receipt_timeout",
            Error::Transport(text) if text == "websocket send timed out" => "send_timeout",
            Error::Transport(text) if text == "websocket pong timed out" => "pong_timeout",
            Error::Transport(text) if text == "subscription acknowledgement timed out" => {
                "subscription_ack_timeout"
            }
            Error::Transport(text) if text.starts_with("websocket connect timed out: ") => {
                "handshake_timeout"
            }
            _ => category,
        };
        Self {
            category,
            reason,
            permanent: crate::runtime::supervisor::is_permanent(error),
            http_status: match error {
                Error::HttpStatus { status, .. } => Some(*status),
                _ => None,
            },
            close_code: match error {
                Error::WebSocketClosed { code, .. } => *code,
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_never_copies_arbitrary_error_text_and_keeps_retry_policy() {
        for error in [
            Error::Subscription("SECRET https://user:password@proxy".into()),
            Error::Transport("SECRET".into()),
            Error::Parse("SECRET".into()),
            Error::MalformedData("crossed L2 book: SECRET".into()),
        ] {
            let d = FailureDiagnostic::from(&error);
            assert!(!format!("{d:?}").contains("SECRET"));
            assert_eq!(
                d.permanent,
                crate::runtime::supervisor::is_permanent(&error)
            );
        }
        let d = FailureDiagnostic::from(&Error::HttpStatus {
            status: 403,
            retry_after: None,
        });
        assert_eq!(d.http_status, Some(403));
        assert!(d.permanent);
        let d = FailureDiagnostic::from(&Error::WebSocketClosed {
            code: Some(1008),
            retryable: false,
        });
        assert_eq!(d.close_code, Some(1008));
        assert!(d.permanent);
        let d = FailureDiagnostic::from(&Error::Parse("bitget book sequence gap detected".into()));
        assert_eq!(d.reason, "book_sequence_gap");
        assert!(!d.permanent);
    }
}
