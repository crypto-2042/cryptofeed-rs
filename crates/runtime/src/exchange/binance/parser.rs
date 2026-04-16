pub fn parse_trade_symbol(raw: &str) -> String {
    raw.replace("USDT", "-USDT")
}

#[cfg(test)]
mod tests {
    use super::parse_trade_symbol;

    #[test]
    fn parses_binance_trade_symbol() {
        assert_eq!(parse_trade_symbol("BTCUSDT"), "BTC-USDT");
    }
}
