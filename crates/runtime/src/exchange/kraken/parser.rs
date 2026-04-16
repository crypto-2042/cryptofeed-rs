pub fn normalize_ws_symbol(raw: &str) -> String {
    raw.replace('/', "-")
}

#[cfg(test)]
mod tests {
    use super::normalize_ws_symbol;

    #[test]
    fn normalizes_kraken_slash_symbol() {
        assert_eq!(normalize_ws_symbol("BTC/USD"), "BTC-USD");
    }
}
