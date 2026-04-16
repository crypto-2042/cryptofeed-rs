pub fn normalize_product_id(raw: &str) -> String {
    raw.to_owned()
}

#[cfg(test)]
mod tests {
    use super::normalize_product_id;

    #[test]
    fn preserves_coinbase_product_format() {
        assert_eq!(normalize_product_id("BTC-USD"), "BTC-USD");
    }
}
