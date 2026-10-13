check:
	cargo check --workspace

test:
	cargo test --workspace

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-features --all-targets -- -D warnings

features:
	cargo check --workspace --no-default-features
	for feature in ticker trade orderbook candles funding index liquidations markprice openinterest recording; do cargo check -p cryptofeed-rs --lib --no-default-features --features "$$feature" || exit 1; done

feature-test:
	for feature in ticker trade orderbook candles funding index liquidations markprice openinterest recording; do cargo test -p cryptofeed-rs --test feature_boundaries --no-default-features --features "$$feature" || exit 1; done

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
