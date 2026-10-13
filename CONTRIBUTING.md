# Contributing

Run the release gates from the workspace root before opening a change:

```bash
cargo fmt --all --check
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo check --workspace --no-default-features
make features
make feature-test
cmp AGENTS.md CLAUDE.md
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

The workspace declares Rust 1.85 and uses Cargo resolver 3 to select compatible
dependencies without a committed Cargo.lock. Also verify
`cargo +1.85.0 check --workspace --all-features` and `cargo audit` against a
freshly generated local lockfile; do not suppress dependency advisories.
Package-content checks verify README/LICENSE files but do not constitute
crates.io publication or registry dependency verification.

Protocol changes must use the current official exchange documentation and
update the corresponding inline parity assertion, sanitized fixture, and
`docs/exchange-protocol-baseline.md` together. Live validation must be recorded
in a dated `docs/reports/live-smoke-YYYY-MM-DD.md` report without credentials.
