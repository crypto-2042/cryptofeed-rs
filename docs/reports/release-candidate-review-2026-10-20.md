# First RC review — 2026-10-20

Status: **evaluation candidate; registry publication NOT RUN**. This review does
not approve stable qualification or close any acceptance issue.

## GitHub audit

- [PR #7](https://github.com/crypto-2042/cryptofeed-rs/pull/7) prepares
  `0.1.0-rc.1` for all eleven crates, with exact internal prerelease requirements.
  Original head `77c34b11897dfa6fddc8f84d1ce0ee316a625e65` passed all five Linux
  jobs in both the [branch run](https://github.com/crypto-2042/cryptofeed-rs/actions/runs/39353164111)
  and [PR run](https://github.com/crypto-2042/cryptofeed-rs/actions/runs/39353474879).
  These runs cover that original head, not subsequent changes.
- [PR #5](https://github.com/crypto-2042/cryptofeed-rs/pull/5), the base64 0.23
  update, is open with merge conflicts. Its earlier CI passed, but this is not
  validation against the RC. It remains separate from release preparation.
- [Issue #3](https://github.com/crypto-2042/cryptofeed-rs/issues/3) retains failed
  subscriber-loss evidence; the observer contention repair does not prove unique
  attribution or complete requalification.
- [Issue #4](https://github.com/crypto-2042/cryptofeed-rs/issues/4) retains the
  Gateio readiness failure. Fixes and later prerequisite PASS observations do
  not replace a full candidate sustained run.
- [Issue #6](https://github.com/crypto-2042/cryptofeed-rs/issues/6) retains the
  Bitget perpetual termination investigation. Static diagnostics exist, but the
  original trigger remains unknown. All three issues remain open.
- GitHub Releases returned an empty list. All eleven read-only crates.io
  `/api/v1/crates/<name>` lookups returned HTTP 404 on this date. Availability
  reserves no names and establishes no upload/owner permission.

## Repeatable engineering gate

The package CI job now runs `bash scripts/verify-packages.sh` after the real
workspace publish dry-run. It fails on any command error, extracts the eleven
archives outside the checkout, tests their workspace, then removes that
workspace root before running separate default/minimal/all-feature consumers.
Every SDK path points at extracted packages. The consumers exercise the public
entrypoint/control/builder, handler traits, exact Decimal/Serde values and an
optional recording writer/reader roundtrip without connecting to exchanges.

The Rust 1.85 CI job now checks `--all-targets`. The release procedure uses the
exact current candidate version instead of the old stable-version example.
No runtime, feature default, retention, protocol or recovery behavior changes.

## Local validation

Checked implementation commit: `6ee917b6b31a0fe48bc6bb972c211bbb2f085f1c`, clean
when packaged. Report/index/status edits follow this commit; archive hashes below
identify that implementation commit only. Repackage the final release commit.

Environment: macOS 15.8 arm64; rustc/cargo 1.93.0; installed rustc 1.85.0.
Fresh dependency resolution in a separate checkout; no toolchain installation.

| Gate | Result | Scope |
| --- | --- | --- |
| Workspace publish dry-run, all features | PASS | Eleven archives build-verified; no `--allow-dirty`, no upload |
| Archive inspection | PASS | Eleven matching RC versions, exact internal requirements, licenses/notices/README/source present; no checkout dependency paths or build/private directories; clean VCS SHA |
| Rust 1.85 all-feature/all-target check | PASS | Fresh checkout resolution; no tracked lockfile |
| Extracted tests and three independent consumers | PASS | 853 tests, zero failed/ignored; default, minimal and all-feature consumers completed all assertions |
| Shell syntax, Rust formatting and diff whitespace | PASS | Release script, consumer and workspace |
| Linux sustained-run qualification | NOT RUN | Historical FAIL/NOT RUN evidence retained |
| Registry upload/download and docs.rs verification | NOT RUN | Requires authorized publication and subsequent real registry verification |

Commands: `cargo publish --workspace --dry-run --all-features`,
`cargo +1.85.0 check --workspace --all-features --all-targets`, and
`bash scripts/verify-packages.sh`. Local extracted builds use two jobs and
line-table debug info through Cargo environment overrides; assertions and
packaged source are unchanged. Initial sandbox DNS denial was followed by the
successful network-enabled dry-run. No exchange traffic was generated.

## Remaining release actions

1. Review and accept the final PR and its new CI runs as an evaluation RC only.
2. Confirm authorized publisher and source-level dependency notice review;
   regenerate final clean-commit archives before any upload. Release core, nine
   data crates, then runtime at matching versions. Verify actual registry-only
   consumers and docs.rs before announcing availability.
3. Resolve the Bitget investigation and execute the frozen Linux qualification
   stages with retained safe diagnostics, loss accounting and recovery evidence
   before stable promotion. Do not alter historical failures or relax thresholds.

## Archive SHA-256 at the checked implementation commit

| Archive | SHA-256 |
| --- | --- |
| `cryptofeed-candles-0.1.0-rc.1.crate` | `d7d4ff6edc3236a073c59a14eb6038bb252219d997d177d316146335a5975bc2` |
| `cryptofeed-core-0.1.0-rc.1.crate` | `c838968f5d1344df25297a2c69a85195d94b469521fd4fe82b19cbc81fbdb8d8` |
| `cryptofeed-funding-0.1.0-rc.1.crate` | `01d9ed75c38437964b06dbf6d29b61b01edf3e33d569ddb2e26b77cf77cbf610` |
| `cryptofeed-index-0.1.0-rc.1.crate` | `dba3366543b7b44f06e8e30fc3a22ddb0d98f12de843d76c4a96b7c4a1422280` |
| `cryptofeed-liquidations-0.1.0-rc.1.crate` | `623430976ddee3315fb30144b4730be293247285c4f62bc57b2618f170baedaf` |
| `cryptofeed-markprice-0.1.0-rc.1.crate` | `8b6ebc064fc3b87f39d639c2d3070428302f1175e4b67e5d974dcd498c72e4c2` |
| `cryptofeed-openinterest-0.1.0-rc.1.crate` | `2845b0f75bdb9d49ca2d695ca91ac08fb585897f1817d59faedd3e87a761e61f` |
| `cryptofeed-orderbook-0.1.0-rc.1.crate` | `0a348a8b20694944fcf48e08f8e6d6c4abde6a0ce1ec6299f3308e89eb4c33e9` |
| `cryptofeed-rs-0.1.0-rc.1.crate` | `06cf47c0b6d8568d80fa99cb28ad13b95f067db30b7c6a69200a98893e4ddc29` |
| `cryptofeed-ticker-0.1.0-rc.1.crate` | `0350203d34089fa4ca15922e8d3bfe53baa766413765e2c046824d589de7f035` |
| `cryptofeed-trade-0.1.0-rc.1.crate` | `d16ffb0bd30272688b2a5ca82e21932934b2ce0e5e262b5b7eae835d407d126d` |
