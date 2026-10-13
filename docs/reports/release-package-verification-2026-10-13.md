# Release-package verification — 2026-10-13

## Candidate and environment

- Source baseline: `ddd135b` plus the MIT/notice/documentation/CI changes delivered
  with this report. No Rust implementation changes in this candidate.
- Host: macOS arm64; rustc 1.93.0 (254b59607, 2026-01-19),
  cargo 1.93.0 (083ac5135, 2025-12-15). No toolchain upgrade performed.
- Fresh MSRV check: already-installed Rust 1.85.0, resolver 3, edition 2024.
- All workspace crates: version 0.1.0, MIT. Original Python reference notice is
  preserved at revision `3a6d3cadf63f3f54887c7abfc87d64da276c4e7e`.
- Public registry publication and sustained live qualification: **NOT RUN**.

## Results

| Check | Result | Evidence / scope |
| --- | --- | --- |
| Workspace publish dry-run, all features | PASS | All 11 archives packaged and build-verified; Cargo explicitly aborted each upload due to dry-run. |
| Archive inspection | PASS | All 11 contain matching MIT license, exact third-party notice, README, source and normalized manifest; MSRV 1.85 retained, runtime rustls >=0.23.45. No checkout-relative dependency paths or .local/.git/target files. |
| Independent consumer, defaults | PASS | 2 offline assertions: entrypoint/control, builder/handler and exact Decimal/Serde trade model. |
| Independent consumer, no default features | PASS | 1 offline entrypoint/control assertion, no data feature required. |
| Independent consumer, all features | PASS | 3 offline assertions including optional recording writer/reader empty-file roundtrip. |
| Extracted package tests | PASS | 837 tests across all 11 extracted crates, including runtime unit/integration/parity tests; no source-checkout paths used. |
| Extracted runtime strict Clippy | PASS | All features and all targets, `-D warnings`. |
| Extracted runtime strict rustdoc | PASS | All features, no dependency docs, `RUSTDOCFLAGS=-D warnings`. |
| Fresh MSRV compilation | PASS | `cargo +1.85.0 generate-lockfile` followed by workspace/all-features/all-targets check. |
| Format / diff hygiene | PASS | `cargo fmt --all --check`, `git diff --check`. |
| Fresh RustSec audit | PASS | 203 lockfile packages, 0 known vulnerabilities, 0 informational warnings, no ignored advisories. |
| Dependency license metadata | PASS (metadata review) | All resolved registry packages declare licenses; SPDX expressions below. This is not a source-level legal audit. |
| Registry name/version lookup | PASS (read-only observation) | All 11 `/api/v1/crates/<name>` endpoints returned 404; no existing package observed. This does not reserve names or establish publishing permission. |
| Linux final-commit package check | CI gate | CI now runs the real workspace publish dry-run, not only file listing. Consult that commit's CI result. |
| 24–72 hour sustained-run stages | NOT RUN | Procedure and acceptance thresholds in [release-validation](../release-validation.md). |
| Actual registry upload/owner/download/docs verification | NOT RUN | Dry-run does not authenticate the publisher or perform server-side upload checks. |

## Method and commands

```sh
cargo publish --workspace --dry-run --all-features --allow-dirty
cargo +1.85.0 generate-lockfile
cargo +1.85.0 check --workspace --all-features --all-targets
cargo fmt --all --check
cargo audit --json
```

The local candidate used `--allow-dirty` to include reviewable license/README
changes. Workspace Cargo staging produced archives under
`target/package/tmp-registry/`; a stale top-level core archive was deliberately
excluded. Each archive was inspected and extracted into a new temporary directory
outside the checkout. Every sibling dependency was patched exclusively to those
extracted directories. The consumer declared exact registry version `=0.1.0`,
with separate default/empty/recording-enabled feature runs. Consumer builds/tests
were offline; its patch configuration did not reference `crates/` in the checkout.

For each extracted package, ran `cargo test --offline --all-features --manifest-path
<extracted-package>/Cargo.toml`. Ran runtime Clippy and rustdoc against the extracted
manifest as well. Initial sandbox execution denied loopback binding in two
WebSocket rejection tests; after permitting local sockets, the full extracted
runtime suite passed. No tests or sequence checks were weakened.
Staging patches unused by smaller crates produced Cargo informational warnings;
they do not represent missing dependencies or ignored compiler diagnostics.

A second local dry-run without `--allow-dirty` passed on clean commit
`59fc313e8138f883238e7c328c3954811c9166af`; all 11 staged archives
reported that commit and clean VCS metadata. Subsequent documentation-only
clarifications do not change Rust source, manifests, licenses or packaged README.
The release CI uses the clean checkout command without `--allow-dirty`.
Independent extracted-package tests validate archive content; staging patches do
not test registry download or publishing authorization. No exchange traffic or
long-running collection was generated by these package tests.

## Resolved security-sensitive versions

- reqwest 0.13.5; tokio-tungstenite/tungstenite 0.30.0.
- rustls 0.23.45 (minimum requirement also 0.23.45); tokio 1.53.2.
- serde_json 1.0.154 with arbitrary_precision and float_roundtrip;
  rust_decimal 1.43.0.
- RustSec database commit `dabd3e7a1d8694b3d3294bab6a514ba2288abf78`,
  updated `2026-10-13T12:23:38+02:00`,
  1308 advisories checked. An audit only detects known published issues.

## Registry dependency license expressions

Counts include dependencies for all targets returned by Cargo metadata, not just
the host's built graph. For alternative licenses, select a permissive option;
retain the applicable notices and any combined Unicode/ISC obligations. Project
MIT licensing does not replace dependency licenses.

| Declared expression | Registry packages |
| --- | --- |
| `(MIT OR Apache-2.0) AND Unicode-3.0` | 1 |
| `Apache-2.0` | 2 |
| `Apache-2.0 AND ISC` | 1 |
| `Apache-2.0 OR ISC OR MIT` | 3 |
| `Apache-2.0 OR MIT` | 10 |
| `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` | 3 |
| `BSD-2-Clause OR Apache-2.0 OR MIT` | 2 |
| `BSD-3-Clause` | 1 |
| `CDLA-Permissive-2.0` | 3 |
| `ISC` | 2 |
| `MIT` | 27 |
| `MIT OR Apache-2.0` | 113 |
| `MIT OR Apache-2.0 OR LGPL-2.1-or-later` | 2 |
| `Unicode-3.0` | 18 |
| `Unlicense OR MIT` | 2 |
| `Unlicense/MIT` | 2 |

## Review-candidate archive SHA-256

These identify the inspected local review archives (`dirty: true` VCS metadata),
not future uploads. A clean-commit repack changes VCS metadata and may change hashes;
retain final uploaded hashes when actual publication is authorized.

| Archive | SHA-256 |
| --- | --- |
| `cryptofeed-candles-0.1.0.crate` | `c87473332b4d70be9beb4820940bf363165f862aff32d633247b7dea335162d8` |
| `cryptofeed-core-0.1.0.crate` | `87307152d9cbec9d6398b32e49975fac7aa5d4cc74062b3a83f605373c5c7537` |
| `cryptofeed-funding-0.1.0.crate` | `fed8167a0d258d614ff80af39dc0d5221118f8248967eec71e2424925c40b973` |
| `cryptofeed-index-0.1.0.crate` | `5a3ed6e33de80df8f8f179868fecfe32aa558e8c45f7a14bca59f5725a47fd7d` |
| `cryptofeed-liquidations-0.1.0.crate` | `9fee1900b99f7a0877342f1a5e982609928101a1c523bec57545018783a5fa88` |
| `cryptofeed-markprice-0.1.0.crate` | `9e952386da484524244b2c0b285c8b0fd06ac8b08a2f4df7b9c3f65e254b469a` |
| `cryptofeed-openinterest-0.1.0.crate` | `fd39a486bddfc0288d756cbfae4623d38032150e3df223fdce3889d96394b7fe` |
| `cryptofeed-orderbook-0.1.0.crate` | `b88cec82e613441863258845b05e01e49bc1eb044e6d244bb823a05dc8314de1` |
| `cryptofeed-rs-0.1.0.crate` | `486fec15a5f54434dbf3c720201b96d848867869c939019b85cd3a30ee334343` |
| `cryptofeed-ticker-0.1.0.crate` | `54dea0433c85c975445e0e6a4de8370c4777bce2a5f0e1ec2a840bbabff32603` |
| `cryptofeed-trade-0.1.0.crate` | `6a8033a5eeeed3cbe0b7f2183cfa146c289dde8f28318b61a5eee34b79220973` |

## Remaining release gates

Follow [the sustained-run plan](../release-validation.md) and
[0.1 compatibility commitments](../compatibility.md). Obtain actual passing
baseline/lifecycle/recovery evidence and verify registry publishing permissions
before a formal 0.1 release. The optional 72-hour extension is recommended for
unattended production qualification. Neither this report nor a green CI claims
those stages have completed.
