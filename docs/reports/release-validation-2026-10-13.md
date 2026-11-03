# Release validation — packaging track re-run — 2026-10-13

Status: **packaging and independent-consumer gates PASS at `3851298`; sustained
live qualification (Preflight/Baseline/Lifecycle/Recovery/Soak/Shutdown) and
actual registry publication are NOT RUN.** A green packaging run is not a release
approval, and the evidence is commit-specific: later commits, including the
`0.1.0-rc.1` candidate, need their own run. See
[release-validation.md](../release-validation.md) and
[compatibility.md](../compatibility.md).

This is an independent re-run of the packaging and independent-consumer gates at
a later commit than
[release-package-verification-2026-10-13.md](release-package-verification-2026-10-13.md).
Archive hashes below replace the earlier ones **for this commit only**; a new
commit repacks and changes them again.

## Candidate and environment

- Commit `3851298d63184785350b3d76e50d32e181a62dc3` (`docs(release): record clean
  dry-run and queued-event semantics`), working tree clean (`dirty=0`); no
  `--allow-dirty` was used.
- Host: macOS 15.8 arm64 (Darwin 24.6.0), 10 CPUs. rustc/cargo 1.93.0
  (`254b59607` / `083ac5135`). MSRV compiler already installed: rustc 1.85.0
  (`4d91de4e4`). No toolchain upgrade or install was performed for the gates.
- All workspace crates 0.1.0, MIT, `rust-version = 1.85`.
- Raw logs, staged artifacts and the consumer harness:
  ignored `.local/release-gates-2026-10-13/` (`gates.log`, `package-checks-2.log`,
  `archive-inspection.txt`, `msrv-fresh.log`, `audit.json`, `metadata.json`,
  `license-summary.txt`, `resolved-lock-1.8{5,9}.toml`).

## Results

| Check | Result | Evidence / scope |
| --- | --- | --- |
| `cargo fmt --all --check`, `git diff --check` | PASS | Clean tree before and after the run; no source was modified. |
| `cargo check --workspace` | PASS | 5s, default features. |
| `cargo check --workspace --no-default-features` | PASS | 1s. |
| Per-feature `cargo check -p cryptofeed-rs --no-default-features --features <f>` | PASS | 9/9 (`ticker`, `trade`, `orderbook`, `candles`, `funding`, `liquidations`, `openinterest`, `index`, `markprice`), 1–2s each. |
| `cargo test --workspace --all-features` | PASS | 103s, **837 passed / 0 failed**: runtime unit 692, runtime integration 114 (catalog 5, feature_boundaries 10, public_parity 87, runtime_control 3, subscriptions 9), data crates 31, doc-tests 0. |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | PASS | 15s, no diagnostics. |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps` | PASS | 12s after fixing an invocation error (see Deviations). |
| Fresh Rust 1.85 all-target compilation | PASS | `cargo +1.85.0 generate-lockfile` (fresh resolution, 203 packages) then `CARGO_TARGET_DIR=<empty> cargo +1.85.0 check --workspace --all-features --all-targets` → rc=0, 14.7s, 222 fresh fingerprint directories / 180 `.rmeta` files in a previously nonexistent target dir. |
| `cargo publish --workspace --dry-run --all-features` | PASS | 65s; 11 archives packaged and build-verified (`Verifying`/`Compiling` each crate); the only warnings are the 11 expected `aborting upload due to dry run`. Workspace staging, not single-crate. |
| Archive inspection (11 `.crate`) | PASS | Every archive contains `LICENSE`, `THIRD_PARTY_NOTICES.md`, `README.md`, `Cargo.toml`, `src/`, `.cargo_vcs_info.json`, `Cargo.toml.orig`; no `.local`, `.git` or `target/` entries; no dependency `path` keys; `version = "0.1.0"`, `license = "MIT"`, `rust-version = "1.85"` in all 11. Cargo-generated archive `Cargo.lock` present, which the procedure allows; the repository still does not track one. |
| Archive VCS metadata | PASS | `.cargo_vcs_info.json` records `sha1 = 3851298d…` with no `dirty` flag. |
| Independent consumer, default features | PASS | 26s; entrypoint/control/builder + Trade Decimal/Serde roundtrip. |
| Independent consumer, no default features | PASS | 7s; entrypoint/control/builder only, no data feature required. |
| Independent consumer, explicit opt-in features | PASS | 12s; `trade` + `recording` without defaults. |
| Independent consumer, all features | PASS | 11s; 9 data features + `recording`, including an empty recording writer/reader roundtrip. |
| Extracted package tests | PASS | **837 passed / 0 failed** across all 11 extracted crates (runtime 806, core 11, orderbook 7, candles/funding/liquidations/ticker/trade 2 each, index/markprice/openinterest 1 each). |
| Fresh RustSec audit | PASS | `cargo-audit 0.22.2`, advisory DB commit `dabd3e7a…` (updated 2026-10-13T12:23:38+02:00): **0 vulnerabilities, 0 warnings**, 203 lockfile packages. |
| Dependency license metadata | PASS (metadata review) | 192 registry packages, none missing `license`/`license_file`; 16 distinct SPDX expressions (table below). Not a source-level legal audit. |
| Registry name/version lookup | PASS (read-only) | All 11 `/api/v1/crates/<name>` lookups returned HTTP 404, so neither name nor any version is taken. This reserves nothing and establishes no publishing permission. |
| Sustained-run stages | NOT RUN | Requires the dedicated Linux observer host described in the procedure. |
| Actual upload / owner / download / docs.rs verification | NOT RUN | Dry-run does not authenticate a publisher or perform server-side upload checks. |

## Archive SHA-256 (clean commit `3851298`)

| Archive | SHA-256 |
| --- | --- |
| `cryptofeed-candles-0.1.0.crate` | `471911ebc039d0c3dfda7e1a7d46dde9e92444e6ee921ce084e075d63aef3069` |
| `cryptofeed-core-0.1.0.crate` | `fd671ede54bcf6dc61fc0030a16c9c72a524587f37634d8e47a05cc2bf8cdb27` |
| `cryptofeed-funding-0.1.0.crate` | `9fbff0fc1ae279073a17783bbe1655536e4d564ce220461f0cd471a53f0ee166` |
| `cryptofeed-index-0.1.0.crate` | `4b053124e2b04059804a4723cbd4448fa7ef6263efcf8664867a1f343dd9cd29` |
| `cryptofeed-liquidations-0.1.0.crate` | `0c20f96a59419d7b458782074f2a95b42359849474b2a97abc87abdc84ec3d83` |
| `cryptofeed-markprice-0.1.0.crate` | `93f35a1061b04b0ebaea64f64fa6763512447786cf6aa32293bc3845f0bdbe23` |
| `cryptofeed-openinterest-0.1.0.crate` | `b4829898fa9fe8127db20475472705364e2f59642c179c3e0a9eabac51d2e156` |
| `cryptofeed-orderbook-0.1.0.crate` | `3e42f87eabcdbbdf079a6c3d8481217399cf426a29d166c2e09899d84e86bc17` |
| `cryptofeed-rs-0.1.0.crate` | `36993329a992b12c691cc801afb12f526ce9a6961170bf2aee4e933b2a4ffe06` |
| `cryptofeed-ticker-0.1.0.crate` | `ce1e4606da0b3068ee6d0fc9d01a64e0067ba341341dfc23e82fe8eed9af9b1f` |
| `cryptofeed-trade-0.1.0.crate` | `87ccd51c8c6e5a8c5afa2a5f11de1ee7c740f6975ebd0f0dda298f4b33476d00` |

Reproduction check (2026-11-03): a fresh worktree at the same commit re-ran
`cargo test --workspace --all-features` on macOS arm64 with Rust 1.93.0 and again
reported 837 passed / 0 failed, matching this table. No other gate was re-run,
so the remaining rows keep their original 2026-10-13 evidence.

## Method and commands

```sh
cargo fmt --all --check
git diff --check
cargo check --workspace
cargo check --workspace --no-default-features
for f in ticker trade orderbook candles funding liquidations openinterest index markprice; do
  cargo check -p cryptofeed-rs --no-default-features --features "$f"
done
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.85.0 generate-lockfile
CARGO_TARGET_DIR=<empty dir> cargo +1.85.0 check --workspace --all-features --all-targets
cargo publish --workspace --dry-run --all-features
cargo-audit audit --json --db ~/.cargo/advisory-db --no-fetch
cargo metadata --format-version 1 --all-features
```

The 11 archives under `target/package/tmp-registry/` were listed with `tar -tzf`
and hashed with `shasum -a 256` (full listing in `archive-inspection.txt`).
Extraction and all consumer builds happened in a `mktemp -d` directory outside
the checkout; no consumer or extracted manifest referenced `crates/` in the
checkout.

Archive packaging removed the need for an unpublished-sibling registry: a staged
workspace root was created next to the extracted crates with
`[patch.crates-io]` entries pointing **only** at extracted directories, and the
extracted suites ran through it (`cargo test -p cryptofeed-<c> --all-features`).
That root was deleted before the three consumer roots ran, each with its own
`[patch.crates-io]` list, so the consumer runs never saw the staged workspace.
Patches simulate the unpublished siblings; they do not establish registry
ownership or test registry download.

Fresh resolution under both compilers produced identical package sets (192
registry + 11 workspace = 203). Resolver 3 is MSRV-aware: because the workspace
declares `rust-version = "1.85"`, the 1.93.0 run also selected
`idna_adapter 1.2.1` and its Rust-1.86-incompatible successors were avoided. A
downstream consumer that declares no `rust-version` does resolve newer
transitive versions (it pulled `idna_adapter 1.2.2`), which is expected and not
a defect of the packaged crates.

## Resolved security-sensitive versions

- reqwest 0.13.5; tokio-tungstenite/tungstenite 0.30.0.
- rustls 0.23.45 (workspace minimum is also 0.23.45); tokio 1.53.2.
- serde_json 1.0.154 with `arbitrary_precision` and `float_roundtrip`;
  rust_decimal 1.43.0.
- RustSec DB commit `dabd3e7a1d8694b3d3294bab6a514ba2288abf78`, updated
  `2026-10-13T12:23:38+02:00`. An audit only detects known published issues.

## Registry dependency license expressions (192 packages)

| Declared expression | Packages |
| --- | --- |
| `MIT OR Apache-2.0` | 113 |
| `MIT` | 27 |
| `Unicode-3.0` | 18 |
| `Apache-2.0 OR MIT` | 10 |
| `Apache-2.0 OR ISC OR MIT` | 3 |
| `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` | 3 |
| `CDLA-Permissive-2.0` | 3 |
| `Apache-2.0` | 2 |
| `BSD-2-Clause OR Apache-2.0 OR MIT` | 2 |
| `ISC` | 2 |
| `MIT OR Apache-2.0 OR LGPL-2.1-or-later` | 2 |
| `Unlicense OR MIT` | 2 |
| `Unlicense/MIT` | 2 |
| `(MIT OR Apache-2.0) AND Unicode-3.0` | 1 |
| `Apache-2.0 AND ISC` | 1 |
| `BSD-3-Clause` | 1 |

Counts cover the full metadata graph, not just the host's compiled set. For
alternative expressions pick a permissive option; retain applicable notices and
any combined Unicode/ISC obligations. Project MIT licensing does not relicense
dependencies, and the 192 packages above (plus their notices) still require a
maintainer-verified source-level review before release.

## Deviations, environment adaptations and limitations

These do not change packaged sources or test assertions, but a reviewer should
see them.

1. **Sandboxed `~/.cargo` writes.** The file sandbox denied creating directories
   under `~/.cargo/registry/{cache,src}`, so the first consumer attempt failed
   only at unpacking `idna_adapter 1.2.2`, and `cargo metadata --all-features`
   failed only at `rustls-native-certs 0.8.5`. Both were re-run with
   `CARGO_HOME=.local/release-gates-2026-10-13/cargo-home` (checkout-local) and
   then passed. The workspace itself needed no adaptation.
2. **`cargo-audit` was not installed.** It was installed offline from the local
   crate cache into `.local/…/tools` (0.22.2) and pointed at the existing local
   advisory clone with `--no-fetch`; the advisory DB revision is recorded above.
   Network access to crates.io, not to any exchange, was used for dependency
   resolution.
3. **First rustdoc invocation failed with rc=127** because
   `RUSTDOCFLAGS=-D warnings` was unquoted and `env` treated `warnings` as the
   program. Re-run correctly; result PASS.
4. **First archive check produced a false `PATH_DEP_FOUND`** because the pattern
   matched package-internal target paths (`src/lib.rs`, `examples/*`, `tests/*`).
   A corrected dependency-only pattern (`path = "../…"`) found none in all 11
   archives; both outputs are retained in `archive-inspection.txt`.
5. **First extracted-suite attempt failed on resolution**, not on tests, because
   crates.io has no `cryptofeed-*` packages. Fixed with the staged workspace
   root described above; the retake passed 837/837.
6. **Host is macOS, not the dedicated Linux observer host** the sustained-run
   section requires. This run produces no live-exchange traffic, no RSS/lag/RSS
   evidence and no sustained resilience claim.
7. **Archive hashes are per-commit.** They identify this clean-commit packaging
   only and must be regenerated on any later commit, including before an actual
   upload.

## Remaining gates before a 0.1 release

1. Sustained-run stages (Preflight → Baseline 24h → Lifecycle 4h → Recovery 4h
   → optional 72h soak, with per-stage shutdown) on the dedicated host, using a
   dedicated observer that is not yet implemented: the current `release_smoke`
   prints counters at exit only and is explicitly only a connectivity probe.
   Acceptance thresholds in [release-validation.md](../release-validation.md)
   must be frozen before that run.
2. Authorized publisher, owner/token permission, actual uploads in
   core → nine data crates → runtime order, then registry download and docs.rs
   verification.
3. Maintainer source-level license/notice review of the 192 registry packages.

No exception was silently waived in this run; every non-PASS item above is
either fixed and re-run, or explicitly NOT RUN.
