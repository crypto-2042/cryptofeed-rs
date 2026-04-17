# AGENTS

These instructions apply to all work under `rust/cryptofeed-rs/`.

## Mission

- Build `cryptofeed-rs` as a pure Rust SDK.
- Keep the workspace split by data category, not by per-exchange crates.
- Keep exchange protocol and transport logic centralized in `crates/runtime`.
- Keep the active delivery scope on public market data unless a later plan explicitly expands scope.

## Workspace Rules

- Shared abstractions belong in `crates/core`.
- Normalized public models and handler traits belong in:
  - `crates/ticker`
  - `crates/trade`
  - `crates/orderbook`
- Runtime orchestration, exchange adapters, websocket/http integration, routing, reconnect, and shutdown logic belong in `crates/runtime`.
- Do not move exchange-specific parsing into the category crates.

## Exchange API Rules

- New exchange implementations must use the latest official API.
- Do not copy a legacy Python exchange version into Rust just because it already exists.
- The Python codebase is a semantic migration reference, not the Rust source of truth.
- Bitget must target official v3 API surfaces.
- Before implementing or updating an exchange adapter, verify the current official REST and WebSocket documentation.

## Runtime Rules

- Preserve the `FeedHandler` user-facing entrypoint.
- Prefer explicit normalized parsing over opaque dynamic conversion.
- Keep runtime behavior changes covered by focused tests.
- If the Rust implementation moves beyond the original design/plan, update the docs instead of silently drifting.

## Verification

Run from `rust/cryptofeed-rs/` when relevant:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-features -- -D warnings`
- `cargo test --workspace`

## Repository Hygiene

- Do not commit `target/`.
- Do not commit `Cargo.lock` for this workspace unless policy intentionally changes.
- This repository often shows transient `.git/index.lock` collisions; verify the lock still exists before acting.
- Preserve unrelated user changes outside `rust/cryptofeed-rs/`.

## Documentation Rules

- Keep `README.md` aligned with actual workspace capabilities.
- Update `PROGRESS.md` after meaningful Rust milestones.
- Update `MEMORY.md` when new project-specific constraints, recurring issues, or operational lessons are learned.
