# AI Coding Workflow

This document describes the working agreement for AI-assisted changes in
`cryptofeed-rs`. Repository-level requirements in `AGENTS.md` remain the source
of truth when they are more specific.

## Project Intent

`cryptofeed-rs` is a pure Rust SDK for normalized cryptocurrency market data.
Public model and handler APIs live in category crates, while exchange protocol,
transport, parsing, routing, reconnect, and shutdown logic live in
`crates/runtime`.

The active scope is public market data. Do not add authenticated or private API
support unless a later plan explicitly expands that scope.

## Before Editing

1. Read `AGENTS.md` and the nearest relevant crate code and tests.
2. Check `git status --short` and preserve unrelated user changes.
3. Read `PARITY.md` before changing exchange coverage or normalized behavior.
4. Verify the latest stable official exchange API documentation before changing
   an adapter, endpoint, channel, or payload shape.
5. Choose the smallest change that satisfies the requested behavior.

The Python project is a semantic migration reference. It is not authoritative
for current exchange protocols or API versions.

## Change Boundaries

- Put shared abstractions in `crates/core`.
- Put normalized public models and handler traits in their category crate.
- Keep exchange-specific parsing and runtime behavior in `crates/runtime`.
- Preserve the `FeedHandler` entrypoint and explicit normalized parsing.
- Avoid unrelated refactors, dependency additions, formatting churn, or new
  configuration layers.
- Add or update a focused test when behavior changes.

## Development Loop

Run commands from the workspace root.

1. Reproduce the behavior with the narrowest relevant unit or integration test.
2. Make the surgical implementation change.
3. Re-run the focused test.
4. Run workspace verification appropriate to the change.
5. Review the diff for unrelated edits and generated files.

Useful commands:

```bash
cargo check --workspace
cargo test -p cryptofeed-rs --test public_parity
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

`make check`, `make test`, `make fmt`, and `make clippy` are convenience aliases.
The canonical completion checks are the commands listed in `AGENTS.md`.

## Harness and Live Smoke Tests

The deterministic public parity workflow is documented in `docs/harness.md`.
Use it for fixture captures, parser normalization assertions, and focused parity
verification.

Examples under `crates/runtime/examples/` connect to live exchange services and
are manual smoke tests. They require network access, can be affected by remote
availability or API changes, and are not substitutes for offline tests.

## Documentation Updates

Update documentation in the same change when its stated facts change:

- `README.md`: user-facing capabilities, examples, or setup.
- `PARITY.md`: exchange/channel support and expansion gates.
- `PROGRESS.md`: meaningful implementation milestones.
- `MEMORY.md`: durable project constraints or recurring operational lessons.
- `docs/harness.md`: fixture formats, commands, outputs, or safety rules.

Do not record secrets, credentials, private account data, or transient debugging
details in repository documentation or fixtures.

## Repository Hygiene

Do not commit `target/` or `Cargo.lock` under the current workspace policy.
Cargo commands may regenerate both locally. Before acting on a reported
`.git/index.lock`, verify that the lock still exists and is not owned by an
active Git process.
