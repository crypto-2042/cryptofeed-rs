# Automatic symbol reconciliation

`DiscoveryFeed` is an opt-in owner for a managed feed. It periodically refreshes
one exchange/product directory, expands normalized-name patterns, and replaces
its feed when selected channel/symbol sets or native names change. This is a
Rust convenience extension: the inspected Python core refreshes mappings
explicitly and does not provide this periodic reconciliation loop.

```rust,no_run
use cryptofeed_rs::prelude::*;
use std::time::Duration;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let control = handler.control_handle();
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown));
    let discovery = DiscoveryFeed::new(
        Okx::new().trade().build(),
        InstrumentKind::Spot,
        ["BTC-USDT", "ETH-USDT"],
    )?
    .interval(Duration::from_secs(300))?
    .start(control.clone())
    .await?;
    // Retain discovery while the application runs. At service shutdown:
    let stopped = discovery.stop().await?;
    control.remove_feed(stopped.identity.id).await?;
    control.shutdown().await?;
    running.await??;
    Ok(())
}
```

Run `cargo run -p cryptofeed-rs --example discovery_public` for a public OKX
observation with one periodic refresh and cleanup. This needs network access
and is not a CI gate. New listing/removal behavior is tested with scripted
catalogs; a short live observation cannot guarantee a real listing change.

## Configuration and selection

The template contains channels, handlers and settings, with no symbols, native
mappings, per-channel symbol sets or registered identity. Product/channel/feature
capabilities are checked before HTTP. `.channel_patterns(channel, patterns)`
overrides selection for a channel already present in the template. Patterns use
MarketCatalog's case-insensitive whole-name `*` and `?` matching.

Initial startup forces a directory refresh; every supplied pattern must match.
It fails before registration on refresh/selection errors. Later cycles allow
individual patterns to disappear: removed symbols are omitted, and a temporarily
empty channel is omitted while another channel still has symbols. If the entire
selection is empty, reconciliation reports Backoff and preserves the last
nonempty configuration. It does not start an idle subscription for future-only
patterns. All selected native names come from the same fetched catalog snapshot.

Catalogs filter explicitly unavailable statuses: Binance `TRADING`, Bybit
`Trading`, Bitget `online/limit_open/limit_close`, and Gate spot
`tradable/buyable/sellable` remain eligible. Bitget order restrictions and Gate
one-direction trading restrictions do not by themselves forbid public data.
OKX's existing `live` and Gate derivative delisting filters remain in place.
Minimal reference rows without optional status retain existing compatibility;
an explicit malformed status fails refresh. These are directory eligibility
rules, not guarantees of activity, order permissions or readiness for every
channel. Precision/full market metadata is still a separate gap.

## Lifecycle, ownership and failure

The default interval is five minutes; the SDK minimum is one minute. Polling is
sequential and begins after the preceding cycle settles. Refresh/selection or
replacement failures preserve the last owned selection and use exponential
backoff: interval, twice interval, and so on, capped at the greater of one hour
and the configured interval. Successful cycles reset failures. Existing shared
HTTP request, connection and snapshot budgets still apply; the interval floor
is not an official exchange quota.

Unchanged selection/native names do not restart a healthy feed. Failed, degraded
or stopped feeds may be restarted on a successful cycle. Changed feeds use the
existing validated, admitted, drain-then-start replacement, with a stable feed ID
and new configuration generation. There is a data gap; switching is not atomic.
`Current` means directory reconciliation succeeded, not remote subscription
readiness. Query `control.state(id)` for readiness and runtime diagnostics.

Every update uses `replace_feed_if_current(identity, candidate)`. A concurrent
manual replacement or removal ends discovery with `OwnershipLost`; discovery
never takes that ID back. Failed candidate attempts may consume configuration
generation numbers without changing the current identity. Busy commands are
retried through the normal backoff. The compare-and-replace check occurs inside
the logical feed worker before preparation, so a preceding state query alone is
not used as ownership protection.

`DiscoveryHandle::snapshot()` and `subscribe()` expose retained/coalesced state:
last owned identity and selections, refresh attempts (including initial load),
successful replacement commands, failure count, next delay, last successful
catalog-fetch time and error. These are not a durable event log.

`stop().await` cancels timers/in-flight directory work and waits for an already
accepted replacement to settle, returning the final owned identity. Dropping
the handle also stops future polling; an accepted replacement can finish.
Stopping discovery leaves the managed feed registered; explicitly remove it if
needed. Runtime shutdown stops the polling owner. No polling is enabled by
default, and no Binance OI polling fallback is introduced.
