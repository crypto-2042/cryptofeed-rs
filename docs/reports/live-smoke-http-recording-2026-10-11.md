# Public HTTP recording smoke — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example http_recording_public -- NEW_PATH`.
Unauthenticated OKX Spot directory plus ticker, five-level L2 and five recent
trades using existing PublicRestClient/current adapters. Direct transport with
capture attached before directory loading. File used create_new in /private/tmp
and is not committed. Exit 0.

| Check | Observed |
| --- | --- |
| Directory | 1,152 normalized market symbols |
| GET attempts | 4 |
| JSON outcomes | 4 |
| Failures/cancellations | 0/0 |
| Recording | 8 observations, 1,252,409 bytes, Complete |
| Offline reader | Full request/result lifecycle, privacy, numeric JSON and footer/count validation completed |

Console: `public HTTP markets=1152 requests=4 json=4 failures=0 cancelled=0 observations=8 bytes=1252409 validated=true`.
Reader validation made no new requests. It did not inject archived responses into
SDK calls, recreate a catalog offline or certify normalized-clock replay; those
integrations remain pending. This is one venue/Spot bounded workflow, not all
endpoints/products, sustained load, authentication or fsync/durability evidence.
Deterministic tests separately cover five-venue public URL shapes, reordered
completion IDs, cancellation, privacy, limits and failure notification.
