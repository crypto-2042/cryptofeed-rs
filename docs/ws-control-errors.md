# Public WebSocket control failures

Verified 2026-10-12 against current official public control contracts. The runtime
previously treated every subscription rejection as permanent, including known
frequency protection and service restarts. Public runtime sessions now classify
verified transient codes before treating other rejections as configuration errors.

| Exchange | Exact representation and scope | Behavior | Official source |
| --- | --- | --- | --- |
| OKX v5 | event `error`, string code `60014` or `50011` | RateLimited, SDK 60-second floor | [WS errors and rate limits](https://app.okx.com/docs-v5/en/) |
| Bitget v3 | event `error`, string code `429`, `30006` or `30007` | RateLimited, SDK 60-second floor | [WS codes](https://www.bitget.com/docs/uta/error-code/websocket), [string envelope](https://www.bitget.com/docs/uta/quick-start) |
| OKX v5 | event `error`, string code `64007` or `64008` | Transient service failure; normal reconnect backoff | [WS errors](https://app.okx.com/docs-v5/en/) |
| Bitget v3 | event `error`, string code `30033` | Transient service upgrade; normal reconnect backoff | [WS codes](https://www.bitget.com/docs/uta/error-code/websocket) |
| Gate v4 | event `subscribe`, integer error.code `3` | Transient service failure; normal reconnect backoff | [Spot](https://www.gate.com/docs/developers/apiv4/ws/), [futures](https://www.gate.com/docs/developers/futures/ws/), [delivery](https://www.gate.com/docs/developers/delivery/ws/en/) |

No string/number coercion or diagnostic-text matching is used. Unknown or malformed
codes keep the existing rejection behavior. Private login quotas and trading WS
codes are excluded. In particular [Bybit's 10429/20003](https://bybit-exchange.github.io/docs/v5/error)
are documented under WS order entry; they are not promoted into public subscription
codes. Its [public connection contract](https://bybit-exchange.github.io/docs/v5/ws/connect)
remains the source for public subscriptions. Binance [stream control errors](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
remain distinct from REST weight errors and trading WebSocket API errors.

## Failure and recovery

`Error::RateLimited { code, retry_after }` preserves the verified native code with
an explicit retry floor. Sixty seconds is conservative SDK policy, not a venue
reset hint. The connection stops processing the failed attempt, withdraws retained
readiness and reconnects with a fresh attempt. Ordinary invalid subscriptions still
fail fast. A service error uses normal jittered exponential retry without inventing
a quota delay.

In live runtime sessions a quota response also extends the process-wide WS start
clock for that exchange. Other feeds/configuration groups cannot immediately create
new connections to that venue. Existing healthy sessions stay active, and other
exchanges keep their own clocks. A later shorter delay never shortens the clock.
The existing global/scoped capacity and pacing checks remain active. Queued WS
establishment retains its capacity reservation while waiting; cancellation releases
both permits. HTTP cooldown and weights are separate and unchanged.

Both supervisor APIs honor the retry floor, including bounded retries. The
shutdown-aware API can interrupt a cooldown promptly; a false watch notification
does not restart early or reset its timer. Existing retry limits/progress rules
still apply, so a caller can deliberately disable retries.

Raw parser replay returns the same typed native failure without sleeping, creating
a connection or changing live quota clocks. It does not simulate recovery timing.
Runtime processing without a readiness monitor also retains the transient class;
low-level pure adapter parsers do not themselves schedule retries.

## Evidence and remaining work

Ten new offline regressions cover exact codes/types/scopes, service failures,
readiness withdrawal, processing without a monitor, layered start-clock extension,
active-session preservation, cancellation, both supervisor APIs, false shutdown
notifications and offline raw replay. `sample_data/public-ws.control-errors.jsonl`
is a synthetic reference matching inline assertions, not a live throttling capture.

Existing connection/topic/frame budgets and paced subscription sends remain active.
[WS handshake status and scoped close-code distinctions](ws-lifecycle-failures.md)
are now implemented. Distributed/shared-IP coordination and adaptive
per-connection send quotas remain separate work. This
change does not certify sustained throughput or intentionally trigger a live ban.
