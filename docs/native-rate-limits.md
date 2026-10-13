# Native REST rate-limit responses

Shared HTTP admission now recognizes selected official REST quota codes in an
otherwise successful HTTP JSON response. This closes the gap where a caller
could receive HTTP 200 plus a native failure envelope and retry without shared
cooldown. HTTP 418/429 and Retry-After handling remain active as before.

| Exchange | Recognized JSON form | Official source |
| --- | --- | --- |
| Binance | Integer `code: -1003` | [Spot errors](https://github.com/binance/binance-spot-api-docs/blob/master/errors.md), [USD-M errors](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/error-code), [COIN-M errors](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/error-code) |
| Bybit v5 | Integer `retCode: 10006` | [Rate-limit rules](https://bybit-exchange.github.io/docs/v5/rate-limit) |
| OKX v5 | String `code: "50011"` | [Rate-limit rules](https://app.okx.com/docs-v5/en/#overview-rate-limits) |
| Bitget v3 | String `code: "429"` | [REST codes](https://www.bitget.com/docs/uta/error-code/restapi), [string envelope](https://www.bitget.com/docs/catalog/market-market-data/market-instruments) |
| Gate v4 | Existing HTTP 429 handling | [API reference](https://www.gate.com/docs/developers/apiv4/en/) |

Codes/representations are scoped to the exchange. Text messages are not searched,
string/number codes are not dynamically coerced, and success bodies are not
throttled merely because a header or message resembles a rate error. Private
order limits, Bybit WS order-entry 10429, OKX subaccount 50061 and Bitget's business
operation 25004 are not classified as public REST request quota. Gate's documented
TOO_MANY_REQUESTS label appears in the convert section; it is not promoted into a
generic successful-HTTP public market envelope without that evidence.

## Delay and response semantics

For a recognized quota envelope, the shared per-exchange cooldown uses:

1. A valid Retry-After duration/date, including an explicit zero delay.
2. For Bybit only, a positive delay to X-Bapi-Limit-Reset-Timestamp (epoch ms).
3. Otherwise the existing SDK 60-second fallback.

The fallback is an SDK choice, not a claim about each venue's quota window. A
malformed/past/current reset timestamp falls back. Hints outside the supported
Instant range are ignored in favor of another usable hint or the fallback. Existing later cooldowns are
never shortened. Diagnostic ban-until text is not parsed. Headers are read as
public delay hints and are not added to the recording schema.

Only the shared cooldown is changed: the original JSON Value is returned intact
and existing REST validators report its failure. No HTTP 429 is invented for an
HTTP-200 response. Ordinary standalone REST methods still do not automatically
retry. Directory fetch retains its bounded retry behavior, with subsequent
network attempts waiting through admission. Recognized quota envelopes are
explicitly rejected before directory cache insertion, including numeric Binance
errors that the previous string-only envelope check could otherwise miss.

Cooldown waits occupy neither process nor scoped HTTP network permits; other
exchanges can continue under their own clocks. [ResourceBudget](resource-budgets.md)
clocks remain enforced at actual SDK admission. Original TCP/TLS/server arrival
scheduling and endpoint weights are not inferred from these codes.

[Offline HTTP payload replay](http-replay.md) bypasses live admission, so replaying
a native rate body does not throttle unrelated live clients. It still passes the
same failure/caching validators; it does not simulate the old delay. Normalized
and causal raw replay clock policies remain separate.

## Validation

Deterministic mock responses and paused clocks cover HTTP-success quota JSON,
Retry-After/reset precedence and fallback, valid/malformed reset headers, deadline
extension, slot release/other-exchange isolation, false-positive code scopes and
native-body replay without live cooldown. A catalog replay regression proves
numeric native errors do not become cached directory successes. The substituted
reference is `sample_data/public-rest.native-quota.jsonl`, matching inline tests.

No live rate limit was intentionally triggered, no native API endpoint changed,
and no ban duration or new private API support is claimed. Native WS control
quotas, other venue endpoint weights and distributed/shared-IP coordination remain separate
work under the full plan.

[Binance endpoint weights](http-weights.md) now supplement these reactive
cooldowns with proactive rolling admission. Other venue weights, native WS
quota controls and distributed coordination remain separate work.
