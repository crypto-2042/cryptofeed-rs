# WebSocket handshake and close failures

The runtime now preserves rejected HTTP upgrade status/delay hints and valid
WebSocket close-frame codes. These extend [native control-error handling](ws-control-errors.md)
without changing exchange endpoints, market parsers or supported products.

## HTTP upgrade and proxy CONNECT

Direct upgrades, upgrades after an HTTP proxy tunnel, and rejected CONNECT
responses retain `Error::HttpStatus { status, retry_after }`. Proxy authentication
407 keeps its existing InvalidConfiguration behavior. Other proxy failures do not
become exchange JSON errors; a status alone does not identify which network hop
rejected the route. Untrusted status reasons, headers, cookies and bodies are not
included in the structured failure. Non-HTTP proxied TLS diagnostics remain generic.

Retry-After accepts unsigned integer seconds or HTTP-date, including explicit
zero and past dates (zero). Signed/fractional/malformed values are ignored; the
bounded CONNECT parser also ignores duplicate Retry-After fields. REST now shares
this unsigned/date parser. The basis is [RFC 9110 §10.2.3](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3)
and [RFC 6585 §4](https://www.rfc-editor.org/rfc/rfc6585.html#section-4).

On live connection failure, a usable hint extends the venue's process-wide WS
start clock. HTTP 418/429 without a usable hint use the SDK 60-second fallback.
Hints outside the supported monotonic clock range do not overflow scheduling.
A transient status such as 503 without a usable hint uses ordinary retry backoff.
Permanent statuses retain the existing fail-fast list (400/401/403/404/405/410/422)
and do not install a cooldown, even if they carry a hint. Both supervisors honor
valid HTTP delay floors; configured retry counts and shutdown behavior still apply.

A proxy failure conservatively delays new WS starts for the requested exchange;
it is not an assertion about the exchange's native quota. HTTP request clocks are
separate. Failed/cancelled establishment releases global and scoped connection
permits; subsequent sessions can reuse them. Admission waits remain outside the
handshake deadline.

## Valid close frames

`Error::WebSocketClosed { code, retryable }` retains the optional numeric code.
The untrusted free-form reason is omitted. The runtime chooses retryability from
known protocol/venue meanings; the standards define meanings, not SDK retry policy.

| Received close | SDK behavior | Basis |
| --- | --- | --- |
| 1002/1003/1007/1008/1009/1010 | Fail fast: protocol/type/payload/policy/size/extension mismatch | [RFC 6455 §7.4.1](https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4.1) |
| OKX 4001/4002/4003/4008/4009 | Fail fast: login/request/subscription limits | [Official close-frame table](https://app.okx.com/docs-v5/en/#error-code-websocket) |
| Normal remote close, going away, server failures, missing code | Reconnect with ordinary backoff | SDK continuity policy; normal remote close is not caller shutdown |
| OKX 4004/4005/4006/4007/4013 | Reconnect with ordinary backoff | [Official close-frame table](https://app.okx.com/docs-v5/en/#error-code-websocket) |
| Unknown venue-specific code | Retain code and retry; do not guess a permanent or quota meaning | SDK compatibility policy |

OKX private-code meanings are applied only to an OKX session; the same number at
another venue is not promoted into that meaning. This adds no authenticated feed
support. Generic RFC policy violation 1008 is terminal SDK policy, not inferred
rate protection; arbitrary reason text is not searched for quota hints.

Failed sessions withdraw retained readiness and preserve their code in last_error.
An already requested shutdown wins over a queued close frame and remains clean.
Invalid frames/transport failures retain the existing transport-error behavior;
this classification describes valid received Close frames. No automatic
subscription reduction, endpoint guessing or reconnect ban-duration parsing occurs.

## Verification and remaining scope

Eleven new deterministic regressions cover status/hint sanitization, strict delay
parsing, scoped close codes, retry policy, shutdown/readiness, and real loopback
HTTP upgrade/CONNECT rejection. A failed 403 handshake releases a one-slot scope;
a subsequent successful session occupies and then releases the same slot. Existing
normal-close and transport-failure tests remain active.

`sample_data/public-ws.lifecycle.jsonl` is a synthetic reference matching inline
assertions. No live exchange rate limit was triggered. Local loopback tests require
permission to bind a local socket and do not access external exchange services.

Distributed/shared-IP coordination, adaptive per-connection send quotas, minimum
packing and independent capture merging remain unfinished. These classifications
do not certify sustained throughput or reproduce historical network timing in replay.
