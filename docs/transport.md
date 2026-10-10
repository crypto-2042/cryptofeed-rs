# Explicit HTTP and WebSocket transport

`TransportConfig` routes a feed's instrument discovery, paginated/periodic
catalog refresh, Binance/Gate L2 snapshot and resnapshot requests, and every
physical WebSocket connection consistently. Clone one configuration to reuse its
HTTP client/pool and catalog cache identity across feeds.

```rust
use cryptofeed_rs::prelude::*;

fn configure() -> Result<ExchangeFeed, Box<dyn std::error::Error>> {
    let transport = TransportConfig::http_proxy("http://127.0.0.1:8080")?;
    // If required, get credentials from your application secret/config source:
    // let transport = transport.basic_auth(username, password)?;
    Ok(Binance::new().trade().l2_book().symbol("BTC-USDT").transport(transport).build())
}
```

Standalone discovery uses
`MarketCatalog::load_with_transport(exchange, product, &transport)` or
`refresh_with_transport`. Plain load/refresh remains direct. `DiscoveryFeed`
automatically uses its template's transport for the initial and every periodic
refresh as well as its replacement subscriptions. Runtime options and routing
are independent: `.runtime_options(...)` configures budgets;
`.transport(...)` configures how requests reach their destination.

## Supported proxy and authentication

The endpoint must be `http://host:port` (port defaults to 80), with a root path
and no userinfo, query or fragment. This increment supports HTTP proxies with
HTTP CONNECT for WebSockets and HTTP/HTTPS request proxying through reqwest.
HTTPS proxy endpoints, SOCKS, PAC, custom CA/mTLS and destination bypass rules
are not claimed. Invalid/unsupported endpoints fail at construction; proxy
failure never silently falls back to a direct connection.

Set Basic authentication with `.basic_auth(username, password)?`, rather than
embedding credentials in a URL. A colon in the username is rejected; passwords
are Base64-encoded, never inserted raw into request headers. Basic credentials
are sent to the proxy, not the exchange. `TransportConfig` Debug hides the
endpoint/authentication, generated authorization headers are marked sensitive,
and CONNECT errors report generic stages/numeric status without quoting proxy
reason/body text. HTTP clients retain no credential-bearing URLs. Proxy CONNECT
407 is a configuration failure and does not burn transient WebSocket retries.

CONNECT uses the original destination authority, including bracketed IPv6 and
port. The SDK bounds response headers to 16 KiB, accepts successful 2xx statuses,
handles interim responses and reads only through the header terminator so TLS
bytes remain untouched. TLS/SNI/certificate validation uses the original exchange
hostname and the existing tokio-tungstenite rustls path; CONNECT is not TLS
interception or a reason to disable certificate verification. The existing
connection deadline covers proxy TCP, CONNECT, destination TLS and WS handshake;
shutdown cancels the owning establishment future.

## Defaults, cache and budgets

Default `TransportConfig::direct()` is explicit for HTTP and WS. The SDK does not
automatically read HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NO_PROXY or system proxy
settings. Earlier HTTP-only reqwest system-proxy inference was inconsistent with
direct WS; callers relying on that must now supply an explicit configuration.
The example reads application-specific proxy environment variables deliberately,
not through hidden library environment discovery.

Cloned proxy configurations share one HTTP client and one catalog cache scope.
Separately constructed configurations, including separately authenticated
configurations at the same endpoint, have different process-local cache keys;
direct and proxy responses are not coalesced or substituted for each other.
No credentials are used in cache keys/log fields. Reuse clones rather than
reconstructing a proxy for every polling cycle. The existing 24-hour TTL and
forced-refresh behavior apply independently within each scope.

Shared connection admission/start pacing and snapshot concurrency/start pacing
remain process-wide across routes. Adding proxies does not multiply those SDK
budgets or account for all other clients sharing an IP. Snapshot and directory
response limits and deadlines remain intact. Normalized models and exchange
protocol endpoints are unchanged; proxy authentication is not an authenticated
exchange feed or trading capability.

## Evidence and protocol references

Offline duplex tests verify authenticated CONNECT, IPv6, interim success,
non-overread into tunnel data, malformed/rejected/incomplete/bounded responses,
credential redaction, clone/client/cache isolation and planning preservation.
They do not need a local listening socket or external service. Existing lifecycle
and handshake timeout tests remain enabled.

The manual `proxy_public` example reads `CRYPTOFEED_HTTP_PROXY` and optional
`CRYPTOFEED_PROXY_USER`/`CRYPTOFEED_PROXY_PASSWORD`, then exercises Binance directory,
Trade/L2 WS and REST bootstrap. The dated
[authenticated proxy smoke](reports/live-smoke-proxy-2026-10-10.md) records an
actual public-service observation through a temporary local forwarding proxy;
that proxy and test credentials are not deployment configuration.

Sources: [HTTP CONNECT semantics, RFC 9110 §9.3.6](https://www.rfc-editor.org/rfc/rfc9110.html#name-connect),
[reqwest 0.12 Proxy source](https://github.com/seanmonstar/reqwest/blob/v0.12.24/src/proxy.rs),
[tokio-tungstenite 0.24 TLS client source](https://github.com/snapview/tokio-tungstenite/blob/v0.24.0/src/tls.rs).
These transport references do not replace exchange-specific protocol baselines.
