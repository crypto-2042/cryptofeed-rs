# Authenticated HTTP proxy smoke — 2026-10-10

A temporary proxy listened only on loopback and required synthetic Basic
credentials. It forwarded CONNECT tunnels without decrypting destination TLS.
`proxy_public` used the same TransportConfig for standalone catalog refresh and
the managed Binance Spot BTC-USDT Trade/L2 feed. No exchange credentials or
private service were used.

| Observation | Result |
| --- | --- |
| Configuration output | HTTP proxy; credentials hidden |
| Catalog | 1,375 eligible normalized spot instruments |
| Proxy-authenticated upstream tunnels | api.binance.com:443, stream.binance.com:9443 |
| Runtime | Ready; Trade and L2 observed |
| Normalized event observations | 9 |
| Recovery owner | feed 1, generation 1, connection 1, epoch 1 |
| Local L2 revision and levels | revision 8, 1,004 bids / 1,001 asks |
| Cleanup | feed removed, runtime shutdown, process exit 0 |

Ready required native subscription confirmation and completed SDK L2 bootstrap;
Binance spot bootstrap fetched its REST snapshot using the same configured HTTP
client. Catalog and snapshot HTTP requests can reuse a single encrypted tunnel,
so the proxy's CONNECT count is not an HTTP request count. The proxy log retained
only authenticated target authorities, never authorization values, TLS contents
or normalized market payloads. The temporary forwarder was removed after the
observation.

This confirms actual authenticated HTTP and WS routing plus REST-assisted book
readiness. It is not throughput, every-exchange proxy certification, a TLS
interception test or a claim of reconnect/failed-proxy behavior during this short
window. Error/limit/redaction and cache-isolation paths are tested separately with
offline doubles. L2 counts are the observed local view, not a fixed full-exchange
depth guarantee.
