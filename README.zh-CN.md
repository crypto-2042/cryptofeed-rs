# cryptofeed-rs

[English](README.md) | [简体中文](README.zh-CN.md)

面向加密货币交易所的归一化公开行情，使用 Rust 编写，适配各所当前官方协议。

`cryptofeed-rs` 是 Python
[`cryptofeed`](https://github.com/bmoscon/cryptofeed) 的 Rust 对应实现：沿用相同的归一化事件模型，但适配器以交易所当前公布的真实 API 为准。Python 只是语义参考，不是协议事实来源。

- **范围：** 仅公开、无需认证的行情。私有数据、账户数据、下单与交易不在范围内。
- **产品：** 13 家交易所的现货、永续/掉期与交割合约。
- **能力：** 有界广播环、就绪/重连监管、托管运行时控制、录制/回放、L2 恢复与结构化 sink。
- **状态：** `0.1.0-rc.1` 评估候选版本。用于无人值守生产前，请先阅读
  [RC 范围与已知限制](docs/release-candidate.md)。

## 快速开始

需要 Rust 1.85 或更高版本。默认启用全部公开数据 feature。本地开发可用 path 依赖；RC 正式发布前建议固定 Git revision：

```toml
[dependencies]
cryptofeed-rs = { path = "path/to/cryptofeed-rs/crates/runtime" }
async-trait = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use std::sync::Arc;

use async_trait::async_trait;
use cryptofeed_rs::prelude::*;

struct Print;

#[async_trait]
impl TradeHandler for Print {
    async fn on_trade(&self, trade: Trade) {
        println!("{} {} {}", trade.symbol.as_str(), trade.price, trade.amount);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    handler.add_feed(
        Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .trade_handler(Arc::new(Print))
            .build(),
    );
    handler.run().await?;
    Ok(())
}
```

`run()` 会自动安装 Ctrl-C 处理。若进程自行管理生命周期，可改用
`run_with_shutdown(handler, watch)` 或下文的管理控制 API。

### 用事件流代替回调

Handler 在 feed 会话内执行，应尽快返回。需要较慢处理的应用可消费有界、可丢事件的广播流：

```rust
let mut events = handler.subscribe();
handler.add_feed(Binance::new().trade().symbol("BTC-USDT").build());

tokio::spawn(async move {
    loop {
        match events.recv().await {
            Ok(FeedEvent::Trade(trade)) => println!("{} {}", trade.symbol.as_str(), trade.price),
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("consumer lagged; {n} events overwritten");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
});

handler.run().await?;
```

广播消费者变慢时会按设计丢弃最旧事件，但永远不会阻塞 feed。保留策略与诊断见
[事件投递](docs/event-streams.md)。

### 托管运行时

需要增删/替换 feed、权威就绪状态和连接锚定事件时，使用 `control_handle()`：

```rust
let mut handler = FeedHandler::new();
let control = handler.control_handle();
let mut events = handler.subscribe_anchored_with_diagnostics();
let (stop, shutdown) = tokio::sync::watch::channel(false);
let running = tokio::spawn(handler.run_with_shutdown(shutdown));

let identity = control
    .add_feed(Binance::new().trade().symbol("BTC-USDT").build())
    .await?;
let state = control.state(identity.id).await?; // Ready/Recovering/failed
let _ = (stop, running);
```

参见 [就绪状态](docs/readiness.md) 与
[连接锚定事件](docs/readiness.md#connection-anchored-market-events)。

## 核心模型

十类归一化公开事件，每类一个 crate：

| 类别 | 类型 | 典型交易所 |
| --- | --- | --- |
| Ticker（BBO） | `Ticker` | Binance、Bitget、Bybit、OKX、Gate、KuCoin、Kraken、Coinbase、HTX、Hyperliquid |
| 成交 | `Trade` | 全部支持交易所 |
| L2 盘口 | `L2Book`（快照/增量） | 全部支持交易所 |
| L1 盘口 | `L1Book` | 提供真实带量 BBO 频道的交易所 |
| K 线 | `Candle` | 多数交易所（周期不同） |
| 资金费率 | `Funding` | 衍生品交易所 |
| 强平 | `Liquidation` | 提供公开强平流的交易所 |
| 持仓量 | `OpenInterest` | 衍生品交易所 |
| 指数价格 | `IndexPrice` | 衍生品交易所（另有 OKX 现货显式指数） |
| 标记价格 | `MarkPrice` | 衍生品交易所；Hyperliquid 现货 ctx |

- `Symbol` 带产品限定（`BTC-USDT` 现货与 `BTC-USDT-PERP` 永续），不含交易所分隔符。
- 价格与数量使用 `rust_decimal::Decimal`；交易所时间与接收时间分开保存。除非交易所明确要求换算（如 KuCoin 合约张数与基础币数量），否则保留原生单位。
- `FeedEvent` 是 non-exhaustive 的：下游 `match` 需要 `_` 分支。
- 能力预检会在建立任何连接前拒绝不支持的产品/频道/周期/深度组合。

## 支持的交易所

能力表与运行时校验使用同一份数据（`markets::capability_matrix()`）。打勾表示该公开频道已实现，并有确定性的全会话 double 覆盖。

### 现货

| 交易所 | Ticker | 成交 | L2 | L1 | K 线 | 指数 |
| --- | --- | --- | --- | --- | --- | --- |
| Binance | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| Bitget v3 | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| Bybit v5 | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| OKX v5 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ 显式指数 |
| Gate.io v4 | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| KuCoin UTA WS V2 | ✓ | ✓ | ✓ | — | ✓ | — |
| Kraken Spot WS v2 | ✓ | ✓ | ✓ | — | ✓ | — |
| Upbit 报价 WS | — | ✓ | ✓ | — | ✓ | — |
| Bithumb 公开 WS | — | ✓ | ✓ | — | — | — |
| Coinbase Advanced Trade | ✓ | ✓ | ✓ | ✓ | ✓ 仅 5m | — |
| HTX 现货 | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| Hyperliquid 现货 | ✓ | ✓ | ✓ | ✓ | ✓ | — |

### 永续 / 掉期

| 交易所 | Ticker | 成交 | L2 | L1 | K 线 | 资金费率 | 强平 | 持仓量 | 指数 | 标记 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance USD-M / 币本位 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ |
| Bitget v3（USDT/USDC/币本位） | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bybit v5 线性 / 反向 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| OKX v5 SWAP | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Gate.io v4（USDT/BTC） | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ 采样 | ✓ | ✓ | ✓ |
| KuCoin UTA V2 | ✓ | ✓ | ✓ | — | ✓ | ✓ | — | ✓ | ✓ | ✓ |
| Deribit JSON-RPC v2 | ✓ | ✓ | ✓ | — | ✓ | ✓ | — | ✓ | ✓ | ✓ |
| HTX USDT-M | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — |
| Hyperliquid | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | ✓ |

### 交割合约

| 交易所 | Ticker | 成交 | L2 | L1 | K 线 | 强平 | 持仓量 | 指数 | 标记 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance USD-M / 币本位 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ |
| Bitget v3 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bybit v5 线性 / 反向 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| OKX v5 FUTURES | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Gate.io v4 USDT 交割 | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | ✓ |
| Deribit JSON-RPC v2 | ✓ | ✓ | ✓ | — | ✓ | — | ✓ | ✓ | ✓ |

期权、OKX MARGIN 与 L3 盘口不属于 0.1 范围，会被预检拒绝。资金费率只存在于永续/掉期行。`—` 表示该所没有适用的公开频道，或 SDK 尚无经过验证的实现。

各所差异（影响如何解读单元格）：

- **韩国交易所：** Upbit/Bithumb 的公开 `ticker` 帧没有买一卖一，因此没有归一化 `Ticker`/`L1Book`；Bithumb 没有公开 K 线流。
- **KuCoin：** 没有独立 L1 频道；`obu depth=1` 只是快照型 L2。期货 `mark-price` 拆分为标记/指数/持仓量；合约数量若要换算为基础币，需使用目录元数据。
- **Kraken：** 仅现货；`book` 对前 10 档做 CRC32 校验；`ticker` 不是 `L1Book` 频道。
- **Coinbase：** 公开 K 线只有 5 分钟；`level2` 依赖重连恢复（无逐产品序列）。
- **Deribit：** 拒绝期权；`book` 使用 change-id 链式校验。
- **HTX：** 资金费率/强平走独立 notification 主机；标记价格、指数与持仓量当前没有公开 topic。
- **Hyperliquid：** `l2Book` 是整簿快照（默认 20 档，`fast` 为 5 档），不是合成增量流；无公开强平。
- **Gate.io：** 永续强平是采样公开流；交割强平被拒绝。
- **Binance：** 合约持仓量没有经过验证的公开 WS 路径，也不会在后台偷偷轮询。

每家交易所的协议基线（官方线格式、确认、心跳与已知缺口）见
[docs/](docs/README.md)。

## 功能指南

### Handler

每个类别都有 handler trait。可为每个类别注册一个主 handler，并追加任意数量的附加 handler；回调串行执行，默认 5 秒超时，并隔离可捕获的 panic。详见
[handler 语义](docs/handlers.md)。

```rust
let feed = Okx::new()
    .trade()
    .l2_book()
    .symbol("BTC-USDT")
    .trade_handler(Arc::new(Print))
    .add_orderbook_handler(Arc::new(Print))
    .build();
```

### 就绪、重试与关闭

托管 feed 暴露 `Preparing → Connecting → Subscribed → Ready`、失败与恢复状态、逐连接订阅确认、epoch 与盘口恢复诊断。暂态错误按有界退避重连；配置/协议类终止错误不会被无限重试。参见
[就绪状态](docs/readiness.md) 与 [运行时选项](docs/runtime-options.md)。

### 录制与回放

可选的 `recording` feature 写入脱敏的原始或归一化流，支持回放与 HTTP 目录回放。参见
[录制](docs/raw-recording.md)（[HTTP 捕获](docs/http-recording.md)）与
[回放](docs/raw-replay.md)（[HTTP 回放](docs/http-replay.md)）。

### L2 恢复与投递深度

可选的 L2 handle 提供原子拷贝的快照加有界更新流；无法桥接的缺口会显式失效。投递的 top-N 深度在快照/增量之间保持一致。参见
[L2 恢复](docs/l2-recovery.md) 与
[投递深度](docs/book-delivery-depth.md)。

### Sink

`run_sink` 从归一化事件流驱动顺序、可失败的 `EventSink`。广播 lag 对 sink 是终止错误，绝不会变成“静默截断的采集”。参见
[sink](docs/sinks.md)。

### 目录与 REST

产品限定的目录发现、显式/`*`/`?` 符号选择、`MarketInfo` 约束，以及用于 ticker/盘口快照的 `PublicRestClient`，都无需先建立 WebSocket。参见
[目录发现](docs/discovery.md)、
[市场元数据](docs/market-metadata.md) 与 [公开 REST](docs/public-rest.md)。

### 传输与代理

`TransportConfig` 将显式代理配置一致地应用到目录、REST 快照与 WebSocket，默认直连。参见
[传输](docs/transport.md)。

## 标的命名与代币化股票

各所原生格式不同（`BTCUSDT`、`BTC-USDT`、`BTC_USDT`、`BTC/USD`、
`KRW-BTC`、小写 `btcusdt`、Hyperliquid `BTC` / `PURR/USDC`）。SDK 归一化 base/quote 与产品类型，同时目录保留精确的原生映射。逐所参考见
[标的命名](docs/instrument-naming.md)。

部分交易所以现货对形式提供**代币化股票 / RWA 股票产品**，命名方案各不相同：

| 交易所 | 模式 | 示例 |
| --- | --- | --- |
| Binance | `<TICKER>B` | `NVDAB`、`AAPLB` |
| OKX | `X<TICKER>` | `XAAPL` |
| Bybit、KuCoin、Gate | `<TICKER>X` | `AAPLX`、`TSLAX` |
| Bitget | `r<TICKER>` | `rAAPL` → `RAAPL-USDT` |
| Gate（Ondo） | `<TICKER>ON` | `TSLAON` |
| Gate（杠杆） | `<TICKER>3L` / `3S` | `AAPL3L` |
| HTX | 无标记小写 | `aapl`、`msft`（多数已下线） |

**这些产品不与加密资产统一语义。** 它们不是美股行情：没有 SIP/合并行情、没有盘前盘后或集合竞价、也没有公司行动流。同一底层资产可能对应多个发行方代币，价格与流动性不同（`TSLAX` vs `TSLAON`）；`3L`/`3S` 是每日再平衡产品，不是股票敞口。把其映射为单一股票式符号，会悄悄引入 SDK 无法验证的语义，因此 0.1 **不做**统一、底层映射或特殊过滤。若目录包含代币化股票对，它只是该交易所的原生现货符号，承担该所的交易时段、点差与发行方风险。

## 验证

CI 会执行，贡献者也可在本地复现：

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
cargo +1.85.0 check --workspace --all-features --all-targets   # MSRV
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

`features` CI job 还会构建并测试隔离 feature 组合。确定性覆盖包括逐所 parser fixture、目录/原生映射校验、能力预检，以及全会话 WebSocket double（订阅、ack/control、数据、恢复、干净关闭）。Fixture 来源与实盘验证记录见
[`sample_data/SOURCES.md`](sample_data/SOURCES.md) 与各所协议文档。CI 不运行真实网络测试，能力表打勾并不等于长稳验收。

## 工作区

| Crate | 用途 |
| --- | --- |
| `cryptofeed-core` | `ExchangeId`、`Channel`、`Symbol`、`Side`、错误类型 |
| `cryptofeed-ticker` … `cryptofeed-markprice` | 各数据类别的归一化模型与 handler trait |
| `cryptofeed-rs` | 运行时：适配器、传输、路由、就绪、录制、sink |

## 文档

- [文档索引](docs/README.md)
- [Parity 清单](PARITY.md)
- [交易所协议基线](docs/exchange-protocol-baseline.md)
- [标的命名](docs/instrument-naming.md)
- [就绪状态](docs/readiness.md) · [事件流](docs/event-streams.md) ·
  [Handler](docs/handlers.md) · [运行时选项](docs/runtime-options.md)

## License

MIT，见 [LICENSE](LICENSE)。
