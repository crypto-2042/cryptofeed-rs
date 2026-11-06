//! Live-only HTX spot trade soak for the close/heartbeat contract.
//!
//! Runs 13 USDT markets for 90 minutes with no handlers so ingestion and the
//! pong path are exercised exactly as in the runtime, and prints anchored
//! event/freshness counters once per minute. Verify with:
//! `cargo run -p cryptofeed-rs --example htx_spot_soak`.

use cryptofeed_rs::prelude::*;
use std::time::Duration;
use tokio::time::{Instant, interval};

const MARKETS: &[(&str, &str)] = &[
    ("BTC-USDT", "BTCUSDT"),
    ("ETH-USDT", "ETHUSDT"),
    ("SOL-USDT", "SOLUSDT"),
    ("PEPE-USDT", "PEPEUSDT"),
    ("DOGE-USDT", "DOGEUSDT"),
    ("XRP-USDT", "XRPUSDT"),
    ("ADA-USDT", "ADAUSDT"),
    ("BNB-USDT", "BNBUSDT"),
    ("LINK-USDT", "LINKUSDT"),
    ("LTC-USDT", "LTCUSDT"),
    ("AVAX-USDT", "AVAXUSDT"),
    ("SUI-USDT", "SUIUSDT"),
    ("TRUMP-USDT", "TRUMPUSDT"),
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let mut anchored = handler.subscribe_anchored_with_diagnostics();
    let mut status = handler.subscribe_status();
    let mut feed = Htx::new().trade();
    for (symbol, native) in MARKETS {
        feed = feed.symbol(symbol).exchange_symbol(native);
    }
    handler.add_feed(feed.build());
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(handler.run_with_shutdown(shutdown));
    let start = Instant::now();
    let mut tick = interval(Duration::from_secs(60));
    let mut total = 0u64;
    loop {
        tokio::select! {
            event = anchored.recv() => match event {
                Ok(_) => total += 1,
                Err(error) => {
                    println!("anchored stream ended: {error}");
                    break;
                }
            },
            state = status.recv() => {
                println!("status {state:?}");
            }
            _ = tick.tick() => {
                let diagnostics = anchored.diagnostics();
                println!(
                    "elapsed={}s events={} received={} lagged={} pending={} closed={}",
                    start.elapsed().as_secs(),
                    total,
                    diagnostics.received,
                    diagnostics.lagged,
                    diagnostics.pending,
                    diagnostics.closed
                );
                if start.elapsed() >= Duration::from_secs(5_400) {
                    break;
                }
            }
        }
    }
    let _ = stop.send(true);
    let _ = task.await;
    Ok(())
}
