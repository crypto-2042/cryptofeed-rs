//! Manual smoke for the public channels completed on 2026-10-08.
//! Stops after 45 seconds and prints normalized per-product event counts.
use cryptofeed_rs::prelude::*;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let mut events = handler.subscribe();
    let mut statuses = handler.subscribe_status();
    handler.add_feed(
        Bitget::new()
            .l1_book()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    handler.add_feed(
        Bitget::new()
            .l1_book()
            .funding()
            .open_interest()
            .index()
            .mark_price()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    handler.add_feed(
        Okx::new()
            .index()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC-USDT-SWAP")
            .build(),
    );
    handler.add_feed(
        Gateio::new()
            .liquidations()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC_USDT")
            .build(),
    );
    let consumer = tokio::spawn(async move {
        let mut counts = BTreeMap::<String, u64>::new();
        let mut status_open = true;
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    Ok(event) => *counts.entry(format!("{:?}/{}/{:?}",event.exchange(),event.symbol(),event.channel())).or_default() += 1,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => eprintln!("lagged: {skipped}"),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
                status = statuses.recv(), if status_open => match status {
                    Ok(status) => eprintln!("feed status: {status:?}"),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => eprintln!("status lagged: {skipped}"),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => status_open = false,
                }
            }
        }
        counts
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(45)).await;
        let _ = shutdown_tx.send(true);
    });
    cryptofeed_rs::runtime::run_with_shutdown(handler, shutdown_rx).await?;
    let counts = consumer.await?;
    println!("normalized_channel_count={}", counts.len());
    for (key, count) in counts {
        println!("{key}={count}");
    }
    Ok(())
}
