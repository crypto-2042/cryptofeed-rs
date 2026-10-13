use cryptofeed_rs::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("CRYPTOFEED_HTTP_PROXY")?;
    let mut transport = TransportConfig::http_proxy(&endpoint)?;
    if let Ok(username) = std::env::var("CRYPTOFEED_PROXY_USER") {
        transport =
            transport.basic_auth(&username, &std::env::var("CRYPTOFEED_PROXY_PASSWORD")?)?;
    }
    println!("transport={transport:?}");
    let catalog = MarketCatalog::refresh_with_transport(
        ExchangeId::Binance,
        InstrumentKind::Spot,
        &transport,
    )
    .await?;
    println!("catalog_symbols={}", catalog.symbols().len());
    let mut handler = FeedHandler::new();
    let books = handler.l2_book_handle();
    let control = handler.control_handle();
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown));
    let observed = async {
        let identity = control
            .add_feed(
                Binance::new()
                    .trade()
                    .l2_book()
                    .symbol("BTC-USDT")
                    .transport(transport)
                    .build(),
            )
            .await?;
        let observation = tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                let state = control.state(identity.id).await?;
                let snapshot = books
                    .recover(identity, &Symbol::spot("BTC", "USDT"))
                    .snapshot;
                if state.is_ready() && state.observed_pairs == 2 {
                    if let Some(book) = snapshot {
                        println!(
                            "ready=true events={} anchor={:?} bids={} asks={}",
                            state.observed_events,
                            book.anchor,
                            book.book.bids.len(),
                            book.book.asks.len()
                        );
                        return Ok::<(), Box<dyn std::error::Error>>(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        control.remove_feed(identity.id).await?;
        observation??;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let stopped = control.shutdown().await;
    let finished = running.await?;
    observed?;
    stopped?;
    finished?;
    Ok(())
}
