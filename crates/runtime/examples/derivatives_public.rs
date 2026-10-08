//! Derivative extended-channel example: Binance USD-M perpetual funding,
//! index price, mark price, and L1 top-of-book alongside the
//! ticker/trade/L2 baseline, delivered through the event-stream consumer
//! mode (`FeedHandler::subscribe`) instead of handler-trait callbacks.
//!
//! Run: `cargo run -p cryptofeed-rs --example derivatives_public`

use cryptofeed_rs::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut feed_handler = FeedHandler::new();
    // Event-stream consumer: no handler traits required. The receiver must
    // be drained; a slow consumer lags and drops the oldest events once the
    // bounded buffer is full.
    let mut events = feed_handler.subscribe();
    let mut tickers = 0u64;

    feed_handler.add_feed(
        Binance::new()
            .ticker()
            .trade()
            .l2_book()
            .l1_book()
            .funding()
            .index()
            .mark_price()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build(),
    );

    let runtime = tokio::spawn(feed_handler.run());
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                eprintln!("event consumer lagged; skipped {skipped} events");
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        };
        match event {
            FeedEvent::Ticker(ticker) => {
                tickers += 1;
                if tickers % 100 == 0 {
                    println!("ticker {} {}", ticker.symbol.as_str(), ticker.bid);
                }
            }
            FeedEvent::Funding(funding) => {
                println!(
                    "funding {} rate={:?}",
                    funding.symbol.as_str(),
                    funding.rate
                );
            }
            FeedEvent::IndexPrice(index) => {
                println!("index {} {}", index.symbol.as_str(), index.price);
            }
            FeedEvent::MarkPrice(mark) => {
                println!("mark {} {}", mark.symbol.as_str(), mark.price);
            }
            FeedEvent::L1Book(book) => {
                println!("l1_book {} bid={}", book.symbol.as_str(), book.bid.price);
            }
            FeedEvent::L2Book(book) => {
                println!(
                    "l2_book {} levels={}",
                    book.symbol().as_str(),
                    book.bids().len()
                );
            }
            _ => {}
        }
    }
    runtime.await??;
    Ok(())
}
