//! Manual release smoke across spot and derivatives on the five active exchanges.
//! Run it, let it collect events for a short window, then press Ctrl-C.

use std::collections::BTreeMap;

use cryptofeed_rs::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let mut events = handler.subscribe();
    let mut statuses = handler.subscribe_status();

    handler.add_feed(
        Binance::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    handler.add_feed(
        Bitget::new()
            .l1_book()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .liquidations()
            .funding()
            .open_interest()
            .index()
            .mark_price()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    handler.add_feed(
        Bybit::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .funding()
            .liquidations()
            .open_interest()
            .index()
            .mark_price()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    handler.add_feed(
        Okx::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .funding()
            .liquidations()
            .open_interest()
            .mark_price()
            .index()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC-USDT-SWAP")
            .build(),
    );
    handler.add_feed(
        Gateio::new()
            .ticker()
            .trade()
            .l2_book()
            .l1_book()
            .candles()
            .funding()
            .open_interest()
            .index()
            .mark_price()
            .liquidations()
            .symbol("BTC-USDT-PERP")
            .exchange_symbol("BTC_USDT")
            .build(),
    );
    handler.add_feed(
        Bitget::new()
            .l1_book()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    handler.add_feed(
        Bybit::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .build(),
    );
    handler.add_feed(
        Okx::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT")
            .build(),
    );
    handler.add_feed(
        Gateio::new()
            .ticker()
            .trade()
            .l2_book()
            .candles()
            .symbol("BTC-USDT")
            .exchange_symbol("BTC_USDT")
            .build(),
    );
    handler.add_feed(
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

    let consumer = tokio::spawn(async move {
        let mut counts = BTreeMap::<String, u64>::new();
        let mut status_open = true;
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    Ok(event) => {
                        let key = format!(
                            "{:?}/{}/{:?}",
                            event.exchange(),
                            event.symbol(),
                            event.channel()
                        );
                        *counts.entry(key).or_default() += 1;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        eprintln!("event receiver lagged; skipped {skipped}");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
                status = statuses.recv(), if status_open => match status {
                    Ok(status) => eprintln!("feed status: {status:?}"),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        eprintln!("status receiver lagged; skipped {skipped}");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => status_open = false,
                }
            }
        }
        counts
    });

    handler.run().await?;
    for (key, count) in consumer.await? {
        println!("{key}={count}");
    }
    Ok(())
}
