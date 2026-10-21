//! Offline independent-consumer smoke for the packaged candidate.
//!
//! Feature sets are selected by the consumer manifests, not by this checkout:
//! - `trade-model`: normalized Trade model, Decimal/Serde roundtrip, handler trait.
//! - `recording`:   opt-in recording writer/reader roundtrip.
//! Without either feature the documented `FeedHandler` -> builder -> `add_feed`
//! entrypoint must still compile and run.
use cryptofeed_rs::prelude::*;

#[cfg(feature = "trade-model")]
struct CountingTrades {
    seen: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "trade-model")]
#[async_trait::async_trait]
impl TradeHandler for CountingTrades {
    async fn on_trade(&self, _trade: Trade) {
        self.seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Documented entrypoint: FeedHandler -> add_feed(builder.build()) -> run().
    let mut handler = FeedHandler::new();
    let _control = handler.control_handle();

    #[cfg(feature = "trade-model")]
    let counter = std::sync::Arc::new(CountingTrades {
        seen: std::sync::atomic::AtomicUsize::new(0),
    });

    #[cfg(feature = "trade-model")]
    let feed = {
        use std::sync::Arc;
        Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .trade_handler(counter.clone() as Arc<dyn TradeHandler>)
            .build()
    };
    #[cfg(not(feature = "trade-model"))]
    let feed = Binance::new().build();

    handler.add_feed(feed);
    assert_eq!(handler.feed_count(), 1, "add_feed must register one feed");
    println!("consumer: entrypoint/control/builder ok (feed_count=1)");

    #[cfg(feature = "trade-model")]
    {
        use std::str::FromStr;
        let trade = Trade {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            side: Side::Buy,
            amount: rust_decimal::Decimal::from_str("0.2501")?,
            price: rust_decimal::Decimal::from_str("64001.20")?,
            exchange_ts: 1_710_000_001.0,
            received_ts: 1_710_000_001.5,
            id: Some("consumer-1".to_owned()),
            implied_volatility: None,
        };
        let json = serde_json::to_string(&trade)?;
        let back: Trade = serde_json::from_str(&json)?;
        assert_eq!(back.price, rust_decimal::Decimal::from_str("64001.20")?);
        assert_eq!(back.amount, rust_decimal::Decimal::from_str("0.2501")?);
        assert_eq!(back.symbol.as_str(), "BTC-USDT");
        assert_eq!(back.exchange, ExchangeId::Binance);
        println!(
            "consumer: trade decimal/serde ok ({}, {})",
            back.price, back.amount
        );
    }

    #[cfg(feature = "recording")]
    {
        use tokio::io::BufReader;
        let path = std::env::temp_dir().join(format!(
            "cfrs-consumer-recording-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;
        let writer = RecordingWriter::new(file, RecordingLimits::default()).await?;
        let summary = writer.finish(RecordingEnd::Complete).await?;
        assert_eq!(summary.events, 0);

        let file = tokio::fs::File::open(&path).await?;
        let mut reader = RecordingReader::new(BufReader::new(file), RecordingLimits::default());
        assert!(reader.next_event().await?.is_none());
        std::fs::remove_file(&path)?;
        println!("consumer: empty recording writer/reader roundtrip ok");
    }

    println!("consumer: ALL ASSERTIONS PASSED");
    Ok(())
}
