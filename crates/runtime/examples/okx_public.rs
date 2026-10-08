use std::sync::Arc;

use async_trait::async_trait;
use cryptofeed_rs::prelude::*;

struct PrintHandler;

#[async_trait]
impl CandleHandler for PrintHandler {
    async fn on_candle(&self, candle: Candle) {
        println!("candle {candle:?}");
    }
}

#[async_trait]
impl TickerHandler for PrintHandler {
    async fn on_ticker(&self, ticker: Ticker) {
        println!("ticker {ticker:?}");
    }
}

#[async_trait]
impl TradeHandler for PrintHandler {
    async fn on_trade(&self, trade: Trade) {
        println!("trade {trade:?}");
    }
}

#[async_trait]
impl OrderBookHandler for PrintHandler {
    async fn on_l2_book(&self, book: L2Book) {
        println!("l2_book {book:?}");
    }
}

#[tokio::main]
async fn main() -> cryptofeed_core::error::Result<()> {
    let handler = Arc::new(PrintHandler);
    let mut feed_handler = FeedHandler::new();

    feed_handler.add_feed(
        Okx::new()
            .candles()
            .ticker()
            .trade()
            .l2_book()
            .candle_handler(handler.clone())
            .ticker_handler(handler.clone())
            .trade_handler(handler.clone())
            .orderbook_handler(handler)
            .symbol("BTC-USDT")
            .build(),
    );

    feed_handler.run().await
}
