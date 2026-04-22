use std::sync::Arc;

use async_trait::async_trait;
use cryptofeed_orderbook::{L2Book, OrderBookHandler};
use cryptofeed_rs::{FeedHandler, bitget::Bitget};
use cryptofeed_ticker::{Ticker, TickerHandler};
use cryptofeed_trade::{Trade, TradeHandler};

struct PrintHandler;

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
        Bitget::new()
            .ticker()
            .trade()
            .l2_book()
            .ticker_handler(handler.clone())
            .trade_handler(handler.clone())
            .orderbook_handler(handler)
            .symbol("BTC-USDT")
            .build(),
    );

    feed_handler.run().await
}
