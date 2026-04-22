#[cfg(feature = "candles")]
pub use cryptofeed_candles::CandleHandler;

#[cfg(feature = "funding")]
pub use cryptofeed_funding::FundingHandler;

#[cfg(feature = "liquidations")]
pub use cryptofeed_liquidations::LiquidationHandler;

#[cfg(feature = "ticker")]
pub use cryptofeed_ticker::TickerHandler;

#[cfg(feature = "trade")]
pub use cryptofeed_trade::TradeHandler;

#[cfg(feature = "orderbook")]
pub use cryptofeed_orderbook::OrderBookHandler;
