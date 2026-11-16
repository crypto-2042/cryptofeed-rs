//! Shared machinery used by every venue module.
//!
//! This module holds the generic (non-venue-specific) helpers: normalized
//! channel dispatch, book-state application, connection book resets, and the
//! join/drain helper shared by multi-connection feeds.

use crate::exchange::ExchangeFeed;
use crate::runtime::dispatch;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::Channel,
};
use serde_json::Value;
use tokio::task::JoinSet;

#[cfg(feature = "orderbook")]
use crate::runtime::book_depth;
#[cfg(feature = "orderbook")]
use cryptofeed_orderbook::L2Book;

#[cfg(feature = "orderbook")]
pub(super) const MAX_BUFFERED_DELTAS_PER_SYMBOL: usize = 512;

pub(super) async fn collect_connection_results(
    label: &str,
    mut tasks: JoinSet<Result<()>>,
) -> Result<()> {
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(feed = label, error = %error, "connection plan terminated");
                tasks.abort_all();
                return Err(Error::Transport(format!("{label}: {error}")));
            }
            Err(error) => {
                tasks.abort_all();
                return Err(Error::Transport(format!("{label} task: {error}")));
            }
        }
    }
    Ok(())
}

pub(super) fn reset_connection_books<'a>(
    feed: &ExchangeFeed,
    book_route: bool,
    candidates: impl IntoIterator<Item = &'a cryptofeed_core::symbol::Symbol>,
) {
    if !book_route {
        return;
    }
    let owned: Vec<_> = candidates
        .into_iter()
        .filter(|symbol| feed.subscribes(Channel::L2Book, symbol))
        .cloned()
        .collect();
    feed.clear_connection_books(&owned);
}

#[cfg(feature = "orderbook")]
pub(super) fn apply_orderbook_state(
    feed: &ExchangeFeed,
    book: &cryptofeed_orderbook::L2Book,
) -> Result<L2Book> {
    use cryptofeed_orderbook::L2Book;

    let symbol = match book {
        L2Book::Snapshot(snapshot) => snapshot.symbol.as_str().to_owned(),
        L2Book::Delta(delta) => delta.symbol.as_str().to_owned(),
    };

    let mut states = feed.orderbook_states.lock().expect("orderbook state lock");
    let state = states.entry(symbol.clone()).or_insert_with(|| {
        cryptofeed_orderbook::L2BookState::new(match book {
            L2Book::Snapshot(snapshot) => snapshot.symbol.clone(),
            L2Book::Delta(delta) => delta.symbol.clone(),
        })
    });
    state.apply(book.clone());
    let result = if feed.l2_book_cross_check && state.is_crossed() {
        Err(Error::MalformedData(format!(
            "crossed L2 book: {}",
            book.symbol().as_str()
        )))
    } else if let Some(maximum) = feed.l2_book_max_depth {
        let exchange = match book {
            L2Book::Snapshot(value) => value.exchange,
            L2Book::Delta(value) => value.exchange,
        };
        let current = state.to_snapshot_with_depth(
            exchange,
            book.exchange_ts(),
            book.received_ts(),
            maximum.get(),
        );
        let mut projections = feed.book_projections.lock().expect("book projection lock");
        let result = book_depth::project(book, &current, projections.get(&symbol));
        if result.is_ok() {
            projections.insert(symbol, current);
        }
        result
    } else {
        Ok(book.clone())
    };
    drop(states);
    if result.is_err() {
        feed.clear_connection_books(std::slice::from_ref(book.symbol()));
    }
    result
}

#[cfg(feature = "candles")]
pub(super) async fn dispatch_candle(feed: &ExchangeFeed, candle: cryptofeed_candles::Candle) {
    if !subscribed_to(feed, Channel::Candles, &candle.symbol) {
        return;
    }
    use crate::exchange::CandlePolicy;
    let deliver = match feed.candle_policy {
        CandlePolicy::All => true,
        CandlePolicy::ClosedOnly => candle.closed == Some(true),
        CandlePolicy::ClosedOrUnknown => candle.closed != Some(false),
    };
    if !deliver {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Candle(candle.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_candle(candle);
        return;
    }
    dispatch::deliver_candle(
        feed.candle_handler.as_ref(),
        &feed.additional_candle_handlers,
        feed.runtime_options.callback_deadline(),
        candle,
    )
    .await;
}

#[cfg(feature = "funding")]
pub(super) async fn dispatch_funding(feed: &ExchangeFeed, funding: cryptofeed_funding::Funding) {
    if !subscribed_to(feed, Channel::Funding, &funding.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Funding(funding.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_funding(funding);
        return;
    }
    dispatch::deliver_funding(
        feed.funding_handler.as_ref(),
        &feed.additional_funding_handlers,
        feed.runtime_options.callback_deadline(),
        funding,
    )
    .await;
}

#[cfg(feature = "liquidations")]
pub(super) async fn dispatch_liquidation(
    feed: &ExchangeFeed,
    liquidation: cryptofeed_liquidations::Liquidation,
) {
    if !subscribed_to(feed, Channel::Liquidations, &liquidation.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Liquidation(liquidation.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_liquidation(liquidation);
        return;
    }
    dispatch::deliver_liquidation(
        feed.liquidation_handler.as_ref(),
        &feed.additional_liquidation_handlers,
        feed.runtime_options.callback_deadline(),
        liquidation,
    )
    .await;
}

#[cfg(feature = "markprice")]
pub(super) async fn dispatch_mark_price(
    feed: &ExchangeFeed,
    mark_price: cryptofeed_markprice::MarkPrice,
) {
    if !subscribed_to(feed, Channel::MarkPrice, &mark_price.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::MarkPrice(mark_price.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_mark_price(mark_price);
        return;
    }
    dispatch::deliver_mark_price(
        feed.mark_price_handler.as_ref(),
        &feed.additional_mark_price_handlers,
        feed.runtime_options.callback_deadline(),
        mark_price,
    )
    .await;
}

#[cfg(feature = "openinterest")]
pub(super) async fn dispatch_open_interest(
    feed: &ExchangeFeed,
    open_interest: cryptofeed_openinterest::OpenInterest,
) {
    if !subscribed_to(feed, Channel::OpenInterest, &open_interest.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::OpenInterest(open_interest.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_open_interest(open_interest);
        return;
    }
    dispatch::deliver_open_interest(
        feed.open_interest_handler.as_ref(),
        &feed.additional_open_interest_handlers,
        feed.runtime_options.callback_deadline(),
        open_interest,
    )
    .await;
}

#[cfg(feature = "index")]
pub(super) async fn dispatch_index_price(
    feed: &ExchangeFeed,
    index_price: cryptofeed_index::IndexPrice,
) {
    if !subscribed_to(feed, Channel::Index, &index_price.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::IndexPrice(index_price.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_index_price(index_price);
        return;
    }
    dispatch::deliver_index_price(
        feed.index_price_handler.as_ref(),
        &feed.additional_index_price_handlers,
        feed.runtime_options.callback_deadline(),
        index_price,
    )
    .await;
}

#[cfg(feature = "ticker")]
pub(super) async fn dispatch_ticker(feed: &ExchangeFeed, ticker: cryptofeed_ticker::Ticker) {
    if !subscribed_to(feed, Channel::Ticker, &ticker.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Ticker(ticker.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_ticker(ticker);
        return;
    }
    dispatch::deliver_ticker(
        feed.ticker_handler.as_ref(),
        &feed.additional_ticker_handlers,
        feed.runtime_options.callback_deadline(),
        ticker,
    )
    .await;
}

#[cfg(feature = "trade")]
pub(super) async fn dispatch_trade(feed: &ExchangeFeed, trade: cryptofeed_trade::Trade) {
    if !subscribed_to(feed, Channel::Trade, &trade.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::Trade(trade.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_trade(trade);
        return;
    }
    dispatch::deliver_trade(
        feed.trade_handler.as_ref(),
        &feed.additional_trade_handlers,
        feed.runtime_options.callback_deadline(),
        trade,
    )
    .await;
}

#[cfg(feature = "orderbook")]
pub(super) async fn dispatch_l2_book(feed: &ExchangeFeed, book: L2Book) -> Result<()> {
    if !subscribed_to(feed, Channel::L2Book, book.symbol()) {
        return Ok(());
    }
    let book = apply_orderbook_state(feed, &book)?;
    if let (Some(monitor), Some(tracker), Some(epoch)) = (
        &feed.monitor,
        &feed.connection_tracker,
        feed.connection_epoch,
    ) {
        if let Some(books) = &monitor.books {
            books.apply(monitor.identity, tracker.id, epoch, &book);
        }
    }
    if matches!(&book, L2Book::Snapshot(_)) {
        feed.book_synchronized(book.symbol().as_str());
    }
    feed.publish_event(crate::feed::FeedEvent::L2Book(book.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_l2_book(book);
        return Ok(());
    }
    dispatch::deliver_l2_book(
        feed.orderbook_handler.as_ref(),
        &feed.additional_orderbook_handlers,
        feed.runtime_options.callback_deadline(),
        book,
    )
    .await;
    Ok(())
}

#[cfg(feature = "orderbook")]
pub(super) async fn dispatch_l1_book(feed: &ExchangeFeed, book: cryptofeed_orderbook::L1Book) {
    if !subscribed_to(feed, Channel::L1Book, &book.symbol) {
        return;
    }
    feed.publish_event(crate::feed::FeedEvent::L1Book(book.clone()));
    if let Some(dispatcher) = &feed.handler_dispatcher {
        dispatcher.enqueue_l1_book(book);
        return;
    }
    dispatch::deliver_l1_book(
        feed.orderbook_handler.as_ref(),
        &feed.additional_orderbook_handlers,
        feed.runtime_options.callback_deadline(),
        book,
    )
    .await;
}

pub(super) fn subscribed_to(
    feed: &ExchangeFeed,
    channel: Channel,
    symbol: &cryptofeed_core::symbol::Symbol,
) -> bool {
    feed.subscribes(channel, symbol)
}

pub(super) fn data_is_non_empty(message: &Value) -> bool {
    match message.get("data") {
        Some(Value::Array(rows)) => !rows.is_empty(),
        Some(Value::Object(fields)) => !fields.is_empty(),
        _ => false,
    }
}
