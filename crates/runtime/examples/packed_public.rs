use cryptofeed_rs::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let books = handler.l2_book_handle();
    let control = handler.control_handle();
    let (_stop, receiver) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(receiver));
    let result = async {
        let mut identities = Vec::new();
        for (exchange, native) in [
            (ExchangeId::Binance, ["ETHUSDT", "BTCUSDT"]),
            (ExchangeId::Bitget, ["ETHUSDT", "BTCUSDT"]),
            (ExchangeId::Bybit, ["ETHUSDT", "BTCUSDT"]),
            (ExchangeId::Okx, ["ETH-USDT", "BTC-USDT"]),
            (ExchangeId::Gateio, ["ETH_USDT", "BTC_USDT"]),
        ] {
            identities.push(control.add_feed(ExchangeFeedBuilder::new(exchange)
                .subscription(Channel::Trade, ["ETH-USDT", "BTC-USDT"])
                .subscription(Channel::L2Book, ["BTC-USDT"])
                .exchange_symbol(native[0]).exchange_symbol(native[1]).build()).await?);
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let mut states = Vec::new();
            for identity in &identities { states.push(control.state(identity.id).await?); }
            let ready = states.iter().all(|state| state.is_ready() && state.observed_pairs == 3 && state.connections_expected == 1 && state.books_expected == 1);
            if ready || tokio::time::Instant::now() >= deadline {
                for state in &states {
                    let btc = books.recover(state.identity, &Symbol::spot("BTC", "USDT")).snapshot;
                    let eth = books.recover(state.identity, &Symbol::spot("ETH", "USDT")).snapshot;
                    println!("  recovery BTC={} ETH={}", btc.is_some(), eth.is_some());
                    if ready && (btc.is_none() || eth.is_some()) { return Err("incorrect sparse recovery scope".into()); }
                    println!("{:?}: state={:?} connections={}/{} subscriptions={}/{} books={}/{} events={} pairs={}",
                        state.exchange, state.state,
                        state.connections.iter().filter(|connection| connection.connected).count(), state.connections_expected,
                        state.connections.iter().map(|connection| connection.subscriptions_confirmed).sum::<usize>(),
                        state.connections.iter().map(|connection| connection.subscriptions_expected).sum::<usize>(),
                        state.books_synchronized, state.books_expected, state.observed_events, state.observed_pairs);
                    for connection in &state.connections { println!("  connection {} epoch {} error={:?}", connection.id, connection.epoch, connection.last_error); }
                }
                if !ready { return Err::<(), Box<dyn std::error::Error>>("not all public feeds became ready in the observation window".into()); }
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }.await;
    let stopped = control.shutdown().await;
    let finished = running.await?;
    result?;
    stopped?;
    finished?;
    Ok(())
}
