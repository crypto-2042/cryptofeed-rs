use cryptofeed_rs::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let books = handler.l2_book_handle();
    let control = handler.control_handle();
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown));
    let result = async {
        let identity = control.add_feed(Okx::new().l2_book().symbol("BTC-USDT").build()).await?;
        let symbol = Symbol::spot("BTC", "USDT");
        let mut recovery = books.recover(identity, &symbol);
        let mut local = L2BookState::new(symbol.clone());
        let mut anchor = recovery.snapshot.as_ref().map(|snapshot| snapshot.anchor);
        if let Some(snapshot) = recovery.snapshot.take() { local.apply(L2Book::Snapshot(snapshot.book)); }
        let observed = tokio::time::timeout(Duration::from_secs(35), async {
            let mut updates = 0;
            loop {
                match recovery.updates.recv().await {
                    Ok(update) => {
                        let Some(book) = update.book else { return Err("book invalidated during observation".into()); };
                        if matches!(book, L2Book::Delta(_)) && anchor.is_none_or(|old| old.identity != update.anchor.identity || old.connection != update.anchor.connection || old.epoch != update.anchor.epoch || old.revision + 1 != update.anchor.revision) {
                            return Err("delta anchor discontinuity".into());
                        }
                        local.apply(book);
                        anchor = Some(update.anchor);
                        updates += 1;
                        if updates >= 3 { return Ok::<_, Box<dyn std::error::Error>>(updates); }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        recovery = books.recover(identity, &symbol);
                        anchor = recovery.snapshot.as_ref().map(|snapshot| snapshot.anchor);
                        local = L2BookState::new(symbol.clone());
                        if let Some(snapshot) = recovery.snapshot.take() { local.apply(L2Book::Snapshot(snapshot.book)); }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }).await;
        // Keep cleanup separate from observation success/failure.
        let latest = books.recover(identity, &symbol).snapshot;
        control.remove_feed(identity.id).await?;
        if books.recover(identity, &symbol).snapshot.is_some() { return Err("retired book is still available".into()); }
        let updates = observed??;
        println!("updates={updates} last_anchor={anchor:?} bids={} asks={} recovery_anchor={:?} retired=true", local.bids().len(), local.asks().len(), latest.as_ref().map(|snapshot| snapshot.anchor));
        Ok::<(), Box<dyn std::error::Error>>(())
    }.await;
    let stopped = control.shutdown().await;
    let finished = running.await?;
    result?;
    stopped?;
    finished?;
    Ok(())
}
