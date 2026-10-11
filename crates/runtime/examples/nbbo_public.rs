use cryptofeed_rs::prelude::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let sources = [
        (ExchangeId::Binance, "BTCUSDT"),
        (ExchangeId::Okx, "BTC-USDT"),
    ]
    .map(|(exchange, native)| {
        let id = handler.add_feed_with_id(
            ExchangeFeedBuilder::new(exchange)
                .l1_book()
                .symbol("BTC-USDT")
                .exchange_symbol(native)
                .build(),
        );
        NbboSource {
            identity: FeedIdentity { id, generation: 1 },
            exchange,
            quantity_unit: NbboQuantityUnit::Base,
        }
    });
    let mut receiver = handler.subscribe_identified();
    let control = handler.control_handle();
    let mut nbbo = Nbbo::new(
        Symbol::spot("BTC", "USDT"),
        sources.to_vec(),
        Duration::from_secs(5),
    )?;
    let origin = tokio::time::Instant::now();
    let runtime = tokio::spawn(handler.run());
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let mut seen = [false;2];
        let mut changes = 0;
        let mut clock = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = clock.tick() => { let _ = nbbo.advance(origin.elapsed())?; }
                event = receiver.recv() => {
                    let envelope = event?;
                    let FeedEvent::L1Book(book) = envelope.event else {continue};
                    let now = origin.elapsed();
                    // Include queue/processing age rather than treating dequeue as freshness.
                    // Received timestamps are local wall-clock receipts; clock jumps fail here.
                    let age = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64() - book.received_ts;
                    let age = Duration::try_from_secs_f64(age)?;
                    let observed = now.checked_sub(age).ok_or("receipt predates monotonic origin")?;
                    if let Some(index) = sources.iter().position(|source|source.identity == envelope.identity) {
                        seen[index] = true;
                    }
                    if nbbo.update(envelope.identity,&book,observed,now)?.is_some() {changes+=1;}
                    if seen.iter().all(|value|*value) && changes>=20 {
                        let now=origin.elapsed();
                        let mut withdrawn=false;
                        for source in sources {
                            if let Some(change)=nbbo.set_available(source.identity,false,now)? {
                                withdrawn=change.quote.is_none();
                            }
                        }
                        if !withdrawn {return Err("missing final withdrawal".into());}
                        // No usable source remains after explicit shutdown invalidation.
                        if nbbo.advance(now)?.is_some() {return Err("duplicate withdrawal".into());}
                        println!("NBBO spot sources=2 changes={changes} both_observed=true sources_suspended=true final_withdrawal=true");
                        return Ok::<(),Box<dyn std::error::Error>>(());
                    }
                }
            }
        }
    }).await;
    let shutdown = control.shutdown().await;
    let finished = runtime.await?;
    shutdown?;
    finished?;
    result??;
    Ok(())
}
