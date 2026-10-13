use cryptofeed_rs::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let control = handler.control_handle();
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown));
    let result = async {
        let discovery = DiscoveryFeed::new(
            Okx::new().trade().build(),
            InstrumentKind::Spot,
            ["BTC-USDT"],
        )?
        .interval(Duration::from_secs(60))?
        .start(control.clone())
        .await?;
        let mut changes = discovery.subscribe();
        let observed = tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let snapshot = changes.borrow().clone();
                if snapshot.refreshes >= 2 && snapshot.state == DiscoveryState::Current {
                    let feed = control.state(snapshot.identity.id).await?;
                    println!("discovery={snapshot:?} runtime={feed:?}");
                    if feed.is_ready() && feed.observed_events > 0 {
                        return Ok::<(), Box<dyn std::error::Error>>(());
                    }
                    return Err("directory refreshed but no ready data feed was observed".into());
                }
                changes.changed().await?;
            }
        })
        .await;
        // Stop polling and remove the managed feed even after observation failure.
        let stopped = discovery.stop().await?;
        control.remove_feed(stopped.identity.id).await?;
        println!(
            "stopped={stopped:?} remaining={}",
            control.feeds().await?.len()
        );
        observed??;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let stopped = control.shutdown().await;
    let finished = running.await?;
    result?;
    stopped?;
    finished?;
    Ok(())
}
