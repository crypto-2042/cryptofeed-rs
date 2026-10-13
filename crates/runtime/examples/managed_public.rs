use cryptofeed_rs::prelude::*;

async fn receive_generation(
    events: &mut tokio::sync::broadcast::Receiver<FeedEnvelope>,
    identity: FeedIdentity,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(25), async {
        loop {
            let envelope = events.recv().await?;
            if envelope.identity == identity {
                println!(
                    "feed={} generation={} {:?}",
                    identity.id.as_u64(),
                    identity.generation,
                    envelope.event
                );
                return Ok::<(), tokio::sync::broadcast::error::RecvError>(());
            }
        }
    })
    .await??;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let control = handler.control_handle();
    let mut events = handler.subscribe_identified();
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown));

    let result = async {
        let first = control
            .add_feed(
                Okx::new()
                    .trade()
                    .symbol("BTC-USDT")
                    .exchange_symbol("BTC-USDT")
                    .build(),
            )
            .await?;
        receive_generation(&mut events, first).await?;
        let second = control
            .replace_feed(
                first.id,
                Okx::new()
                    .trade()
                    .symbol("ETH-USDT")
                    .exchange_symbol("ETH-USDT")
                    .build(),
            )
            .await?;
        receive_generation(&mut events, second).await?;
        control.remove_feed(second.id).await?;
        println!("remaining feeds={}", control.feeds().await?.len());
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    // Close the runtime even if the manual service observation timed out.
    let stopped = control.shutdown().await;
    let finished = running.await?;
    result?;
    stopped?;
    finished?;
    Ok(())
}
