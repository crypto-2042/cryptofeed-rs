use cryptofeed_rs::{feed::FeedEvent, prelude::*};
fn observe(
    frame: RawObservation,
    sequences: &mut u64,
    receipts: &mut std::collections::HashSet<u64>,
) -> Result<bool, Box<dyn std::error::Error>> {
    *sequences += 1;
    if frame.sequence != *sequences {
        return Err("raw observation sequence mismatch".into());
    }
    if frame.session.feed.exchange != ExchangeId::Okx || frame.session.feed.identity.is_none() {
        return Err("missing raw source identity".into());
    }
    if let RawObservationKind::Received(RawPayload::Json { value, .. }) = frame.kind {
        if value["arg"]["channel"] == "trades"
            && value["data"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
        {
            receipts.insert(frame.observed_ts.to_bits());
            return Ok(true);
        }
    }
    Ok(false)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (capture, mut receiver) = raw_capture_channel(64, 1024 * 1024)?;
    let mut handler = FeedHandler::new();
    handler.add_feed(
        Okx::new()
            .trade()
            .symbol("BTC-USDT")
            .raw_capture(capture.clone())
            .build(),
    );
    let mut normalized = handler.subscribe();
    let control = handler.control_handle();
    let runtime = tokio::spawn(async move { handler.run().await });
    let mut sequences = 0;
    let mut receipts = std::collections::HashSet::new();
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let mut packets = 0;
        while packets < 20 {
            let frame = receiver.recv().await?.ok_or("raw stream ended early")?;
            if observe(frame, &mut sequences, &mut receipts)? {
                packets += 1;
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(packets)
    })
    .await;
    let shutdown = control.shutdown().await;
    let runtime_result = runtime.await?;
    shutdown?;
    runtime_result?;
    let packets = result??;
    drop(capture);
    while let Some(frame) = receiver.recv().await? {
        observe(frame, &mut sequences, &mut receipts)?;
    }
    let mut count = 0;
    while let Ok(event) = normalized.try_recv() {
        if let FeedEvent::Trade(trade) = event {
            if !receipts.contains(&trade.received_ts.to_bits()) {
                return Err("raw/model receipt clock mismatch".into());
            }
            count += 1;
        }
    }
    if count < packets {
        return Err("not enough normalized trades".into());
    }
    println!(
        "raw market packets={packets}; total observations={sequences}; normalized trades={count}; exact receipt clocks verified"
    );
    Ok(())
}
