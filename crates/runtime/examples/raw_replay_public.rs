use cryptofeed_rs::{feed::FeedEvent, prelude::*};
async fn observe(exchange: ExchangeId) -> Result<(), Box<dyn std::error::Error>> {
    let (capture, input) = raw_capture_channel(64, 1024 * 1024)?;
    let mut handler = FeedHandler::new();
    handler.add_feed(
        ExchangeFeedBuilder::new(exchange)
            .trade()
            .symbol("BTC-USDT")
            .raw_capture(capture)
            .build(),
    );
    let mut events = handler.subscribe();
    let control = handler.control_handle();
    let live = tokio::spawn(async move {
        let mut trades = Vec::new();
        loop {
            match events.recv().await {
                Ok(FeedEvent::Trade(trade)) => {
                    if trades.len() >= 10_000 {
                        return Err(cryptofeed_core::error::Error::Protocol(
                            "live comparison ledger limit".into(),
                        ));
                    }
                    trades.push(trade);
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    return Err(cryptofeed_core::error::Error::Protocol(
                        "live comparison ledger lagged".into(),
                    ));
                }
            }
        }
        Ok(trades)
    });
    let runtime = tokio::spawn(async move { handler.run().await });
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let mut bytes = Vec::new();
    let limits =
        RawRecordingLimits::new(RecordingLimits::new(12, 4 * 1024 * 1024, 1024 * 1024)?, 16)?;
    let captured = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        record_raw_stream(&mut bytes, input, limits, stop),
    )
    .await;
    let shutdown = control.shutdown().await;
    let runtime_result = runtime.await?;
    shutdown?;
    runtime_result?;
    let live = live.await??;
    let captured = captured??;
    let mut reader = RawRecordingReader::new(bytes.as_slice(), limits);
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let mut replayed = 0;
    let summary = reader
        .replay(RawReplayOptions::default(), stop, |item| {
            if let RawReplayItem::Market(RawReplayEvent {
                event: FeedEvent::Trade(trade),
                ..
            }) = item
            {
                if !live.contains(&trade) {
                    return std::future::ready(Err(cryptofeed_core::error::Error::Protocol(
                        "offline model differs from live normalization".into(),
                    )));
                }
                replayed += 1;
            }
            std::future::ready(Ok(()))
        })
        .await?
        .ok_or("replay stopped")?;
    if summary.recording != captured || summary.models != replayed || replayed == 0 {
        return Err("invalid native replay summary".into());
    }
    println!(
        "{exchange:?}/Spot: observations={}, models={}, all models exactly match live fields/clocks; offline parser replay verified",
        captured.events, replayed
    );
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut failures = Vec::new();
    for exchange in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ] {
        match observe(exchange).await {
            Ok(()) => {}
            Err(error) => {
                println!("{exchange:?} failed={error}");
                failures.push(format!("{exchange:?}"));
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("raw parser replay failures: {}", failures.join(", ")).into())
    }
}
