use cryptofeed_rs::{feed::FeedEvent, prelude::*};
async fn observe(exchange: ExchangeId) -> Result<(), Box<dyn std::error::Error>> {
    let (capture, input) = raw_capture_channel(256, 8 * 1024 * 1024)?;
    let mut handler = FeedHandler::new();
    handler.add_feed(
        ExchangeFeedBuilder::new(exchange)
            .l2_book()
            .symbol("BTC-USDT")
            .raw_capture(capture)
            .build(),
    );
    let mut events = handler.subscribe();
    let control = handler.control_handle();
    let live = tokio::spawn(async move {
        let mut books = Vec::new();
        loop {
            match events.recv().await {
                Ok(FeedEvent::L2Book(book)) => {
                    if books.len() >= 10_000 {
                        return Err(cryptofeed_core::error::Error::Protocol(
                            "live book ledger limit".into(),
                        ));
                    }
                    books.push(book);
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    return Err(cryptofeed_core::error::Error::Protocol(
                        "live book ledger lagged".into(),
                    ));
                }
            }
        }
        Ok(books)
    });
    let runtime = tokio::spawn(async move { handler.run().await });
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let mut bytes = Vec::new();
    let limits = RawRecordingLimits::new(
        RecordingLimits::new(200, 32 * 1024 * 1024, 8 * 1024 * 1024)?,
        16,
    )?;
    let captured = tokio::time::timeout(
        std::time::Duration::from_secs(90),
        record_raw_stream(&mut bytes, input, limits, stop),
    )
    .await;
    let shutdown = control.shutdown().await;
    let runtime_result = runtime.await?;
    shutdown?;
    runtime_result?;
    let live = live.await??;
    let capture = captured??;
    let mut inspect = RawRecordingReader::new(bytes.as_slice(), limits);
    let mut snapshots = 0;
    let mut processing = 0;
    while let Some(record) = inspect.next_observation().await? {
        match record.kind {
            RawObservationKind::HttpSnapshot { .. } => snapshots += 1,
            RawObservationKind::Processing { .. } => processing += 1,
            _ => {}
        }
    }
    let mut reader = RawRecordingReader::new(bytes.as_slice(), limits);
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let mut replayed = 0;
    let summary = reader
        .replay(
            RawReplayOptions::default().output_limits(8192, 100_000)?,
            stop,
            |item| {
                if let RawReplayItem::Market(RawReplayEvent {
                    event: FeedEvent::L2Book(book),
                    ..
                }) = item
                {
                    if !live.contains(&book) {
                        return std::future::ready(Err(cryptofeed_core::error::Error::Protocol(
                            "offline L2 differs from live model/clock".into(),
                        )));
                    }
                    replayed += 1;
                }
                std::future::ready(Ok(()))
            },
        )
        .await?
        .ok_or("L2 replay stopped")?;
    if snapshots == 0 || replayed == 0 || capture != summary.recording || summary.models != replayed
    {
        return Err("missing HTTP snapshot/native L2 replay evidence".into());
    }
    println!(
        "{exchange:?}/Spot L2: observations={}, HTTP snapshots={snapshots}, processing={processing}, models={replayed}; all fields/clocks exactly match live; offline only",
        capture.events
    );
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut failed = Vec::new();
    for exchange in [ExchangeId::Binance, ExchangeId::Gateio] {
        if let Err(error) = observe(exchange).await {
            println!("{exchange:?} failed={error}");
            failed.push(format!("{exchange:?}"));
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("raw HTTP/L2 replay failures: {}", failed.join(", ")).into())
    }
}
