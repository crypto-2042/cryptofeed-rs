//! Offline independent-capture replay: [--recorded] ID OFFSET_MS FILE ...
use cryptofeed_rs::prelude::*;
use std::time::Duration;
use tokio::{fs::File, io::BufReader, sync::watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args: Vec<_> = std::env::args().skip(1).collect();
    let recorded = args.first().is_some_and(|arg| arg == "--recorded");
    if recorded {
        args.remove(0);
    }
    if args.is_empty() || args.len() % 3 != 0 || args.len() / 3 > 16 {
        return Err("usage: raw_merge [--recorded] ID OFFSET_MS FILE ... (1..=16 captures)".into());
    }
    let mut inputs = Vec::new();
    for input in args.chunks_exact(3) {
        inputs.push(RawMergeInput::recording(
            &input[0],
            Duration::from_millis(input[1].parse()?),
            BufReader::new(File::open(&input[2]).await?),
            RawRecordingLimits::default(),
        )?);
    }
    let replay =
        RawReplayOptions::default().delivery(ReplayOptions::default().timing(if recorded {
            ReplayTiming::Recorded
        } else {
            ReplayTiming::Immediate
        }));
    let (stop, shutdown) = watch::channel(false);
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = stop.send(true);
        }
    });
    let result = replay_raw_merge(
        inputs,
        RawMergeOptions::default().replay(replay),
        shutdown,
        |record| {
            match record.item {
                RawReplayItem::SessionStarted(session) => println!(
                    "{} @{}ns session {} started {:?}",
                    record.capture_id, record.elapsed_ns, session.session_id, session.feed.exchange
                ),
                RawReplayItem::Market(event) => println!(
                    "{} @{}ns session {} sequence {} {:?} {}",
                    record.capture_id,
                    record.elapsed_ns,
                    event.session_id,
                    event.observation_sequence,
                    event.event.channel(),
                    event.event.symbol()
                ),
                RawReplayItem::SessionEnded { session, clean } => println!(
                    "{} @{}ns session {} ended clean={clean}",
                    record.capture_id, record.elapsed_ns, session.session_id
                ),
                _ => {}
            }
            async { Ok(()) }
        },
    )
    .await;
    signal.abort();
    match result? {
        Some(summary) => println!(
            "validated {} captures, {} observations, {} models, {} logical bytes",
            summary.recordings.len(),
            summary.observations,
            summary.models,
            summary.bytes
        ),
        None => println!("stopped; inputs were not certified to EOF"),
    }
    Ok(())
}
