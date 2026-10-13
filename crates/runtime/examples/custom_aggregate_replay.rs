//! Caller-defined per-source buy/sell counts over a normalized trade recording.
use cryptofeed_core::error::{Error, Result};
use cryptofeed_rs::prelude::*;
use std::{collections::HashMap, time::Duration};
use tokio::io::BufReader;

type Series = (FeedIdentity, ExchangeId, Symbol);
#[derive(Default)]
struct Counts {
    buy: u64,
    sell: u64,
}
type State = HashMap<Series, Counts>;
fn accumulate(state: &mut State, input: &FeedEnvelope) -> Result<()> {
    let FeedEvent::Trade(trade) = &input.event else {
        return Err(Error::InvalidConfiguration(
            "trade-only custom calculation".into(),
        ));
    };
    let key = (input.identity, trade.exchange, trade.symbol.clone());
    if state.len() >= 128 && !state.contains_key(&key) {
        return Err(Error::InvalidConfiguration(
            "example series limit reached".into(),
        ));
    }
    let counts = state.entry(key).or_default();
    match trade.side {
        Side::Buy => counts.buy += 1,
        Side::Sell => counts.sell += 1,
    }
    Ok(())
}
fn verify(window: AggregateWindow<State>) -> Result<(u64, bool)> {
    let count = window
        .state
        .values()
        .map(|counts| counts.buy + counts.sell)
        .sum();
    if window.events != count {
        return Err(Error::Protocol("custom state/window count mismatch".into()));
    }
    Ok((count, window.closed))
}
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: custom_aggregate_replay TRADE_RECORDING [WINDOW_SECONDS]")?;
    let seconds: u64 = std::env::args().nth(2).unwrap_or("60".into()).parse()?;
    let file = tokio::fs::File::open(path).await?;
    let mut reader = RecordingReader::new(BufReader::new(file), RecordingLimits::default());
    let mut aggregate = CustomAggregate::new(
        Duration::from_secs(seconds),
        10_000,
        || Ok(State::new()),
        accumulate,
    )?;
    let (mut input, mut output, mut closed, mut partial) = (0, 0, 0, 0);
    while let Some(row) = reader.next_event().await? {
        let now = Duration::from_nanos(row.elapsed_ns);
        let event = FeedEnvelope {
            identity: row.identity,
            event: row.event,
        };
        input += 1;
        if let Some(window) = aggregate.push(&event, now)? {
            let (count, is_closed) = verify(window)?;
            output += count;
            if is_closed {
                closed += 1;
            } else {
                partial += 1;
            }
        }
    }
    if let Some(window) = aggregate.finish()? {
        let (count, is_closed) = verify(window)?;
        output += count;
        if is_closed {
            closed += 1;
        } else {
            partial += 1;
        }
    }
    if input != output {
        return Err("custom input/output count mismatch".into());
    }
    println!(
        "offline custom events={input} closed={closed} partial={partial} per-source buy/sell counts verified"
    );
    Ok(())
}
