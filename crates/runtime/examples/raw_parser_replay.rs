use cryptofeed_rs::prelude::*;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: raw_parser_replay RAW_WS_PATH")?;
    let file = tokio::fs::File::open(path).await?;
    let mut reader = RawRecordingReader::new(
        tokio::io::BufReader::new(file),
        RawRecordingLimits::default(),
    );
    let (_stop, stop) = tokio::sync::watch::channel(false);
    let mut active = std::collections::HashSet::new();
    let mut models = 0;
    let summary = reader
        .replay(RawReplayOptions::default(), stop, |item| {
            match item {
                RawReplayItem::SessionStarted(info) => {
                    active.insert(info.session_id);
                }
                RawReplayItem::SessionEnded { session, .. } => {
                    active.remove(&session.session_id);
                }
                RawReplayItem::Market(model) => {
                    if !active.contains(&model.session_id) {
                        return std::future::ready(Err(cryptofeed_core::error::Error::Protocol(
                            "model outside active replay session".into(),
                        )));
                    }
                    models += 1;
                }
                _ => {}
            }
            std::future::ready(Ok(()))
        })
        .await?
        .ok_or("raw replay stopped")?;
    if models != summary.models {
        return Err("replay model count mismatch".into());
    }
    println!(
        "offline native replay: observations={}, models={}, end={:?}, open prefix sessions={}",
        summary.recording.events,
        models,
        summary.recording.end,
        active.len()
    );
    Ok(())
}
