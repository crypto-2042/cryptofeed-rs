//! Offline synthetic SDK packing example; generated names are caller assertions.
use cryptofeed_rs::{exchange::bitget::adapter::BitgetAdapter, prelude::*};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let names: Vec<_> = (0..48).map(|index| format!("S{index}-USDT")).collect();
    let mut feed = Bitget::new()
        .subscription(Channel::Trade, names.iter())
        .subscription(Channel::L2Book, names.iter())
        .subscription(Channel::Ticker, [names[0].as_str(), names[1].as_str()])
        .build();
    feed.exchange_symbols = (0..48).map(|index| format!("S{index}USDT")).collect();
    let before = feed.connection_feeds()?.len();
    // FeedHandler uses the same option after catalog/native mapping validation.
    let native = feed.exchange_symbols.clone();
    let mut feed = Bitget::new()
        .subscription(Channel::Trade, names.iter())
        .subscription(Channel::L2Book, names.iter())
        .subscription(Channel::Ticker, [names[0].as_str(), names[1].as_str()])
        .runtime_options(RuntimeOptions::default().minimum_connections(Some(1000))?)
        .build();
    feed.exchange_symbols = native;
    let groups = feed.connection_feeds()?;
    let topics: Vec<_> = groups
        .iter()
        .map(|group| {
            let value: serde_json::Value =
                serde_json::from_str(&BitgetAdapter::subscription_message(group)).unwrap();
            value["args"].as_array().unwrap().len()
        })
        .collect();
    assert_eq!(before, 3);
    assert_eq!(topics, vec![49, 49]);
    println!(
        "synthetic: {before} -> {} connections, native args {topics:?}; lower bound 2",
        groups.len()
    );
    Ok(())
}
