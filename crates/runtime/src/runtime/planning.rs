use crate::exchange::{
    ExchangeFeed, binance::adapter::BinanceAdapter, bitget::adapter::BitgetAdapter,
    bybit::adapter::BybitAdapter, gateio::adapter::GateioAdapter, okx::adapter::OkxAdapter,
};
use cryptofeed_core::{
    error::{Error, Result},
    exchange::{Channel, ExchangeId},
};
use serde_json::Value;

// Official limits and conservative SDK budgets are distinguished in the
// protocol baseline. Count generated native topics, not normalized channels.
fn fits(feed: &ExchangeFeed) -> Result<bool> {
    match feed.exchange {
        ExchangeId::Binance => Ok(BinanceAdapter::connection_plans(feed)?.iter().all(|plan| {
            plan.websocket_url
                .split_once("?streams=")
                .is_some_and(|(_, streams)| streams.split('/').count() <= 1024)
        })),
        ExchangeId::Bitget => {
            let payload: Value = serde_json::from_str(&BitgetAdapter::subscription_message(feed))
                .map_err(|error| Error::Parse(error.to_string()))?;
            Ok(payload["args"]
                .as_array()
                .is_some_and(|args| args.len() < 50))
        }
        ExchangeId::Bybit => Ok(BybitAdapter::subscription_urls(feed).iter().all(|url| {
            let planned = super::bybit_feed_for_url(feed, url);
            BybitAdapter::subscription_message(&planned).len() + 64 <= 21000
        })),
        ExchangeId::Okx => Ok(OkxAdapter::subscription_urls(feed).iter().all(|url| {
            let mut planned = feed.clone();
            planned
                .channels
                .retain(|channel| (*channel == Channel::Candles) == url.ends_with("/business"));
            OkxAdapter::subscription_message(&planned).len() <= 64 * 1024
        })),
        ExchangeId::Gateio => Ok(GateioAdapter::connection_plans(feed)?.iter().all(|plan| {
            // SDK resource ceilings; these are not claimed as Gate limits.
            plan.subscription_messages.len() <= 1024
                && plan
                    .subscription_messages
                    .iter()
                    .all(|message| message.len() + 64 <= 64 * 1024)
        })),
        _ => Err(Error::UnsupportedExchange(format!("{:?}", feed.exchange))),
    }
}

pub(crate) fn physical_connection_count(feed: &ExchangeFeed) -> Result<usize> {
    Ok(match feed.exchange {
        ExchangeId::Binance => BinanceAdapter::connection_plans(feed)?.len(),
        ExchangeId::Bybit => BybitAdapter::subscription_urls(feed).len(),
        ExchangeId::Okx => OkxAdapter::subscription_urls(feed).len(),
        ExchangeId::Gateio => GateioAdapter::connection_plans(feed)?.len(),
        ExchangeId::Bitget => 1,
        _ => return Err(Error::UnsupportedExchange(format!("{:?}", feed.exchange))),
    })
}

pub(crate) fn validate_connection_counts(feeds: &[ExchangeFeed]) -> Result<()> {
    let mut counts = std::collections::HashMap::new();
    for feed in feeds {
        let connections = physical_connection_count(feed)?;
        let count = counts.entry(feed.exchange).or_insert(0);
        *count += connections;
        if *count > super::budget::MAX_CONNECTIONS_PER_EXCHANGE {
            return Err(Error::InvalidConfiguration(format!(
                "{:?}: planned connections exceed SDK budget of {}",
                feed.exchange,
                super::budget::MAX_CONNECTIONS_PER_EXCHANGE
            )));
        }
    }
    Ok(())
}

fn slice(feed: &ExchangeFeed, start: usize, len: usize) -> ExchangeFeed {
    let mut planned = feed.clone();
    planned.symbols = feed.symbols[start..start + len].to_vec();
    if !planned.channel_subscriptions.is_empty() {
        for (_, symbols) in &mut planned.channel_subscriptions {
            symbols.retain(|symbol| planned.symbols.contains(symbol));
        }
        planned
            .channel_subscriptions
            .retain(|(_, symbols)| !symbols.is_empty());
        planned.channels.retain(|channel| {
            planned
                .channel_subscriptions
                .iter()
                .any(|(candidate, _)| candidate == channel)
        });
    }
    if !feed.exchange_symbols.is_empty() {
        planned.exchange_symbols = feed.exchange_symbols[start..start + len].to_vec();
    }
    planned
}

pub(crate) fn shard(feed: &ExchangeFeed) -> Result<Vec<ExchangeFeed>> {
    if !feed.exchange_symbols.is_empty() && feed.exchange_symbols.len() != feed.symbols.len() {
        return Err(Error::InvalidConfiguration(
            "normalized and exchange symbol counts must match".to_owned(),
        ));
    }
    if fits(feed)? {
        return Ok(vec![feed.clone()]);
    }
    let mut result = Vec::new();
    let mut start = 0;
    while start < feed.symbols.len() {
        if !fits(&slice(feed, start, 1))? {
            return Err(Error::InvalidConfiguration(format!(
                "{:?}: one instrument exceeds the subscription budget",
                feed.exchange
            )));
        }
        let (mut low, mut high) = (1, feed.symbols.len() - start);
        // Generated topic/message sizes are monotonic as symbols are appended.
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if fits(&slice(feed, start, middle))? {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        result.push(slice(feed, start, low));
        start += low;
    }
    Ok(result)
}

#[cfg(all(
    test,
    feature = "trade",
    feature = "funding",
    feature = "index",
    feature = "markprice"
))]
mod tests {
    use super::*;
    use crate::prelude::*;

    fn feed(exchange: ExchangeId, count: usize) -> ExchangeFeed {
        ExchangeFeedBuilder::new(exchange)
            .trade()
            .instruments((0..count).map(|i| Symbol::spot(&format!("S{i}"), "USDT")))
            .build()
    }

    #[test]
    fn binance_exact_stream_limit_and_overflow() {
        assert_eq!(shard(&feed(ExchangeId::Binance, 1024)).unwrap().len(), 1);
        let groups = shard(&feed(ExchangeId::Binance, 1025)).unwrap();
        assert_eq!(
            groups.iter().map(|g| g.symbols.len()).collect::<Vec<_>>(),
            [1024, 1]
        );
    }

    #[test]
    fn bitget_uses_the_recommended_channel_budget_after_deduplication() {
        let groups = shard(&feed(ExchangeId::Bitget, 50)).unwrap();
        assert_eq!(
            groups.iter().map(|g| g.symbols.len()).collect::<Vec<_>>(),
            [49, 1]
        );
        let shared = Bitget::new()
            .funding()
            .index()
            .mark_price()
            .instruments((0..49).map(|i| Symbol::perpetual(&format!("S{i}"), "USDT")))
            .build();
        assert_eq!(shard(&shared).unwrap().len(), 1);
    }

    #[test]
    fn bybit_character_budget_preserves_native_pairs() {
        let names: Vec<_> = (0..30)
            .map(|i| format!("S{i}{}", "X".repeat(1000)))
            .collect();
        let mut input = feed(ExchangeId::Bybit, names.len());
        input.exchange_symbols = names.clone();
        let groups = shard(&input).unwrap();
        assert!(groups.len() > 1);
        assert_eq!(
            groups
                .iter()
                .flat_map(|g| g.exchange_symbols.clone())
                .collect::<Vec<_>>(),
            names
        );
        assert!(groups.iter().all(|g| fits(g).unwrap()));
    }

    #[test]
    fn okx_and_gate_resource_boundaries_split_without_losing_symbols() {
        let mut okx = feed(ExchangeId::Okx, 100);
        okx.exchange_symbols = (0..100)
            .map(|i| format!("S{i}{}", "X".repeat(1000)))
            .collect();
        let groups = shard(&okx).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.iter().map(|g| g.symbols.len()).sum::<usize>(), 100);
        assert!(
            groups
                .iter()
                .all(|g| OkxAdapter::subscription_message(g).len() <= 64 * 1024)
        );
        let groups = shard(&feed(ExchangeId::Gateio, 1025)).unwrap();
        assert_eq!(
            groups.iter().map(|g| g.symbols.len()).collect::<Vec<_>>(),
            [1024, 1]
        );
    }

    #[test]
    fn shared_binance_price_topics_are_counted_once() {
        let input = crate::prelude::Binance::new()
            .funding()
            .index()
            .mark_price()
            .instruments((0..1024).map(|i| Symbol::perpetual(&format!("S{i}"), "USDT")))
            .build();
        assert_eq!(shard(&input).unwrap().len(), 1);
    }

    #[test]
    fn planned_connection_count_rejects_over_budget_before_connecting() {
        let feeds: Vec<_> = (0..100).map(|_| feed(ExchangeId::Bitget, 1)).collect();
        validate_connection_counts(&feeds).unwrap();
        let mut too_many = feeds;
        too_many.push(feed(ExchangeId::Bitget, 1));
        assert!(validate_connection_counts(&too_many).is_err());
    }

    #[test]
    fn oversized_single_symbol_is_rejected_without_connecting() {
        let mut input = feed(ExchangeId::Okx, 1);
        input.exchange_symbols = vec!["X".repeat(65536)];
        assert!(shard(&input).is_err());
    }
}
