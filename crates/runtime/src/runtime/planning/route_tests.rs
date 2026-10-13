use super::*;
use crate::prelude::*;
use std::collections::HashMap;
#[cfg(feature = "candles")]
use std::time::Duration;
fn physical(feeds: &[ExchangeFeed]) -> usize {
    feeds
        .iter()
        .map(|feed| physical_connection_count(feed).unwrap())
        .sum()
}
fn exact_projection(original: &ExchangeFeed, groups: &[ExchangeFeed]) {
    let mut actual = HashMap::new();
    for group in groups {
        assert!(fits(group).unwrap());
        group.validate_subscription_configuration().unwrap();
        assert_eq!(
            group.product_kind().unwrap(),
            original.product_kind().unwrap()
        );
        for (index, symbol) in group.symbols.iter().enumerate() {
            let position = original
                .symbols
                .iter()
                .position(|value| value == symbol)
                .unwrap();
            if !original.exchange_symbols.is_empty() {
                assert_eq!(
                    group.exchange_symbols[index],
                    original.exchange_symbols[position]
                );
            }
            for channel in &group.channels {
                if group.subscribes(*channel, symbol) {
                    *actual.entry((*channel, symbol.clone())).or_insert(0) += 1;
                }
            }
        }
    }
    let mut expected = 0;
    for symbol in &original.symbols {
        for channel in &original.channels {
            if original.subscribes(*channel, symbol) {
                expected += 1;
                assert_eq!(actual.get(&(*channel, symbol.clone())), Some(&1));
            }
        }
    }
    assert_eq!(actual.len(), expected);
}
#[cfg(feature = "candles")]
#[test]
fn route_first_okx_keeps_small_business_route_whole_when_public_shards() {
    let names: Vec<_> = (0..100).map(|index| format!("S{index}-USDT")).collect();
    let mut input = Okx::new()
        .subscription(Channel::Trade, names.iter())
        .subscription(Channel::Candles, [names[0].as_str(), names[99].as_str()])
        .build();
    input.exchange_symbols = (0..100)
        .map(|index| format!("S{index}{}", "X".repeat(1000)))
        .collect();
    let budget = ResourceBudget::new(
        ResourcePolicy::default()
            .websocket(3, Duration::from_millis(1100))
            .unwrap(),
    );
    input.transport = TransportConfig::direct().with_resource_budget(budget);
    let groups = input.connection_feeds().unwrap();
    assert_eq!(physical(&groups), 3);
    assert_eq!(
        groups
            .iter()
            .filter(|group| group.channels.contains(&Channel::Candles))
            .count(),
        1
    );
    validate_connection_counts(&groups).unwrap();
    exact_projection(&input, &groups);
}
#[cfg(feature = "orderbook")]
#[test]
fn route_first_binance_keeps_market_sparse_pairs_out_of_public_capacity_shards() {
    let names: Vec<_> = (0..1025)
        .map(|index| format!("S{index}-USDT-PERP"))
        .collect();
    let mut input = Binance::new()
        .subscription(Channel::L2Book, names.iter())
        .subscription(Channel::Trade, [names[0].as_str(), names[1024].as_str()])
        .build();
    input.exchange_symbols = (0..1025).map(|index| format!("S{index}USDT")).collect();
    let groups = input.connection_feeds().unwrap();
    assert_eq!(physical(&groups), 3);
    let market: Vec<_> = groups
        .iter()
        .filter(|group| group.channels.contains(&Channel::Trade))
        .collect();
    assert_eq!(market.len(), 1);
    assert_eq!(market[0].symbols.len(), 2);
    assert!(
        BinanceAdapter::connection_plans(market[0])
            .unwrap()
            .iter()
            .all(|plan| plan.snapshot_urls.is_empty())
    );
    exact_projection(&input, &groups);
}
#[test]
fn route_first_bybit_preserves_one_inverse_route_when_long_linear_names_shard() {
    let mut symbols = vec![Symbol::perpetual("IA", "USD")];
    symbols.extend((0..40).map(|index| Symbol::perpetual(&format!("S{index}"), "USDT")));
    symbols.push(Symbol::perpetual("IB", "USD"));
    let mut input = Bybit::new().trade().instruments(symbols).build();
    input.exchange_symbols = vec!["IAUSD".into()];
    input
        .exchange_symbols
        .extend((0..40).map(|index| format!("S{index}{}", "X".repeat(1000))));
    input.exchange_symbols.push("IBUSD".into());
    let groups = input.connection_feeds().unwrap();
    assert_eq!(physical(&groups), 3);
    assert_eq!(
        groups
            .iter()
            .filter(|group| BybitAdapter::subscription_urls(group)
                .iter()
                .any(|url| url.ends_with("/inverse")))
            .count(),
        1
    );
    exact_projection(&input, &groups);
}
#[test]
fn route_first_gate_keeps_settlement_routes_separate_from_usdt_message_shards() {
    let mut symbols = vec![Symbol::perpetual("IA", "USD")];
    symbols.extend((0..1025).map(|index| Symbol::perpetual(&format!("S{index}"), "USDT")));
    symbols.push(Symbol::perpetual("IB", "USD"));
    let mut input = Gateio::new().trade().instruments(symbols).build();
    input.exchange_symbols = std::iter::once("IA_USD".into())
        .chain((0..1025).map(|index| format!("S{index}_USDT")))
        .chain(std::iter::once("IB_USD".into()))
        .collect();
    let groups = input.connection_feeds().unwrap();
    assert_eq!(physical(&groups), 3);
    assert_eq!(
        groups
            .iter()
            .filter(|group| GateioAdapter::connection_plans(group)
                .unwrap()
                .iter()
                .any(|plan| plan.websocket_url.ends_with("/btc")))
            .count(),
        1
    );
    exact_projection(&input, &groups);
}
#[test]
fn route_first_single_route_and_small_multiroute_feeds_keep_existing_order() {
    let mut input = Bitget::new()
        .trade()
        .instruments((0..50).map(|index| Symbol::spot(&format!("S{index}"), "USDT")))
        .build();
    input.exchange_symbols = (0..50).map(|index| format!("N{index}USDT")).collect();
    let groups = input.connection_feeds().unwrap();
    assert_eq!(
        groups
            .iter()
            .map(|group| group.symbols.len())
            .collect::<Vec<_>>(),
        vec![49, 1]
    );
    assert_eq!(
        groups
            .iter()
            .flat_map(|group| group.exchange_symbols.clone())
            .collect::<Vec<_>>(),
        input.exchange_symbols
    );
    assert!(
        groups
            .iter()
            .all(|group| group.channel_subscriptions.is_empty())
    );
    exact_projection(&input, &groups);
    let small = Bybit::new()
        .trade()
        .symbol("BTC-USDT-PERP")
        .symbol("BTC-USD-PERP")
        .build();
    let groups = small.connection_feeds().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].symbols, small.symbols);
    assert_eq!(physical(&groups), 2);
}

#[test]
fn route_first_rejects_mixed_logical_products_before_route_projection() {
    let input = Bybit::new()
        .trade()
        .symbol("BTC-USDT")
        .symbol("BTC-USDT-PERP")
        .build();
    assert!(matches!(
        input.connection_feeds(),
        Err(Error::InvalidConfiguration(_))
    ));
    let empty = Bybit::new().trade().build();
    assert!(matches!(
        empty.connection_feeds(),
        Err(Error::InvalidConfiguration(_))
    ));
}
#[cfg(feature = "candles")]
#[test]
fn route_first_does_not_salvage_an_oversized_symbol_by_dropping_a_route() {
    let mut input = Okx::new()
        .subscription(Channel::Trade, ["BTC-USDT"])
        .subscription(Channel::Candles, ["ETH-USDT"])
        .build();
    input.exchange_symbols = vec!["X".repeat(65536), "ETH-USDT".into()];
    assert!(matches!(
        input.connection_feeds(),
        Err(Error::InvalidConfiguration(_))
    ));
}
