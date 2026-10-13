use super::*;
use crate::prelude::*;
use std::{collections::HashMap, sync::atomic::Ordering};
fn work() -> Budget<'static> {
    Budget {
        remaining: 1_000_000,
        cancelled: None,
    }
}
fn brute(weights: &[usize], capacity: usize) -> usize {
    for count in 1..=weights.len() {
        for mut encoding in 0..count.pow(weights.len() as u32) {
            let mut sums = vec![0; count];
            for weight in weights {
                sums[encoding % count] += weight;
                encoding /= count;
            }
            if sums.iter().all(|sum| *sum <= capacity) {
                return count;
            }
        }
    }
    unreachable!()
}
#[test]
fn minimum_packing_exhaustive_kernel_and_lower_bounds_match_independent_small_oracle() {
    for size in 1..=6 {
        for mut encoding in 0..3usize.pow(size as u32) {
            let weights: Vec<_> = (0..size)
                .map(|_| {
                    let weight = encoding % 3 + 1;
                    encoding /= 3;
                    weight
                })
                .collect();
            let expected = brute(&weights, 5);
            let order: Vec<_> = (0..size).collect();
            assert!(lower_bound(weights.iter().sum(), 5, &weights) <= expected);
            let mut oracle = |indices: &[usize]| {
                Ok(indices.iter().map(|index| weights[*index]).sum::<usize>() <= 5)
            };
            for count in 1..=expected {
                assert_eq!(
                    feasible(&order, count, &mut work(), &mut oracle)
                        .unwrap()
                        .is_some(),
                    count == expected
                );
                assert_eq!(
                    feasible_with_bound(
                        &order,
                        count,
                        &mut work(),
                        &mut oracle,
                        Some((&weights, 5))
                    )
                    .unwrap()
                    .is_some(),
                    count == expected
                );
            }
        }
    }
}
#[test]
fn minimum_packing_exact_search_finds_solution_that_first_fit_misses() {
    let weights = [9, 8, 7, 6, 5, 5];
    let order: Vec<_> = (0..weights.len()).collect();
    let mut oracle =
        |indices: &[usize]| Ok(indices.iter().map(|index| weights[*index]).sum::<usize>() <= 20);
    assert_eq!(
        first_fit(&order, &mut work(), &mut oracle).unwrap().len(),
        3
    );
    assert!(
        feasible(&order, 2, &mut work(), &mut oracle)
            .unwrap()
            .is_some()
    );
    assert_eq!(lower_bound(98, 49, &[2; 49]), 3);
}
#[test]
fn minimum_packing_shared_aliases_do_not_allow_equal_load_symmetry_pruning() {
    let atoms = [vec![0], vec![1], vec![0, 2], vec![1, 3]];
    let mut oracle = |indices: &[usize]| {
        Ok(indices
            .iter()
            .flat_map(|index| atoms[*index].iter())
            .collect::<std::collections::HashSet<_>>()
            .len()
            <= 2)
    };
    assert!(
        feasible(&[0, 1, 2, 3], 2, &mut work(), &mut oracle)
            .unwrap()
            .is_some()
    );
    assert_eq!(lower_bound(4, 2, &[1, 1, 2, 2]), 2);
}
#[test]
fn minimum_packing_budget_and_cooperative_cancel_return_no_unproven_layout() {
    let mut limited = Budget {
        remaining: 1,
        cancelled: None,
    };
    assert!(
        feasible(&[0, 1, 2], 2, &mut limited, &mut |_| Ok(false))
            .err()
            .unwrap()
            .to_string()
            .contains("optimality not proved")
    );
    let cancelled = AtomicBool::new(false);
    let mut budget = Budget {
        remaining: 100,
        cancelled: Some(&cancelled),
    };
    let mut calls = 0;
    let result = feasible(&[0, 1, 2], 2, &mut budget, &mut |_| {
        calls += 1;
        cancelled.store(true, Ordering::Release);
        Ok(false)
    });
    assert!(result.err().unwrap().to_string().contains("cancelled"));
    assert_eq!(calls, 1);
}
fn bitget() -> ExchangeFeed {
    let names: Vec<_> = (0..50).map(|index| format!("S{index}-USDT")).collect();
    let mut feed = Bitget::new()
        .subscription(Channel::Trade, names.iter())
        .subscription(
            Channel::L2Book,
            names
                .iter()
                .enumerate()
                .filter(|(index, _)| !matches!(index, 25 | 26))
                .map(|(_, name)| name),
        )
        .build();
    feed.exchange_symbols = (0..50).map(|index| format!("S{index}USDT")).collect();
    feed
}
fn exact_pairs(original: &ExchangeFeed, groups: &[ExchangeFeed]) {
    let mut actual = HashMap::new();
    for group in groups {
        assert!(fits(group).unwrap());
        for (index, symbol) in group.symbols.iter().enumerate() {
            let expected = original
                .symbols
                .iter()
                .position(|known| known == symbol)
                .unwrap();
            assert_eq!(
                group.exchange_symbols[index],
                original.exchange_symbols[expected]
            );
            for channel in &group.channels {
                if group.subscribes(*channel, symbol) {
                    *actual.entry((*channel, symbol.clone())).or_insert(0) += 1;
                }
            }
        }
    }
    let mut total = 0;
    for symbol in &original.symbols {
        for channel in &original.channels {
            if original.subscribes(*channel, symbol) {
                assert_eq!(actual.get(&(*channel, symbol.clone())), Some(&1));
                total += 1;
            }
        }
    }
    assert_eq!(actual.len(), total);
}
#[test]
fn minimum_packing_bitget_reorders_noncontiguous_instruments_and_proves_two_sockets() {
    let mut feed = bitget();
    assert_eq!(feed.connection_feeds().unwrap().len(), 3);
    feed.runtime_options = RuntimeOptions::default()
        .minimum_connections(Some(1000))
        .unwrap();
    let budget = ResourceBudget::new(
        ResourcePolicy::default()
            .websocket(2, std::time::Duration::from_millis(1100))
            .unwrap(),
    );
    feed.transport = TransportConfig::direct().with_resource_budget(budget);
    let groups = feed.connection_feeds().unwrap();
    assert_eq!(groups.len(), 2);
    validate_connection_counts(&groups).unwrap();
    exact_pairs(&feed, &groups);
    for group in groups {
        let (used, capacity) = pressure(&group).unwrap();
        assert_eq!((used, capacity), (49, 49));
    }
}
#[test]
fn minimum_packing_native_exact_search_resolves_greedy_residue_failure() {
    let names: Vec<_> = (0..48).map(|index| format!("S{index}-USDT")).collect();
    let mut feed = Bitget::new()
        .subscription(Channel::Trade, names.iter())
        .subscription(Channel::L2Book, names.iter())
        .subscription(Channel::Ticker, [names[0].as_str(), names[1].as_str()])
        .build();
    feed.exchange_symbols = (0..48).map(|index| format!("S{index}USDT")).collect();
    assert_eq!(feed.connection_feeds().unwrap().len(), 3);
    let weights = std::iter::repeat_n(3, 2)
        .chain(std::iter::repeat_n(2, 46))
        .collect::<Vec<_>>();
    let mut oracle =
        |indices: &[usize]| Ok(indices.iter().map(|index| weights[*index]).sum::<usize>() <= 49);
    assert_eq!(
        first_fit(&(0..48).collect::<Vec<_>>(), &mut work(), &mut oracle)
            .unwrap()
            .len(),
        3
    );
    feed.runtime_options = RuntimeOptions::default()
        .minimum_connections(Some(1000))
        .unwrap();
    let groups = feed.connection_feeds().unwrap();
    assert_eq!(groups.len(), 2);
    exact_pairs(&feed, &groups);
    assert!(groups.iter().all(|group| pressure(group).unwrap().0 == 49));
}
#[test]
fn minimum_packing_byte_models_reduce_bybit_and_okx_three_sockets_to_two() {
    for (exchange, sizes) in [
        (ExchangeId::Bybit, [11800, 11800, 8500, 8500]),
        (ExchangeId::Okx, [40000, 40000, 24000, 24000]),
    ] {
        let mut feed = ExchangeFeedBuilder::new(exchange)
            .trade()
            .instruments((0..4).map(|index| Symbol::spot(&format!("S{index}"), "USDT")))
            .build();
        feed.exchange_symbols = sizes
            .iter()
            .enumerate()
            .map(|(index, size)| format!("S{index}{}", "X".repeat(*size)))
            .collect();
        assert_eq!(feed.connection_feeds().unwrap().len(), 3);
        feed.runtime_options = RuntimeOptions::default()
            .minimum_connections(Some(1000))
            .unwrap();
        let groups = feed.connection_feeds().unwrap();
        assert_eq!(groups.len(), 2);
        exact_pairs(&feed, &groups);
    }
}
#[test]
fn minimum_packing_stream_and_message_models_prove_uniform_optima_for_binance_and_gate() {
    for exchange in [ExchangeId::Binance, ExchangeId::Gateio] {
        let mut feed = ExchangeFeedBuilder::new(exchange)
            .trade()
            .l2_book()
            .instruments((0..1025).map(|index| Symbol::spot(&format!("S{index}"), "USDT")))
            .runtime_options(
                RuntimeOptions::default()
                    .minimum_connections(Some(10000))
                    .unwrap(),
            )
            .build();
        feed.exchange_symbols = (0..1025)
            .map(|index| {
                if exchange == ExchangeId::Binance {
                    format!("S{index}USDT")
                } else {
                    format!("S{index}_USDT")
                }
            })
            .collect();
        let groups = feed.connection_feeds().unwrap();
        assert_eq!(groups.len(), 3);
        exact_pairs(&feed, &groups);
    }
}
#[cfg(all(feature = "funding", feature = "index", feature = "markprice"))]
#[test]
fn minimum_packing_retains_shared_price_topics_across_physical_routes() {
    let names: Vec<_> = (0..1025)
        .map(|index| format!("S{index}-USDT-PERP"))
        .collect();
    let mut feed = Binance::new()
        .subscription(Channel::L2Book, names.iter())
        .subscription(Channel::Funding, names.iter())
        .subscription(Channel::Index, names.iter())
        .subscription(Channel::MarkPrice, names.iter())
        .runtime_options(
            RuntimeOptions::default()
                .minimum_connections(Some(20000))
                .unwrap(),
        )
        .build();
    feed.exchange_symbols = (0..1025).map(|index| format!("S{index}USDT")).collect();
    let groups = feed.connection_feeds().unwrap();
    assert_eq!(
        groups
            .iter()
            .map(|group| physical_connection_count(group).unwrap())
            .sum::<usize>(),
        4
    );
    exact_pairs(&feed, &groups);
}
#[test]
fn minimum_packing_low_budget_does_not_fallback_and_single_overflow_stays_an_error() {
    let mut feed = bitget();
    feed.runtime_options = RuntimeOptions::default()
        .minimum_connections(Some(1))
        .unwrap();
    assert!(
        feed.connection_feeds()
            .err()
            .unwrap()
            .to_string()
            .contains("optimality not proved")
    );
    let cancel = AtomicBool::new(true);
    assert!(
        feed.connection_feeds_cancelable(Some(&cancel))
            .err()
            .unwrap()
            .to_string()
            .contains("cancelled")
    );
    let mut oversized = Okx::new()
        .trade()
        .symbol("BTC-USDT")
        .runtime_options(
            RuntimeOptions::default()
                .minimum_connections(Some(100))
                .unwrap(),
        )
        .build();
    oversized.exchange_symbols = vec!["X".repeat(65536)];
    assert!(oversized.connection_feeds().is_err());
}
#[test]
fn minimum_packing_policy_preserves_defaults_and_rejects_zero_work() {
    let empty = Bitget::new()
        .symbol("BTC-USDT")
        .runtime_options(
            RuntimeOptions::default()
                .minimum_connections(Some(100))
                .unwrap(),
        )
        .build();
    assert!(empty.connection_feeds().is_err());
    assert_eq!(RuntimeOptions::default().packing_evaluation_limit(), None);
    assert!(
        RuntimeOptions::default()
            .minimum_connections(Some(0))
            .is_err()
    );
    let options = RuntimeOptions::default()
        .minimum_connections(Some(100))
        .unwrap();
    assert_eq!(options.packing_evaluation_limit(), Some(100));
    assert_eq!(
        options
            .minimum_connections(None)
            .unwrap()
            .packing_evaluation_limit(),
        None
    );
}
#[test]
fn minimum_packing_native_pressure_matches_existing_budget_oracle_for_subsets() {
    for exchange in [
        ExchangeId::Binance,
        ExchangeId::Bitget,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Gateio,
    ] {
        let mut feed = ExchangeFeedBuilder::new(exchange)
            .trade()
            .ticker()
            .l1_book()
            .l2_book()
            .candles()
            .instruments((0..4).map(|index| Symbol::spot(&format!("S{index}"), "USDT")))
            .build();
        feed.exchange_symbols = (0..4)
            .map(|index| format!("S{index}{}", "X".repeat(1000)))
            .collect();
        for mask in 1usize..16 {
            let group = subset(
                &feed,
                &(0..4)
                    .filter(|index| mask & (1 << index) != 0)
                    .collect::<Vec<_>>(),
            );
            let (used, capacity) = pressure(&group).unwrap();
            assert_eq!(used <= capacity, fits(&group).unwrap());
        }
    }
}
