//! Exact fixed-route, atomic-instrument packing with bounded search and proof.
use super::*;
use std::{cmp::Reverse, sync::atomic::AtomicBool};

struct Budget<'a> {
    remaining: u64,
    cancelled: Option<&'a AtomicBool>,
}
impl Budget<'_> {
    fn tick(&mut self) -> Result<()> {
        check_cancelled(self.cancelled)?;
        if self.remaining == 0 {
            return Err(Error::InvalidConfiguration(
                "minimum connection search budget exhausted; optimality not proved".into(),
            ));
        }
        self.remaining -= 1;
        Ok(())
    }
}
fn subset(feed: &ExchangeFeed, indices: &[usize]) -> ExchangeFeed {
    let mut indices = indices.to_vec();
    indices.sort_unstable();
    let mut result = feed.clone();
    result.symbols = indices
        .iter()
        .map(|index| feed.symbols[*index].clone())
        .collect();
    if !feed.exchange_symbols.is_empty() {
        result.exchange_symbols = indices
            .iter()
            .map(|index| feed.exchange_symbols[*index].clone())
            .collect();
    }
    if !result.channel_subscriptions.is_empty() {
        let selected: std::collections::HashSet<_> = result.symbols.iter().collect();
        result
            .channel_subscriptions
            .retain(|(channel, _)| result.channels.contains(channel));
        for (_, symbols) in &mut result.channel_subscriptions {
            symbols.retain(|symbol| selected.contains(symbol));
        }
        result
            .channel_subscriptions
            .retain(|(_, symbols)| !symbols.is_empty());
        result.channels.retain(|channel| {
            result
                .channel_subscriptions
                .iter()
                .any(|(entry, _)| entry == channel)
        });
        let active: std::collections::HashSet<_> = result
            .channel_subscriptions
            .iter()
            .flat_map(|(_, symbols)| symbols.iter())
            .collect();
        let keep: Vec<_> = result
            .symbols
            .iter()
            .map(|symbol| active.contains(symbol))
            .collect();
        result.symbols = std::mem::take(&mut result.symbols)
            .into_iter()
            .enumerate()
            .filter_map(|(index, symbol)| keep[index].then_some(symbol))
            .collect();
        if !result.exchange_symbols.is_empty() {
            result.exchange_symbols = std::mem::take(&mut result.exchange_symbols)
                .into_iter()
                .enumerate()
                .filter_map(|(index, native)| keep[index].then_some(native))
                .collect();
        }
    }
    result
}
fn frame_pressure(text: String, limit: usize, reserve: usize) -> Result<(usize, usize)> {
    let mut payload: Value =
        serde_json::from_str(&text).map_err(|error| Error::Parse(error.to_string()))?;
    let args = payload["args"].as_array().ok_or_else(|| {
        Error::Protocol("subscription args absent from minimum packing model".into())
    })?;
    let pressure = args.iter().map(|arg| arg.to_string().len() + 1).sum();
    payload["args"] = serde_json::json!([]);
    let header = payload.to_string().len() + reserve;
    let capacity = limit.checked_sub(header.saturating_sub(1)).ok_or_else(|| {
        Error::InvalidConfiguration("subscription header exceeds minimum packing budget".into())
    })?;
    Ok((pressure, capacity))
}
// Native demand is a union of generated streams/args/messages. One-route byte
// pressure excludes its fixed JSON envelope. Thus ceil(union/capacity) is a lower
// bound; when singleton pressures sum to that union, costs are disjoint/additive.
fn pressure(feed: &ExchangeFeed) -> Result<(usize, usize)> {
    match feed.exchange {
        ExchangeId::Binance => Ok((
            BinanceAdapter::connection_plans(feed)?
                .iter()
                .map(|plan| plan.streams.len())
                .sum(),
            BINANCE_STREAMS,
        )),
        ExchangeId::Bitget => {
            let value: Value = serde_json::from_str(&BitgetAdapter::subscription_message(feed))
                .map_err(|error| Error::Parse(error.to_string()))?;
            Ok((
                value["args"]
                    .as_array()
                    .ok_or_else(|| Error::Protocol("Bitget args missing".into()))?
                    .len(),
                BITGET_TOPICS,
            ))
        }
        ExchangeId::Bybit => frame_pressure(
            BybitAdapter::subscription_message(feed),
            BYBIT_BYTES,
            REQUEST_METADATA_BYTES,
        ),
        ExchangeId::Okx => frame_pressure(OkxAdapter::subscription_message(feed), OKX_BYTES, 0),
        ExchangeId::Gateio => Ok((
            GateioAdapter::connection_plans(feed)?
                .iter()
                .map(|plan| plan.subscription_messages.len())
                .sum(),
            GATE_MESSAGES,
        )),
        _ => Err(Error::UnsupportedExchange(format!("{:?}", feed.exchange))),
    }
}
fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let next = a % b;
        a = b;
        b = next;
    }
    a
}
fn lower_bound(total: usize, capacity: usize, weights: &[usize]) -> usize {
    let divisor = weights.iter().copied().fold(0, gcd);
    let effective = if weights.iter().sum::<usize>() == total && divisor > 0 {
        capacity / divisor * divisor
    } else {
        capacity
    };
    total.div_ceil(effective).max(1)
}
fn contiguous<F>(size: usize, budget: &mut Budget<'_>, fits: &mut F) -> Result<Vec<Vec<usize>>>
where
    F: FnMut(&[usize]) -> Result<bool>,
{
    let mut result = vec![];
    let mut start = 0;
    while start < size {
        let (mut low, mut high) = (1, size - start);
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            budget.tick()?;
            if fits(&(start..start + middle).collect::<Vec<_>>())? {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        result.push((start..start + low).collect());
        start += low;
    }
    Ok(result)
}

fn first_fit<F>(order: &[usize], budget: &mut Budget<'_>, fits: &mut F) -> Result<Vec<Vec<usize>>>
where
    F: FnMut(&[usize]) -> Result<bool>,
{
    let mut bins: Vec<Vec<usize>> = vec![];
    for item in order {
        let mut placed = false;
        for bin in &mut bins {
            budget.tick()?;
            bin.push(*item);
            if fits(bin)? {
                placed = true;
                break;
            }
            bin.pop();
        }
        if !placed {
            budget.tick()?;
            bins.push(vec![*item]);
        }
    }
    Ok(bins)
}
// Iterative exhaustive assignment avoids input-sized recursion. Only empty bins
// are symmetric: equal scalar loads can have different shared native aliases.
#[cfg(test)]
fn feasible<F>(
    order: &[usize],
    count: usize,
    budget: &mut Budget<'_>,
    fits: &mut F,
) -> Result<Option<Vec<Vec<usize>>>>
where
    F: FnMut(&[usize]) -> Result<bool>,
{
    feasible_with_bound(order, count, budget, fits, None)
}
fn feasible_with_bound<F>(
    order: &[usize],
    count: usize,
    budget: &mut Budget<'_>,
    fits: &mut F,
    additive: Option<(&[usize], usize)>,
) -> Result<Option<Vec<Vec<usize>>>>
where
    F: FnMut(&[usize]) -> Result<bool>,
{
    let mut bins = vec![vec![]; count];
    let mut next = vec![0; order.len()];
    let mut chosen = vec![0; order.len()];
    let mut depth = 0;
    loop {
        if depth == order.len() {
            return Ok(Some(
                bins.into_iter().filter(|bin| !bin.is_empty()).collect(),
            ));
        }
        budget.tick()?;
        if let Some((weights, capacity)) = additive {
            let divisor = order[depth..]
                .iter()
                .map(|index| weights[*index])
                .fold(0, gcd);
            let remaining: usize = order[depth..].iter().map(|index| weights[*index]).sum();
            let usable: usize = if divisor == 0 {
                remaining
            } else {
                bins.iter()
                    .map(|bin: &Vec<usize>| {
                        let used: usize = bin.iter().map(|index| weights[*index]).sum();
                        capacity.saturating_sub(used) / divisor * divisor
                    })
                    .sum()
            };
            if remaining > usable {
                if depth == 0 {
                    return Ok(None);
                }
                next[depth] = 0;
                depth -= 1;
                bins[chosen[depth]].pop();
                continue;
            }
        }
        let mut placed = false;
        while next[depth] < count {
            let index = next[depth];
            next[depth] += 1;
            let empty = bins[index].is_empty();
            if empty {
                next[depth] = count;
            }
            budget.tick()?;
            bins[index].push(order[depth]);
            if empty || fits(&bins[index])? {
                chosen[depth] = index;
                depth += 1;
                if depth < order.len() {
                    next[depth] = 0;
                }
                placed = true;
                break;
            }
            bins[index].pop();
        }
        if placed {
            continue;
        }
        if depth == 0 {
            return Ok(None);
        }
        next[depth] = 0;
        depth -= 1;
        bins[chosen[depth]].pop();
    }
}
pub(super) fn shard(
    feed: &ExchangeFeed,
    limit: u64,
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<ExchangeFeed>> {
    if feed.channels.is_empty() {
        return Err(Error::InvalidConfiguration(
            "minimum packing requires a subscribed channel".into(),
        ));
    }
    let mut budget = Budget {
        remaining: limit,
        cancelled,
    };
    budget.tick()?;
    if fits(feed)? {
        return Ok(vec![feed.clone()]);
    }
    let mut output = vec![];
    for route in route_feeds(feed, cancelled)? {
        budget.tick()?;
        if fits(&route)? {
            output.push(route);
            continue;
        }
        let (total, capacity) = pressure(&route)?;
        let mut weights = Vec::with_capacity(route.symbols.len());
        for index in 0..route.symbols.len() {
            budget.tick()?;
            let item = subset(&route, &[index]);
            if !fits(&item)? {
                return Err(Error::InvalidConfiguration(format!(
                    "{:?}: one instrument exceeds the subscription budget",
                    route.exchange
                )));
            }
            let (weight, item_capacity) = pressure(&item)?;
            if item_capacity != capacity {
                return Err(Error::Protocol(
                    "minimum packing envelope changed across instruments".into(),
                ));
            }
            weights.push(weight);
        }
        let lower = lower_bound(total, capacity, &weights);
        let mut oracle = |indices: &[usize]| fits(&subset(&route, indices));
        let original = contiguous(route.symbols.len(), &mut budget, &mut oracle)?;
        if lower > original.len() {
            return Err(Error::Protocol(
                "minimum packing lower bound contradicts a valid layout".into(),
            ));
        }
        if original.len() == lower {
            output.extend(original.into_iter().map(|indices| subset(&route, &indices)));
            continue;
        }
        let mut order: Vec<_> = (0..weights.len()).collect();
        order.sort_by_key(|index| (Reverse(weights[*index]), *index));
        let greedy = first_fit(&order, &mut budget, &mut oracle)?;
        let mut upper = if greedy.len() < original.len() {
            greedy
        } else {
            original
        };
        if lower > upper.len() {
            return Err(Error::Protocol(
                "minimum packing lower bound contradicts a valid layout".into(),
            ));
        }
        let additive =
            (weights.iter().sum::<usize>() == total).then_some((weights.as_slice(), capacity));
        for count in lower..upper.len() {
            if let Some(found) =
                feasible_with_bound(&order, count, &mut budget, &mut oracle, additive)?
            {
                if found.len() < lower {
                    return Err(Error::Protocol(
                        "minimum packing lower bound contradicts a valid layout".into(),
                    ));
                }
                upper = found;
                break;
            }
        }
        upper.sort_by_key(|bin| *bin.iter().min().expect("nonempty packing bin"));
        for bin in upper {
            let group = subset(&route, &bin);
            budget.tick()?;
            if !fits(&group)? {
                return Err(Error::Protocol(
                    "minimum packing result exceeds native budget".into(),
                ));
            }
            output.push(group);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
