//! Verified Binance public costs and conservative rolling admission windows.
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use tokio::time::Instant;
use url::Url;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) enum Bucket {
    Spot,
    Futures,
    UsdFunding,
}
impl Bucket {
    fn defaults(self) -> (u32, Duration) {
        match self {
            Self::Spot => (6000, Duration::from_secs(60)),
            Self::Futures => (2400, Duration::from_secs(60)),
            Self::UsdFunding => (500, Duration::from_secs(300)),
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Charge {
    pub bucket: Bucket,
    pub cost: u32,
}
fn invalid() -> Error {
    Error::InvalidConfiguration("unknown or unsupported Binance public request weight".into())
}
pub(super) fn charge(exchange: ExchangeId, input: &str) -> Result<Option<Charge>> {
    if exchange != ExchangeId::Binance {
        return Ok(None);
    }
    let url = Url::parse(input).map_err(|_| invalid())?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    let host = url.host_str().ok_or_else(invalid)?;
    let (bucket, operation) = match host {
        "api.binance.com" | "data-api.binance.vision" => (
            Bucket::Spot,
            url.path().strip_prefix("/api/v3/").ok_or_else(invalid)?,
        ),
        "fapi.binance.com" | "dapi.binance.com" => (
            Bucket::Futures,
            url.path()
                .strip_prefix(if host == "fapi.binance.com" {
                    "/fapi/v1/"
                } else {
                    "/dapi/v1/"
                })
                .ok_or_else(invalid)?,
        ),
        _ => return Err(invalid()),
    };
    let mut params = HashMap::new();
    for (key, value) in url.query_pairs() {
        if params.insert(key.to_string(), value.to_string()).is_some() {
            return Err(invalid());
        }
    }
    let limit = |default: u32, maximum: u32| -> Result<u32> {
        let value = match params.get("limit") {
            Some(value) => value.parse::<u32>().map_err(|_| invalid())?,
            None => default,
        };
        if value == 0 || value > maximum {
            return Err(invalid());
        }
        Ok(value)
    };
    if params.get("symbol").is_some_and(String::is_empty) {
        return Err(invalid());
    }
    let symbol = params.contains_key("symbol") && !params.contains_key("symbols");
    let cost = match (bucket, operation) {
        (Bucket::Spot, "exchangeInfo") => 20,
        (Bucket::Spot, "ticker/bookTicker") => {
            if symbol {
                2
            } else {
                4
            }
        }
        (Bucket::Spot, "depth") => match limit(100, 5000)? {
            1..=100 => 5,
            101..=500 => 25,
            501..=1000 => 50,
            _ => 250,
        },
        (Bucket::Spot, "trades") => 25,
        (Bucket::Spot, "aggTrades") => 4,
        (Bucket::Spot, "klines") => 2,
        (Bucket::Futures, "exchangeInfo") => 1,
        (Bucket::Futures, "ticker/bookTicker") => {
            if symbol {
                2
            } else {
                5
            }
        }
        (Bucket::Futures, "depth") => match limit(500, 1000)? {
            5 | 10 | 20 | 50 => 2,
            100 => 5,
            500 => 10,
            1000 => 20,
            _ => return Err(invalid()),
        },
        (Bucket::Futures, "trades") => 5,
        (Bucket::Futures, "aggTrades") => 20,
        (Bucket::Futures, "klines") => match limit(500, 1500)? {
            1..=99 => 1,
            100..=499 => 2,
            500..=1000 => 5,
            _ => 10,
        },
        (Bucket::Futures, "fundingRate") if host == "fapi.binance.com" => {
            return Ok(Some(Charge {
                bucket: Bucket::UsdFunding,
                cost: 1,
            }));
        }
        (Bucket::Futures, "fundingRate") => 1,
        _ => return Err(invalid()),
    };
    Ok(Some(Charge { bucket, cost }))
}
struct Window {
    capacity: u32,
    duration: Duration,
    used: u32,
    entries: VecDeque<(Instant, u32)>,
    // Independent conservative floor; never replaces the local rolling ledger.
    observed: Option<(Instant, u64)>,
}
impl Window {
    fn new(bucket: Bucket) -> Self {
        let (capacity, duration) = bucket.defaults();
        Self {
            capacity,
            duration,
            used: 0,
            entries: VecDeque::new(),
            observed: None,
        }
    }
    fn ready(&mut self, now: Instant, cost: u32, ceiling: u32) -> Result<Instant> {
        if self.observed.is_some_and(|(until, _)| until <= now) {
            self.observed = None;
        }
        while self
            .entries
            .front()
            .is_some_and(|(at, _)| *at + self.duration <= now)
        {
            self.used -= self.entries.pop_front().expect("entry").1;
        }
        let capacity = self.capacity.min(ceiling);
        if cost > capacity {
            return Err(Error::InvalidConfiguration(
                "request weight exceeds effective Binance window ceiling".into(),
            ));
        }
        let observed_deadline = self
            .observed
            .filter(|(_, used)| used.saturating_add(u64::from(cost)) > u64::from(capacity))
            .map_or(now, |(until, _)| until);
        if self.used + cost <= capacity {
            return Ok(observed_deadline);
        }
        let mut remaining = self.used;
        for (at, weight) in &self.entries {
            remaining -= weight;
            if remaining + cost <= capacity {
                return Ok((*at + self.duration).max(observed_deadline));
            }
        }
        unreachable!("expired weight can satisfy a valid charge")
    }
    fn commit(&mut self, now: Instant, cost: u32) {
        self.entries.push_back((now, cost));
        self.used += cost;
        if let Some((_, used)) = self.observed.as_mut().filter(|(until, _)| *until > now) {
            *used = used.saturating_add(u64::from(cost));
        }
    }
}
#[derive(Default)]
pub(super) struct Tracker {
    windows: HashMap<Bucket, Window>,
}
impl Tracker {
    pub(super) fn observe_headers(
        &mut self,
        exchange: ExchangeId,
        url: &str,
        headers: &reqwest::header::HeaderMap,
        now: Instant,
    ) {
        let Ok(Some(charge)) = charge(exchange, url) else {
            return;
        };
        // USD funding has a distinct request-count window. USD-M bookTicker
        // explicitly documents an inaccurate used-weight header.
        if charge.bucket == Bucket::UsdFunding
            || Url::parse(url).is_ok_and(|url| {
                url.host_str() == Some("fapi.binance.com")
                    && url.path() == "/fapi/v1/ticker/bookTicker"
            })
        {
            return;
        }
        let mut values = headers.get_all("x-mbx-used-weight-1m").iter();
        let Some(value) = values.next().and_then(|value| value.to_str().ok()) else {
            return;
        };
        if values.next().is_some() || value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit())
        {
            return;
        }
        let Ok(used) = value.parse::<u32>() else {
            return;
        };
        let window = self
            .windows
            .entry(charge.bucket)
            .or_insert_with(|| Window::new(charge.bucket));
        if window.observed.is_some_and(|(until, _)| until <= now) {
            window.observed = None;
        }
        if u64::from(used) > window.observed.map_or(0, |(_, used)| used) {
            window.observed = Some((now + window.duration, u64::from(used)));
        }
    }
    pub(super) fn ready(&mut self, charge: Charge, ceiling: u32, now: Instant) -> Result<Instant> {
        self.windows
            .entry(charge.bucket)
            .or_insert_with(|| Window::new(charge.bucket))
            .ready(now, charge.cost, ceiling)
    }
    pub(super) fn commit(&mut self, charge: Charge, now: Instant) {
        self.windows
            .get_mut(&charge.bucket)
            .expect("checked window")
            .commit(now, charge.cost);
    }
    pub(super) fn observe_catalog(&mut self, exchange: ExchangeId, url: &str, value: &Value) {
        let Ok(Some(charge)) = charge(exchange, url) else {
            return;
        };
        if !url
            .split('?')
            .next()
            .is_some_and(|path| path.ends_with("/exchangeInfo"))
            || !value.get("symbols").is_some_and(Value::is_array)
        {
            return;
        }
        if let Some(rows) = value.get("rateLimits").and_then(Value::as_array) {
            for row in rows {
                if row["rateLimitType"].as_str() == Some("REQUEST_WEIGHT")
                    && row["interval"].as_str() == Some("MINUTE")
                    && row["intervalNum"].as_u64() == Some(1)
                {
                    if let Some(limit) = row["limit"]
                        .as_u64()
                        .and_then(|value| u32::try_from(value).ok())
                        .filter(|value| *value > 0)
                    {
                        let window = self
                            .windows
                            .entry(charge.bucket)
                            .or_insert_with(|| Window::new(charge.bucket));
                        window.capacity = window.capacity.min(limit);
                    }
                }
            }
        }
    }
}
#[cfg(test)]
mod tests;
