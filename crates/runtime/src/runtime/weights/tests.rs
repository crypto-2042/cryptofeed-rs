use super::*;
fn cost(path: &str) -> u32 {
    charge(
        ExchangeId::Binance,
        &format!("https://api.binance.com/api/v3/{path}"),
    )
    .unwrap()
    .unwrap()
    .cost
}
#[test]
fn official_spot_costs_and_depth_boundaries_use_current_weights() {
    for (path, expected) in [
        ("exchangeInfo", 20),
        ("trades?symbol=BTCUSDT", 25),
        ("aggTrades?symbol=BTCUSDT", 4),
        ("klines?symbol=BTCUSDT", 2),
        ("ticker/bookTicker?symbol=BTCUSDT", 2),
        ("ticker/bookTicker", 4),
        ("ticker/bookTicker?symbols=%5B%22BTCUSDT%22%5D", 4),
    ] {
        assert_eq!(cost(path), expected);
    }
    for (limit, expected) in [
        (1, 5),
        (100, 5),
        (101, 25),
        (500, 25),
        (501, 50),
        (1000, 50),
        (1001, 250),
        (5000, 250),
    ] {
        assert_eq!(cost(&format!("depth?limit={limit}")), expected);
    }
}
#[test]
fn derivative_tiers_and_funding_windows_are_not_copied_from_spot() {
    for host in ["fapi.binance.com", "dapi.binance.com"] {
        let prefix = if host.starts_with('f') {
            "fapi"
        } else {
            "dapi"
        };
        for (path, expected) in [
            ("exchangeInfo", 1),
            ("trades", 5),
            ("aggTrades", 20),
            ("ticker/bookTicker?symbol=BTCUSDT", 2),
            ("ticker/bookTicker", 5),
            ("depth?limit=50", 2),
            ("depth?limit=100", 5),
            ("depth?limit=500", 10),
            ("depth?limit=1000", 20),
            ("klines?limit=99", 1),
            ("klines?limit=100", 2),
            ("klines?limit=499", 2),
            ("klines?limit=500", 5),
            ("klines?limit=1000", 5),
            ("klines?limit=1001", 10),
        ] {
            assert_eq!(
                charge(
                    ExchangeId::Binance,
                    &format!("https://{host}/{prefix}/v1/{path}")
                )
                .unwrap()
                .unwrap()
                .cost,
                expected
            );
        }
    }
    let um = charge(
        ExchangeId::Binance,
        "https://fapi.binance.com/fapi/v1/fundingRate",
    )
    .unwrap()
    .unwrap();
    assert_eq!(um.bucket, Bucket::UsdFunding);
    assert_eq!(um.bucket.defaults(), (500, Duration::from_secs(300)));
    assert_eq!(
        charge(
            ExchangeId::Binance,
            "https://dapi.binance.com/dapi/v1/fundingRate"
        )
        .unwrap()
        .unwrap()
        .bucket,
        Bucket::Futures
    );
}
#[test]
fn unknown_or_malformed_binance_routes_never_guess_a_low_cost() {
    for url in [
        "https://api.binance.com/api/v3/order",
        "https://api.binance.com/api/v3/depth?limit=0",
        "https://api.binance.com/api/v3/depth?limit=5001",
        "https://api.binance.com/api/v3/depth?limit=abc",
        "https://fapi.binance.com/fapi/v1/depth?limit=99",
        "https://api.binance.com/api/v3/depth?limit=1&limit=2",
        "https://unknown.example/api/v3/depth",
    ] {
        assert!(charge(ExchangeId::Binance, url).is_err());
    }
    assert!(
        charge(
            ExchangeId::Okx,
            "https://openapi.okx.com/api/v5/market/books"
        )
        .unwrap()
        .is_none()
    );
}
#[tokio::test(start_paused = true)]
async fn rolling_window_checks_the_whole_interval_not_only_reset_boundaries() {
    let now = Instant::now();
    let mut window = Window {
        capacity: 10,
        duration: Duration::from_secs(60),
        used: 0,
        entries: VecDeque::new(),
        observed: None,
    };
    assert_eq!(window.ready(now, 6, 10).unwrap(), now);
    window.commit(now, 6);
    let later = now + Duration::from_secs(59);
    assert_eq!(window.ready(later, 4, 10).unwrap(), later);
    window.commit(later, 4);
    assert_eq!(
        window.ready(later, 5, 10).unwrap(),
        now + Duration::from_secs(60)
    );
    assert_eq!(
        window.ready(now + Duration::from_secs(60), 7, 10).unwrap(),
        later + Duration::from_secs(60)
    );
    assert!(window.ready(now, 11, 10).is_err());
}
#[tokio::test(start_paused = true)]
async fn directory_limits_can_only_tighten_and_private_or_other_windows_are_ignored() {
    let mut tracker = Tracker::default();
    let url = "https://api.binance.com/api/v3/exchangeInfo";
    tracker.observe_catalog(ExchangeId::Binance,url,&serde_json::json!({"symbols":[],"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":100}]}));
    assert_eq!(tracker.windows[&Bucket::Spot].capacity, 100);
    tracker.observe_catalog(ExchangeId::Binance,url,&serde_json::json!({"symbols":[],"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":99999},{"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1}]}));
    assert_eq!(tracker.windows[&Bucket::Spot].capacity, 100);
}

fn headers(value: &str) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-mbx-used-weight-1m", value.parse().unwrap());
    headers
}
const SPOT_URL: &str = "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT";

#[tokio::test(start_paused = true)]
async fn observed_usage_tightens_scoped_threshold_and_counts_later_admissions() {
    let mut tracker = Tracker::default();
    let now = Instant::now();
    let charge = charge(ExchangeId::Binance, SPOT_URL).unwrap().unwrap();
    tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &headers("96"), now);
    assert_eq!(tracker.ready(charge, 100, now).unwrap(), now);
    tracker.commit(charge, now);
    assert_eq!(tracker.ready(charge, 100, now).unwrap(), now);
    tracker.commit(charge, now);
    assert_eq!(
        tracker.ready(charge, 100, now).unwrap(),
        now + Duration::from_secs(60)
    );
    assert_eq!(tracker.windows[&Bucket::Spot].used, 4);
    assert_eq!(tracker.ready(charge, 6000, now).unwrap(), now);
}

#[tokio::test(start_paused = true)]
async fn smaller_equal_or_reset_headers_do_not_release_or_extend_a_floor() {
    let mut tracker = Tracker::default();
    let now = Instant::now();
    let charge = charge(ExchangeId::Binance, SPOT_URL).unwrap().unwrap();
    tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &headers("100"), now);
    for value in ["0", "99", "100"] {
        tracker.observe_headers(
            ExchangeId::Binance,
            SPOT_URL,
            &headers(value),
            now + Duration::from_secs(59),
        );
    }
    assert_eq!(
        tracker
            .ready(charge, 100, now + Duration::from_secs(59))
            .unwrap(),
        now + Duration::from_secs(60)
    );
    assert_eq!(
        tracker
            .ready(charge, 100, now + Duration::from_secs(60))
            .unwrap(),
        now + Duration::from_secs(60)
    );
    // The next increasing sample gets its own bounded receipt-time retention.
    tracker.observe_headers(
        ExchangeId::Binance,
        SPOT_URL,
        &headers("101"),
        now + Duration::from_secs(61),
    );
    assert_eq!(
        tracker
            .ready(charge, 100, now + Duration::from_secs(61))
            .unwrap(),
        now + Duration::from_secs(121)
    );
}

#[tokio::test(start_paused = true)]
async fn header_expiry_never_erases_local_weight_and_larger_samples_only_tighten() {
    let mut tracker = Tracker::default();
    let now = Instant::now();
    let charge = charge(ExchangeId::Binance, SPOT_URL).unwrap().unwrap();
    tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &headers("98"), now);
    let later = now + Duration::from_secs(30);
    assert_eq!(tracker.ready(charge, 100, later).unwrap(), later);
    tracker.commit(charge, later);
    tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &headers("101"), later);
    assert_eq!(
        tracker.ready(charge, 100, later).unwrap(),
        later + Duration::from_secs(60)
    );
    // A separate very strict caller still waits on the surviving local ledger.
    assert_eq!(
        tracker
            .ready(charge, 2, now + Duration::from_secs(60))
            .unwrap(),
        later + Duration::from_secs(60)
    );
    assert_eq!(
        tracker
            .ready(charge, 2, later + Duration::from_secs(60))
            .unwrap(),
        later + Duration::from_secs(60)
    );
}

#[tokio::test(start_paused = true)]
async fn header_scopes_types_duplicates_and_documented_exceptions_are_strict() {
    let now = Instant::now();
    for value in ["", "-1", "+1", "1.5", " 1", "1 ", "4294967296", "1,2"] {
        let mut tracker = Tracker::default();
        tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &headers(value), now);
        assert!(tracker.windows.is_empty(), "{value:?}");
    }
    let mut tracker = Tracker::default();
    let mut duplicate = headers("6000");
    duplicate.append("x-mbx-used-weight-1m", "1".parse().unwrap());
    tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &duplicate, now);
    for (exchange, url) in [
        (ExchangeId::Okx, SPOT_URL),
        (ExchangeId::Binance, "https://other.invalid/api/v3/trades"),
        (
            ExchangeId::Binance,
            "https://fapi.binance.com/fapi/v1/fundingRate",
        ),
        (
            ExchangeId::Binance,
            "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=BTCUSDT",
        ),
    ] {
        tracker.observe_headers(exchange, url, &headers("6000"), now);
    }
    assert!(tracker.windows.is_empty());
    tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &headers("4294967295"), now);
    let charge = charge(ExchangeId::Binance, SPOT_URL).unwrap().unwrap();
    assert_eq!(
        tracker.ready(charge, 6000, now).unwrap(),
        now + Duration::from_secs(60)
    );
    assert_eq!(
        tracker
            .ready(charge, 6000, now + Duration::from_secs(60))
            .unwrap(),
        now + Duration::from_secs(60)
    );
}

#[tokio::test(start_paused = true)]
async fn derivative_feedback_uses_shared_futures_pool_without_touching_spot_or_funding() {
    let now = Instant::now();
    let mut tracker = Tracker::default();
    for url in [
        "https://fapi.binance.com/fapi/v1/depth?limit=100",
        "https://dapi.binance.com/dapi/v1/depth?limit=100",
    ] {
        tracker.observe_headers(ExchangeId::Binance, url, &headers("2400"), now);
    }
    let futures = charge(
        ExchangeId::Binance,
        "https://dapi.binance.com/dapi/v1/fundingRate",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        tracker.ready(futures, 6000, now).unwrap(),
        now + Duration::from_secs(60)
    );
    for url in [SPOT_URL, "https://fapi.binance.com/fapi/v1/fundingRate"] {
        assert_eq!(
            tracker
                .ready(
                    charge(ExchangeId::Binance, url).unwrap().unwrap(),
                    6000,
                    now
                )
                .unwrap(),
            now
        );
    }
}

#[tokio::test(start_paused = true)]
async fn expiring_observation_does_not_clear_later_local_admissions() {
    let now = Instant::now();
    let mut tracker = Tracker::default();
    let charge = charge(ExchangeId::Binance, SPOT_URL).unwrap().unwrap();
    tracker.observe_headers(ExchangeId::Binance, SPOT_URL, &headers("98"), now);
    let later = now + Duration::from_secs(30);
    assert_eq!(tracker.ready(charge, 100, later).unwrap(), later);
    tracker.commit(charge, later);
    assert_eq!(
        tracker
            .ready(charge, 2, now + Duration::from_secs(60))
            .unwrap(),
        later + Duration::from_secs(60)
    );
    assert!(tracker.windows[&Bucket::Spot].observed.is_none());
    assert_eq!(tracker.windows[&Bucket::Spot].used, 2);
    assert_eq!(
        tracker
            .ready(charge, 2, later + Duration::from_secs(60))
            .unwrap(),
        later + Duration::from_secs(60)
    );
}
