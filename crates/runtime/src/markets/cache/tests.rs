use super::*;
use serde_json::json;
#[test]
fn entry_bound_evicts_lru_and_reads_promote_without_extending_ttl() {
    let now = SystemTime::now();
    let mut cache = Cache::new(2, 100, Duration::from_secs(10));
    cache.insert("a", b"1".to_vec(), now);
    cache.insert("b", b"2".to_vec(), now);
    assert!(cache.get("a", now + Duration::from_secs(1)).is_some());
    cache.insert("c", b"3".to_vec(), now + Duration::from_secs(2));
    assert!(cache.get("b", now + Duration::from_secs(2)).is_none());
    assert!(cache.get("a", now + Duration::from_secs(9)).is_some());
    assert!(cache.get("a", now + Duration::from_secs(10)).is_none());
    assert!(cache.get("c", now + Duration::from_secs(10)).is_some());
}
#[test]
fn byte_bound_includes_keys_replacement_reaccounts_and_skipped_success_invalidates_old_key() {
    let now = SystemTime::now();
    let mut cache = Cache::new(10, 12, Duration::from_secs(10));
    assert!(cache.insert("aa", b"1234".to_vec(), now));
    assert!(cache.insert("bb", b"1234".to_vec(), now));
    assert_eq!(cache.bytes, 12);
    assert!(cache.insert("aa", b"1".to_vec(), now));
    assert_eq!(cache.bytes, 9);
    assert!(cache.insert("cc", b"1234".to_vec(), now));
    assert_eq!(cache.bytes, 9);
    assert!(cache.get("bb", now).is_none());
    assert!(!cache.insert("aa", vec![b'x'; 13], now));
    assert!(cache.get("aa", now).is_none());
    assert_eq!(cache.bytes, 6);
}
#[test]
fn every_access_sweeps_expired_unvisited_namespaces_and_empty_accounting_recovers() {
    let now = SystemTime::now();
    let mut cache = Cache::new(3, 100, Duration::from_secs(10));
    for key in ["capture-1", "replay-1", "proxy-1"] {
        cache.insert(key, b"{}".to_vec(), now);
    }
    assert!(
        cache
            .get("unknown", now + Duration::from_secs(10))
            .is_none()
    );
    assert!(cache.entries.is_empty());
    assert_eq!(cache.bytes, 0);
    cache.insert("new", b"{}".to_vec(), now + Duration::from_secs(10));
    assert_eq!(cache.bytes, 5);
    // A wall-clock rollback cannot keep a future-dated entry trusted indefinitely.
    assert!(cache.get("new", now + Duration::from_secs(9)).is_none());
    assert_eq!(cache.bytes, 0);
}
#[test]
fn eviction_drops_cache_ownership_but_preserves_active_read_handles() {
    let now = SystemTime::now();
    let mut cache = Cache::new(1, 100, Duration::from_secs(10));
    cache.insert("a", b"{\"value\":1}".to_vec(), now);
    let bytes = cache.get("a", now).unwrap();
    cache.insert("b", b"{\"value\":2}".to_vec(), now);
    assert!(cache.get("a", now).is_none());
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap()["value"], 1);
    assert_eq!(cache.entries.len(), 1);
}
#[test]
fn bounded_encoding_and_roundtrip_keep_arbitrary_json_precision() {
    let value: Value =
        serde_json::from_str(r#"{"id":9007199254740993,"price":100.1234567890123456789012345}"#)
            .unwrap();
    let bytes = encode(&value, 1024).unwrap();
    let decoded: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, value);
    assert_eq!(
        decoded["price"].to_string(),
        "100.1234567890123456789012345"
    );
    assert!(encode(&json!({"padding":"x".repeat(1000)}), 32).is_none());
    assert!(encode(&value, 0).is_none());
}
#[test]
fn repeated_capture_replay_namespaces_stay_within_both_limits() {
    let now = SystemTime::now();
    let mut cache = Cache::new(8, 128, Duration::from_secs(10));
    for index in 0..10_000 {
        let key = format!("replay-{index}");
        cache.insert(&key, b"{\"v\":1}".to_vec(), now);
        assert!(cache.entries.len() <= 8);
        assert!(cache.bytes <= 128);
    }
    assert!(cache.get("replay-0", now).is_none());
    assert!(cache.get("replay-9999", now).is_some());
}
