//! Bounded process-wide retention of serialized directory responses.
use serde_json::Value;
use std::{
    collections::HashMap,
    io::Write,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, SystemTime},
};
const MAX_ENTRIES: usize = 256;
const MAX_BYTES: usize = 128 * 1024 * 1024;
const TTL: Duration = Duration::from_secs(24 * 60 * 60);
struct Entry {
    stored: SystemTime,
    touched: u64,
    bytes: Arc<[u8]>,
}
struct Cache {
    entries: HashMap<Arc<str>, Entry>,
    bytes: usize,
    clock: u64,
    max_entries: usize,
    max_bytes: usize,
    ttl: Duration,
}
impl Cache {
    fn new(max_entries: usize, max_bytes: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            clock: 0,
            max_entries,
            max_bytes,
            ttl,
        }
    }
    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes -= key.len() + entry.bytes.len();
        }
    }
    fn prune(&mut self, now: SystemTime) {
        let bytes = &mut self.bytes;
        self.entries.retain(|key, entry| {
            let keep = now
                .duration_since(entry.stored)
                .is_ok_and(|age| age < self.ttl);
            if !keep {
                *bytes -= key.len() + entry.bytes.len();
            }
            keep
        });
    }
    fn get(&mut self, key: &str, now: SystemTime) -> Option<Arc<[u8]>> {
        self.prune(now);
        let entry = self.entries.get_mut(key)?;
        self.clock = self.clock.saturating_add(1);
        entry.touched = self.clock;
        Some(entry.bytes.clone())
    }
    fn insert(&mut self, key: &str, bytes: Vec<u8>, now: SystemTime) -> bool {
        self.prune(now);
        // This is a successful response replacing an old one. If it cannot be retained,
        // invalidate the old key rather than serving it as the newly refreshed value.
        self.remove(key);
        if self.max_entries == 0
            || key.len() > self.max_bytes
            || bytes.len() > self.max_bytes - key.len()
        {
            return false;
        }
        let weight = key.len() + bytes.len();
        while self.entries.len() >= self.max_entries || weight > self.max_bytes - self.bytes {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.touched)
                .map(|(key, _)| key.clone())
                .expect("retained entry");
            self.remove(&oldest);
        }
        self.clock = self.clock.saturating_add(1);
        self.bytes += weight;
        self.entries.insert(
            Arc::from(key),
            Entry {
                stored: now,
                touched: self.clock,
                bytes: Arc::from(bytes.into_boxed_slice()),
            },
        );
        true
    }
}
fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Cache::new(MAX_ENTRIES, MAX_BYTES, TTL)))
}
struct Buffer {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("catalog cache entry limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode(value: &Value, maximum: usize) -> Option<Vec<u8>> {
    let mut buffer = Buffer {
        bytes: Vec::new(),
        maximum,
    };
    serde_json::to_writer(&mut buffer, value).ok()?;
    Some(buffer.bytes)
}
pub(super) fn read_catalog_cache(key: &str) -> Option<Value> {
    let bytes = {
        let mut cache = cache().lock().ok()?;
        cache.get(key, SystemTime::now())?
    };
    // JSON decoding and caller-owned allocations stay outside the global lock.
    serde_json::from_slice(&bytes).ok()
}
pub(super) fn write_catalog_cache(key: &str, value: &Value) {
    let maximum = MAX_BYTES
        .saturating_sub(key.len())
        .min(super::MAX_CATALOG_RESPONSE_BYTES);
    let bytes = encode(value, maximum);
    if let Ok(mut cache) = cache().lock() {
        if let Some(bytes) = bytes {
            cache.insert(key, bytes, SystemTime::now());
        } else {
            cache.prune(SystemTime::now());
            cache.remove(key);
        }
    }
}
#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "recording"))]
pub(super) fn evict_for_test(key: &str) {
    cache().lock().unwrap().remove(key);
}
