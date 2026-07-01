//! Tiny in-memory replay-protection cache: a nonce is only accepted once
//! within `ttl`. Process-local and unpersisted — a restart briefly reopens
//! the replay window, which is an accepted trade-off (documented in the
//! architecture doc) rather than pulling in a shared store for a
//! single-process daemon.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct NonceCache {
    seen: Mutex<HashMap<String, Instant>>,
    ttl: Duration,
}

impl NonceCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            seen: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Returns `true` (and records the nonce) if it has not been seen
    /// within the last `ttl`; returns `false` if it's a replay. Also
    /// opportunistically sweeps expired entries so the cache doesn't grow
    /// unbounded.
    pub fn check_and_record(&self, nonce: &str) -> bool {
        let mut seen = self.seen.lock().unwrap();
        let now = Instant::now();
        seen.retain(|_, seen_at| now.duration_since(*seen_at) < self.ttl);

        if seen.contains_key(nonce) {
            return false;
        }
        seen.insert(nonce.to_string(), now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_use_of_a_nonce_is_accepted() {
        let cache = NonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_record("abc"));
    }

    #[test]
    fn replaying_the_same_nonce_is_rejected() {
        let cache = NonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_record("abc"));
        assert!(!cache.check_and_record("abc"));
    }

    #[test]
    fn distinct_nonces_are_independent() {
        let cache = NonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_record("a"));
        assert!(cache.check_and_record("b"));
    }

    #[test]
    fn a_nonce_is_accepted_again_after_the_ttl_expires() {
        let cache = NonceCache::new(Duration::from_millis(20));
        assert!(cache.check_and_record("abc"));
        std::thread::sleep(Duration::from_millis(40));
        assert!(cache.check_and_record("abc"));
    }
}
