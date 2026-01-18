//! LRU cache for file metadata.
//!
//! This module provides a thread-safe LRU cache for file metadata,
//! reducing redundant HEAD requests to S3 for metadata prefetching.

use pf_traits::FileMetadata;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::{debug, trace};

/// Default maximum number of entries in the cache.
pub const DEFAULT_MAX_ENTRIES: usize = 1000;

/// Default TTL for cache entries.
pub const DEFAULT_TTL_SECS: u64 = 300; // 5 minutes

/// Cached metadata entry with timestamp.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// The cached metadata.
    metadata: FileMetadata,
    /// When this entry was cached.
    cached_at: Instant,
}

/// Thread-safe LRU cache for file metadata.
///
/// The cache uses a simple eviction strategy: when full, entries are
/// evicted based on age (oldest first). Entries also expire after
/// a configurable TTL.
pub struct MetadataCache {
    /// Maximum number of entries.
    max_entries: usize,

    /// TTL for cache entries.
    ttl: Duration,

    /// The cache itself, protected by a RwLock.
    cache: RwLock<HashMap<String, CacheEntry>>,
}

impl MetadataCache {
    /// Create a new metadata cache with default configuration.
    pub fn new() -> Self {
        Self::with_config(DEFAULT_MAX_ENTRIES, Duration::from_secs(DEFAULT_TTL_SECS))
    }

    /// Create a new metadata cache with custom configuration.
    pub fn with_config(max_entries: usize, ttl: Duration) -> Self {
        Self {
            max_entries,
            ttl,
            cache: RwLock::new(HashMap::with_capacity(max_entries)),
        }
    }

    /// Get metadata from the cache if available and not expired.
    pub fn get(&self, uri: &str) -> Option<FileMetadata> {
        let cache = self.cache.read().ok()?;
        let entry = cache.get(uri)?;

        // Check if entry has expired
        if entry.cached_at.elapsed() > self.ttl {
            trace!(uri = %uri, "Cache entry expired");
            return None;
        }

        trace!(uri = %uri, "Cache hit");
        Some(entry.metadata.clone())
    }

    /// Insert metadata into the cache.
    pub fn insert(&self, uri: &str, metadata: FileMetadata) {
        let mut cache = match self.cache.write() {
            Ok(guard) => guard,
            Err(_) => return, // Lock poisoned, skip caching
        };

        // Evict if at capacity
        if cache.len() >= self.max_entries {
            self.evict_oldest(&mut cache);
        }

        cache.insert(
            uri.to_string(),
            CacheEntry {
                metadata,
                cached_at: Instant::now(),
            },
        );

        debug!(uri = %uri, cache_size = cache.len(), "Inserted metadata into cache");
    }

    /// Remove expired entries and evict the oldest if necessary.
    fn evict_oldest(&self, cache: &mut HashMap<String, CacheEntry>) {
        // First, remove expired entries
        let now = Instant::now();
        cache.retain(|_, entry| now.duration_since(entry.cached_at) < self.ttl);

        // If still at capacity, remove the oldest entry
        if cache.len() >= self.max_entries {
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.cached_at)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest_key);
                trace!(uri = %oldest_key, "Evicted oldest cache entry");
            }
        }
    }

    /// Get the current number of entries in the cache.
    pub fn len(&self) -> usize {
        self.cache.read().map(|c| c.len()).unwrap_or(0)
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Remove expired entries from the cache.
    pub fn cleanup_expired(&self) {
        if let Ok(mut cache) = self.cache.write() {
            let now = Instant::now();
            let before = cache.len();
            cache.retain(|_, entry| now.duration_since(entry.cached_at) < self.ttl);
            let removed = before - cache.len();
            if removed > 0 {
                debug!(removed, remaining = cache.len(), "Cleaned up expired cache entries");
            }
        }
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_metadata(size: u64) -> FileMetadata {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        FileMetadata::new(size, schema)
    }

    #[test]
    fn test_cache_basic_operations() {
        let cache = MetadataCache::new();

        // Initially empty
        assert!(cache.is_empty());
        assert!(cache.get("s3://bucket/file.parquet").is_none());

        // Insert and retrieve
        let metadata = create_test_metadata(1024);
        cache.insert("s3://bucket/file.parquet", metadata.clone());
        assert_eq!(cache.len(), 1);

        let retrieved = cache.get("s3://bucket/file.parquet").unwrap();
        assert_eq!(retrieved.size_bytes, 1024);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = MetadataCache::with_config(3, Duration::from_secs(300));

        // Fill the cache
        for i in 0..3 {
            cache.insert(&format!("s3://bucket/file{}.parquet", i), create_test_metadata(i as u64));
        }
        assert_eq!(cache.len(), 3);

        // Insert one more - should evict oldest
        cache.insert("s3://bucket/file3.parquet", create_test_metadata(3));
        assert_eq!(cache.len(), 3);

        // Oldest entry (file0) should be evicted
        assert!(cache.get("s3://bucket/file0.parquet").is_none());
        assert!(cache.get("s3://bucket/file3.parquet").is_some());
    }

    #[test]
    fn test_cache_expiration() {
        let cache = MetadataCache::with_config(100, Duration::from_millis(50));

        cache.insert("s3://bucket/file.parquet", create_test_metadata(1024));
        assert!(cache.get("s3://bucket/file.parquet").is_some());

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(60));

        // Entry should now be expired
        assert!(cache.get("s3://bucket/file.parquet").is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = MetadataCache::new();

        cache.insert("s3://bucket/file1.parquet", create_test_metadata(1024));
        cache.insert("s3://bucket/file2.parquet", create_test_metadata(2048));
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_cleanup_expired() {
        let cache = MetadataCache::with_config(100, Duration::from_millis(50));

        cache.insert("s3://bucket/file.parquet", create_test_metadata(1024));
        assert_eq!(cache.len(), 1);

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(60));

        // Cleanup should remove expired entry
        cache.cleanup_expired();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_thread_safety() {
        use std::thread;

        let cache = Arc::new(MetadataCache::new());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let cache = cache.clone();
                thread::spawn(move || {
                    for j in 0..100 {
                        let uri = format!("s3://bucket/file_{}_{}.parquet", i, j);
                        cache.insert(&uri, create_test_metadata((i * 100 + j) as u64));
                        let _ = cache.get(&uri);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should have entries (may have evicted some due to capacity)
        assert!(cache.len() > 0);
    }
}
