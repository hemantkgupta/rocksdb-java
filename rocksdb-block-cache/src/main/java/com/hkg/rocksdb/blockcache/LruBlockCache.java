package com.hkg.rocksdb.blockcache;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Single-shard LRU block cache, bounded by total payload bytes. Backed by a
 * {@link LinkedHashMap} in access-order: every {@link #lookup} promotes the
 * entry to the most-recently-used end; every {@link #insert} appends there.
 * When the running byte total exceeds {@link #capacityBytes()}, the oldest
 * entries are evicted until the cache is back under budget.
 *
 * <p>Concurrency: all public methods synchronise on the instance. RocksDb's
 * original cache shards the lock; this implementation keeps a single lock for
 * simplicity. At RocksDb's target scales (one embedding process, 8 MiB cache)
 * the contention is acceptable.
 */
public final class LruBlockCache implements BlockCache {

    private final long capacityBytes;
    private final LinkedHashMap<CacheKey, byte[]> map;
    private long currentBytes;

    public LruBlockCache(long capacityBytes) {
        if (capacityBytes <= 0L) {
            throw new IllegalArgumentException("capacityBytes must be > 0, got " + capacityBytes);
        }
        this.capacityBytes = capacityBytes;
        this.map = new LinkedHashMap<>(16, 0.75f, /* accessOrder = */ true);
        this.currentBytes = 0L;
    }

    @Override
    public synchronized Optional<byte[]> lookup(CacheKey key) {
        Objects.requireNonNull(key, "key");
        byte[] hit = map.get(key);
        return hit == null ? Optional.empty() : Optional.of(hit);
    }

    @Override
    public synchronized void insert(CacheKey key, byte[] payload) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(payload, "payload");
        byte[] prior = map.put(key, payload);
        if (prior != null) {
            currentBytes -= prior.length;
        }
        currentBytes += payload.length;
        evictIfNeeded();
    }

    @Override
    public byte[] lookupOrLoad(CacheKey key, Loader loader) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");
        synchronized (this) {
            byte[] hit = map.get(key);
            if (hit != null) return hit;
        }
        // Load outside the lock so two concurrent misses on distinct keys don't serialise on disk I/O.
        byte[] loaded = loader.load();
        synchronized (this) {
            // Another thread may have inserted under the same key while we were loading; keep theirs.
            byte[] raced = map.get(key);
            if (raced != null) return raced;
            map.put(key, loaded);
            currentBytes += loaded.length;
            evictIfNeeded();
            return loaded;
        }
    }

    @Override
    public synchronized long usageBytes() {
        return currentBytes;
    }

    @Override
    public long capacityBytes() {
        return capacityBytes;
    }

    @Override
    public synchronized int size() {
        return map.size();
    }

    private void evictIfNeeded() {
        // Caller holds the monitor.
        while (currentBytes > capacityBytes && !map.isEmpty()) {
            Iterator<Map.Entry<CacheKey, byte[]>> it = map.entrySet().iterator();
            Map.Entry<CacheKey, byte[]> oldest = it.next();
            it.remove();
            currentBytes -= oldest.getValue().length;
        }
    }
}
