package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.blockcache.CacheKey;
import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify that the engine's read path goes through the shared {@link BlockCache}.
 * Tests exercise the cache as observed from outside: a hit changes the cache's
 * usage; a miss populates it; many readers share one cache.
 */
class RocksDbBlockCacheTest {

    /** Build an engine wired to a tiny, externally-observable cache. */
    private static RocksDb openWithCache(Path dir, BlockCache cache) throws IOException {
        return RocksDb.open(dir, true, cache);
    }

    @Test
    void engineDefaultsToEightMiBLruCache(@TempDir Path dir) throws IOException {
        try (RocksDb db = RocksDb.open(dir)) {
            BlockCache cache = db.blockCache();
            assertThat(cache).isInstanceOf(LruBlockCache.class);
            assertThat(cache.capacityBytes()).isEqualTo(Constants.BLOCK_CACHE_DEFAULT_BYTES);
        }
    }

    @Test
    void secondReadOfSameKeyIsServedFromCache(@TempDir Path dir) throws IOException {
        LruBlockCache cache = new LruBlockCache(1L << 20);
        try (RocksDb db = openWithCache(dir, cache)) {
            db.put(Key.of("k"), Slice.of("v"));
            db.flush();           // persist to L0 — subsequent gets go through SSTable
            // Cold read populates the cache.
            assertThat(db.get(Key.of("k"))).hasValueSatisfying(
                v -> assertThat(new String(v.toBytes())).isEqualTo("v"));
            long usageAfterFirst = cache.usageBytes();
            int sizeAfterFirst = cache.size();
            assertThat(usageAfterFirst).isGreaterThan(0L);
            assertThat(sizeAfterFirst).isGreaterThanOrEqualTo(1);

            // Repeat reads: usage and entry count must not grow — every block is a hit.
            for (int i = 0; i < 50; i++) {
                assertThat(db.get(Key.of("k"))).isPresent();
            }
            assertThat(cache.usageBytes()).isEqualTo(usageAfterFirst);
            assertThat(cache.size()).isEqualTo(sizeAfterFirst);
        }
    }

    @Test
    void cacheIsSharedAcrossMultipleSstables(@TempDir Path dir) throws IOException {
        LruBlockCache cache = new LruBlockCache(1L << 20);
        try (RocksDb db = openWithCache(dir, cache)) {
            // Three separate L0 SSTables, one key in each.
            db.put(Key.of("a"), Slice.of("av"));
            db.flush();
            db.put(Key.of("b"), Slice.of("bv"));
            db.flush();
            db.put(Key.of("c"), Slice.of("cv"));
            db.flush();

            int before = cache.size();
            assertThat(db.get(Key.of("a"))).isPresent();
            assertThat(db.get(Key.of("b"))).isPresent();
            assertThat(db.get(Key.of("c"))).isPresent();
            int after = cache.size();
            assertThat(after - before).isGreaterThanOrEqualTo(3); // one data block per file
        }
    }

    @Test
    void usageStaysUnderCapacityUnderManyDistinctReads(@TempDir Path dir) throws IOException {
        // Tiny cache forces eviction.
        LruBlockCache cache = new LruBlockCache(16 * 1024);
        try (RocksDb db = openWithCache(dir, cache)) {
            // 5000 keys produce many data blocks across several L0 SSTables.
            for (int i = 0; i < 5000; i++) {
                String k = String.format("key-%05d", i);
                String v = "val-" + i;
                db.put(Key.of(k), Slice.of(v));
                if (i % 1000 == 999) db.flush();
            }
            db.flush();
            for (int i = 0; i < 5000; i++) {
                String k = String.format("key-%05d", i);
                Optional<Slice> v = db.get(Key.of(k));
                assertThat(v).withFailMessage("missing key %s", k).isPresent();
            }
            assertThat(cache.usageBytes()).isLessThanOrEqualTo(cache.capacityBytes());
        }
    }

    @Test
    void cacheSurvivesEngineReopenButStartsEmpty(@TempDir Path dir) throws IOException {
        // Each engine instance constructs its own cache; reopen yields a fresh one.
        try (RocksDb db = RocksDb.open(dir)) {
            db.put(Key.of("k"), Slice.of("v"));
            db.flush();
            db.get(Key.of("k"));
            assertThat(db.blockCache().usageBytes()).isGreaterThan(0L);
        }
        try (RocksDb db = RocksDb.open(dir)) {
            assertThat(db.blockCache().usageBytes()).isZero(); // cold cache on reopen
            assertThat(db.get(Key.of("k"))).isPresent();
            assertThat(db.blockCache().usageBytes()).isGreaterThan(0L);
        }
    }

    @Test
    void cacheInstrumentationObservesHitsAndMisses(@TempDir Path dir) throws IOException {
        AtomicLong hits = new AtomicLong();
        AtomicLong loads = new AtomicLong();
        BlockCache instrumented = new BlockCache() {
            private final LruBlockCache inner = new LruBlockCache(1L << 20);
            @Override public Optional<byte[]> lookup(CacheKey k) {
                Optional<byte[]> r = inner.lookup(k);
                if (r.isPresent()) hits.incrementAndGet();
                return r;
            }
            @Override public void insert(CacheKey k, byte[] payload) { inner.insert(k, payload); }
            @Override public byte[] lookupOrLoad(CacheKey k, Loader loader) throws IOException {
                Optional<byte[]> hit = inner.lookup(k);
                if (hit.isPresent()) { hits.incrementAndGet(); return hit.get(); }
                loads.incrementAndGet();
                byte[] loaded = loader.load();
                inner.insert(k, loaded);
                return loaded;
            }
            @Override public long usageBytes() { return inner.usageBytes(); }
            @Override public long capacityBytes() { return inner.capacityBytes(); }
            @Override public int size() { return inner.size(); }
        };

        try (RocksDb db = openWithCache(dir, instrumented)) {
            db.put(Key.of("k"), Slice.of("v"));
            db.flush();
            // First fetch: miss → load.
            db.get(Key.of("k"));
            // Subsequent fetches: all hits.
            for (int i = 0; i < 10; i++) db.get(Key.of("k"));
        }
        assertThat(loads.get()).isGreaterThanOrEqualTo(1L);
        assertThat(hits.get()).isGreaterThanOrEqualTo(10L);
    }
}
