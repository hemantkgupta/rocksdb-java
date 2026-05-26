package com.hkg.rocksdb.blockcache;

import com.hkg.rocksdb.common.FileNumber;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LruBlockCacheTest {

    private static CacheKey key(long file, long offset) {
        return new CacheKey(new FileNumber(file), offset);
    }

    @Test
    void insertThenLookupReturnsValue() {
        LruBlockCache c = new LruBlockCache(1024);
        byte[] payload = new byte[] {1, 2, 3};
        c.insert(key(1, 0), payload);
        assertThat(c.lookup(key(1, 0))).hasValue(payload);
        assertThat(c.usageBytes()).isEqualTo(3L);
        assertThat(c.size()).isEqualTo(1);
    }

    @Test
    void lookupMissReturnsEmpty() {
        LruBlockCache c = new LruBlockCache(1024);
        assertThat(c.lookup(key(7, 4096))).isEmpty();
        assertThat(c.usageBytes()).isZero();
    }

    @Test
    void insertOfExistingKeyOverwritesAndUpdatesBytes() {
        LruBlockCache c = new LruBlockCache(1024);
        c.insert(key(1, 0), new byte[] {1, 2, 3});
        c.insert(key(1, 0), new byte[] {7, 8});
        assertThat(c.lookup(key(1, 0)).orElseThrow()).containsExactly(7, 8);
        assertThat(c.usageBytes()).isEqualTo(2L);
        assertThat(c.size()).isEqualTo(1);
    }

    @Test
    void lruEvictionAtCapacityBoundary() {
        // Capacity 100 B; insert four 40-byte payloads — only the two most recent should survive.
        LruBlockCache c = new LruBlockCache(100);
        c.insert(key(1, 0),   new byte[40]);
        c.insert(key(1, 100), new byte[40]);
        // Touching key (1,0) promotes it.
        assertThat(c.lookup(key(1, 0))).isPresent();
        // Now insert two more — the least-recently-used (1,100) should evict first.
        c.insert(key(1, 200), new byte[40]);
        assertThat(c.lookup(key(1, 100))).isEmpty();
        assertThat(c.lookup(key(1, 0))).isPresent();
        assertThat(c.lookup(key(1, 200))).isPresent();
        assertThat(c.usageBytes()).isLessThanOrEqualTo(100L);
    }

    @Test
    void usageStaysWithinCapacityUnderChurn() {
        LruBlockCache c = new LruBlockCache(1024);
        for (int i = 0; i < 100; i++) {
            c.insert(key(1, i * 64L), new byte[64]);
        }
        assertThat(c.usageBytes()).isLessThanOrEqualTo(1024L);
        assertThat(c.size()).isLessThanOrEqualTo(16);
    }

    @Test
    void lookupOrLoadCallsLoaderOnMissAndCachesResult() throws Exception {
        LruBlockCache c = new LruBlockCache(1024);
        AtomicInteger loads = new AtomicInteger();
        byte[] payload = new byte[] {9, 9, 9};
        byte[] first  = c.lookupOrLoad(key(1, 0), () -> { loads.incrementAndGet(); return payload; });
        byte[] second = c.lookupOrLoad(key(1, 0), () -> { loads.incrementAndGet(); return payload; });
        assertThat(first).isEqualTo(payload);
        assertThat(second).isSameAs(payload);
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void capacityMustBePositive() {
        assertThatThrownBy(() -> new LruBlockCache(0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LruBlockCache(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cacheKeyRejectsNegativeOffset() {
        assertThatThrownBy(() -> new CacheKey(new FileNumber(1), -1L))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cacheIsSafeForConcurrentLookupOrLoad() throws Exception {
        LruBlockCache c = new LruBlockCache(64 * 1024);
        ExecutorService es = Executors.newFixedThreadPool(8);
        try {
            AtomicInteger errors = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);
            for (int t = 0; t < 8; t++) {
                final int tid = t;
                es.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < 200; i++) {
                            CacheKey k = key(1, (i % 32) * 4096L);
                            c.lookupOrLoad(k, () -> new byte[256]);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                });
            }
            start.countDown();
            es.shutdown();
            assertThat(es.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            assertThat(errors.get()).isZero();
            assertThat(c.usageBytes()).isLessThanOrEqualTo(c.capacityBytes());
        } finally {
            es.shutdownNow();
        }
    }
}
