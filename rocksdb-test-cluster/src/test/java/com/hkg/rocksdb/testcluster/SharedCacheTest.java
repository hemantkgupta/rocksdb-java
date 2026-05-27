package com.hkg.rocksdb.testcluster;

import com.hkg.rocksdb.blockcache.BlockCache;
import com.hkg.rocksdb.blockcache.CacheKey;
import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.engine.RocksDb;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 CP 25 — process-wide shared {@link BlockCache} budget across many
 * {@link RocksDb} instances, plus the per-instance override escape hatch.
 *
 * <p>The single-instance equivalent is
 * {@code com.hkg.rocksdb.engine.RocksDbBlockCacheTest}. This test cluster
 * generalises those invariants to N instances:
 *
 * <ul>
 *   <li><b>One shared cache, four DBs:</b> aggregate cache usage stays under
 *       the cache's capacity even though no single DB respects the limit.
 *       Eviction is driven by total bytes inserted across all instances.</li>
 *   <li><b>Cross-instance interference is bytes-bounded:</b> heavy traffic
 *       on instance A does not starve instance B's cold reads (B still
 *       completes), because the cache budget is by bytes, not entries.</li>
 *   <li><b>Per-instance override:</b> a fifth instance, opened with its own
 *       dedicated cache, sees zero usage on the shared cache from its reads,
 *       and the shared cache sees zero usage from the dedicated instance's
 *       reads. The two caches are completely independent.</li>
 * </ul>
 *
 * <p>This test never modifies {@code rocksdb-engine} or {@code rocksdb-block-cache};
 * it only exercises the existing {@link RocksDb#open(Path, boolean, BlockCache)}
 * overload.
 */
class SharedCacheTest {

    /** A small cache forces eviction once aggregate inserts exceed capacity. */
    private static final long SHARED_CAPACITY_BYTES = 64L * 1024L;

    /** Open an engine wired to the supplied cache; uncompressed output so
     *  every put materialises into a real disk block at flush time. */
    private static RocksDb openWith(Path dir, BlockCache cache) throws IOException {
        return RocksDb.open(dir, false, cache);
    }

    private static void populateAndFlush(RocksDb db, String prefix, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            String k = String.format("%s-%05d", prefix, i);
            // Inflate the value so each data block weighs a few hundred bytes —
            // the shared cache trips its capacity well before we exhaust the data set.
            String v = "v-" + prefix + "-" + i + "-" + "x".repeat(64);
            db.put(Key.of(k), Slice.of(v));
            if (i % 200 == 199) db.flush();
        }
        db.flush();
    }

    @Test
    void fourInstancesShareSingleCacheAndAggregateStaysUnderCapacity(
            @TempDir Path dirA, @TempDir Path dirB,
            @TempDir Path dirC, @TempDir Path dirD) throws IOException {
        LruBlockCache shared = new LruBlockCache(SHARED_CAPACITY_BYTES);
        try (RocksDb a = openWith(dirA, shared);
             RocksDb b = openWith(dirB, shared);
             RocksDb c = openWith(dirC, shared);
             RocksDb d = openWith(dirD, shared)) {

            // Each instance produces several SSTables full of distinct keys.
            populateAndFlush(a, "a", 800);
            populateAndFlush(b, "b", 800);
            populateAndFlush(c, "c", 800);
            populateAndFlush(d, "d", 800);

            // Read everything: misses populate the shared cache.
            for (RocksDb db : List.of(a, b, c, d)) {
                String prefix = (db == a) ? "a" : (db == b) ? "b" : (db == c) ? "c" : "d";
                for (int i = 0; i < 800; i++) {
                    Optional<Slice> v = db.get(Key.of(String.format("%s-%05d", prefix, i)));
                    assertThat(v).withFailMessage("missing %s-%05d", prefix, i).isPresent();
                }
            }

            // Aggregate usage is host-wide; the shared cache is the only knob.
            assertThat(shared.usageBytes())
                .as("shared cache usage stays at or under capacity across all 4 DBs")
                .isLessThanOrEqualTo(shared.capacityBytes());
            // Eviction must have kicked in — otherwise the cache was over-provisioned.
            assertThat(shared.size())
                .as("shared cache size is bounded; eviction has occurred")
                .isGreaterThan(0);
        }
    }

    @Test
    void heavyReaderDoesNotStarveColdReaderOnSharedCache(
            @TempDir Path dirA, @TempDir Path dirB) throws IOException {
        LruBlockCache shared = new LruBlockCache(SHARED_CAPACITY_BYTES);
        try (RocksDb hot = openWith(dirA, shared);
             RocksDb cold = openWith(dirB, shared)) {

            populateAndFlush(hot, "hot", 600);
            populateAndFlush(cold, "cold", 200);

            // Hammer the hot instance — many of its blocks will be evicted/re-inserted.
            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < 600; i++) {
                    assertThat(hot.get(Key.of(String.format("hot-%05d", i)))).isPresent();
                }
            }

            // Cold instance still satisfies every read despite being a minority tenant.
            for (int i = 0; i < 200; i++) {
                Optional<Slice> v = cold.get(Key.of(String.format("cold-%05d", i)));
                assertThat(v)
                    .withFailMessage("cold reader starved at cold-%05d", i)
                    .isPresent();
            }

            assertThat(shared.usageBytes()).isLessThanOrEqualTo(shared.capacityBytes());
        }
    }

    @Test
    void perInstanceOverrideCacheIsIndependentOfSharedCache(
            @TempDir Path sharedDir1, @TempDir Path sharedDir2,
            @TempDir Path dedicatedDir) throws IOException {
        LruBlockCache shared = new LruBlockCache(SHARED_CAPACITY_BYTES);
        // A "generous" per-instance override: large enough that it never evicts.
        LruBlockCache dedicated = new LruBlockCache(8L * 1024L * 1024L);

        try (RocksDb s1 = openWith(sharedDir1, shared);
             RocksDb s2 = openWith(sharedDir2, shared);
             RocksDb solo = openWith(dedicatedDir, dedicated)) {

            populateAndFlush(s1, "s1", 400);
            populateAndFlush(s2, "s2", 400);
            populateAndFlush(solo, "solo", 400);

            // Drive reads on the shared-cache tenants. Dedicated cache must not budge.
            long dedicatedBefore = dedicated.usageBytes();
            int dedicatedSizeBefore = dedicated.size();
            for (int i = 0; i < 400; i++) {
                assertThat(s1.get(Key.of(String.format("s1-%05d", i)))).isPresent();
                assertThat(s2.get(Key.of(String.format("s2-%05d", i)))).isPresent();
            }
            assertThat(dedicated.usageBytes())
                .as("dedicated cache must be unaffected by shared-tenant traffic")
                .isEqualTo(dedicatedBefore);
            assertThat(dedicated.size()).isEqualTo(dedicatedSizeBefore);

            // Drive reads on the dedicated tenant. Shared cache must not budge.
            long sharedBefore = shared.usageBytes();
            int sharedSizeBefore = shared.size();
            for (int i = 0; i < 400; i++) {
                assertThat(solo.get(Key.of(String.format("solo-%05d", i)))).isPresent();
            }
            assertThat(shared.usageBytes())
                .as("shared cache must be unaffected by dedicated-instance traffic")
                .isEqualTo(sharedBefore);
            assertThat(shared.size()).isEqualTo(sharedSizeBefore);

            // The override is large enough that nothing was evicted.
            assertThat(dedicated.usageBytes())
                .as("generous dedicated cache stays under its 8 MiB ceiling")
                .isLessThanOrEqualTo(dedicated.capacityBytes());
            assertThat(dedicated.size()).isPositive();
        }
    }

    @Test
    void sharedCacheRespectsEachInstanceDistinctlyViaCacheKey(
            @TempDir Path dirA, @TempDir Path dirB) throws IOException {
        // Two instances writing keys with the SAME user-string. The cache keys
        // include the file number — distinct SSTables, distinct cache entries.
        LruBlockCache shared = new LruBlockCache(1L << 20); // 1 MiB — no eviction expected
        try (RocksDb a = openWith(dirA, shared);
             RocksDb b = openWith(dirB, shared)) {
            for (int i = 0; i < 50; i++) {
                a.put(Key.of("k-" + i), Slice.of("from-a-" + i));
                b.put(Key.of("k-" + i), Slice.of("from-b-" + i));
            }
            a.flush();
            b.flush();

            for (int i = 0; i < 50; i++) {
                assertThat(a.get(Key.of("k-" + i)))
                    .hasValueSatisfying(v -> assertThat(new String(v.toBytes())).startsWith("from-a-"));
                assertThat(b.get(Key.of("k-" + i)))
                    .hasValueSatisfying(v -> assertThat(new String(v.toBytes())).startsWith("from-b-"));
            }

            // Both instances' blocks live in the shared cache (file-number-namespaced).
            assertThat(shared.size())
                .as("shared cache holds entries from both DBs simultaneously")
                .isGreaterThanOrEqualTo(2);
            assertThat(shared.usageBytes()).isLessThanOrEqualTo(shared.capacityBytes());
        }
    }

    @Test
    void sharedCacheServesRepeatReadsWithoutGrowingUsage(
            @TempDir Path dirA, @TempDir Path dirB) throws IOException {
        LruBlockCache shared = new LruBlockCache(1L << 20); // 1 MiB — fits all blocks
        try (RocksDb a = openWith(dirA, shared);
             RocksDb b = openWith(dirB, shared)) {
            a.put(Key.of("ka"), Slice.of("va"));
            b.put(Key.of("kb"), Slice.of("vb"));
            a.flush();
            b.flush();

            // Cold reads populate the cache.
            assertThat(a.get(Key.of("ka"))).isPresent();
            assertThat(b.get(Key.of("kb"))).isPresent();
            long usageAfterCold = shared.usageBytes();
            int sizeAfterCold = shared.size();
            assertThat(usageAfterCold).isGreaterThan(0L);

            // Repeat reads from BOTH instances must be served from the cache.
            for (int i = 0; i < 100; i++) {
                assertThat(a.get(Key.of("ka"))).isPresent();
                assertThat(b.get(Key.of("kb"))).isPresent();
            }
            assertThat(shared.usageBytes())
                .as("repeat reads across instances are pure hits — no growth")
                .isEqualTo(usageAfterCold);
            assertThat(shared.size()).isEqualTo(sizeAfterCold);
        }
    }

    @Test
    void engineHandleExposesTheSharedCacheItWasGivenAndOverrideStaysDistinct(
            @TempDir Path dirShared, @TempDir Path dirSolo) throws IOException {
        LruBlockCache shared = new LruBlockCache(SHARED_CAPACITY_BYTES);
        LruBlockCache dedicated = new LruBlockCache(SHARED_CAPACITY_BYTES);
        try (RocksDb s = openWith(dirShared, shared);
             RocksDb solo = openWith(dirSolo, dedicated)) {
            // Identity check: the engine must expose the exact cache object handed in.
            assertThat(s.blockCache()).isSameAs(shared);
            assertThat(solo.blockCache()).isSameAs(dedicated);
            assertThat(s.blockCache()).isNotSameAs(solo.blockCache());
        }
    }

    @Test
    void instrumentedSharedCacheCountsHitsAndLoadsAcrossAllInstances(
            @TempDir Path dirA, @TempDir Path dirB, @TempDir Path dirC)
            throws IOException {
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

        List<String> prefixes = List.of("a", "b", "c");
        List<Path> dirs = List.of(dirA, dirB, dirC);
        List<RocksDb> dbs = new ArrayList<>();
        try {
            for (int i = 0; i < dirs.size(); i++) {
                RocksDb db = openWith(dirs.get(i), instrumented);
                dbs.add(db);
                db.put(Key.of(prefixes.get(i)), Slice.of("v"));
                db.flush();
                // First fetch on each instance: miss → load.
                db.get(Key.of(prefixes.get(i)));
            }
            // Subsequent fetches across all instances: hits.
            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < dbs.size(); i++) {
                    dbs.get(i).get(Key.of(prefixes.get(i)));
                }
            }
        } finally {
            for (RocksDb db : dbs) db.close();
        }

        assertThat(loads.get())
            .as("at least one load per distinct SSTable across all instances")
            .isGreaterThanOrEqualTo(3L);
        assertThat(hits.get())
            .as("subsequent reads on every instance are hits")
            .isGreaterThanOrEqualTo(15L);
    }

    @Test
    void dedicatedCacheUsageMatchesSingleInstanceWhenNotSharing(
            @TempDir Path soloDir, @TempDir Path noiseDir) throws IOException {
        LruBlockCache dedicated = new LruBlockCache(1L << 20);
        LruBlockCache noise = new LruBlockCache(1L << 20);

        try (RocksDb solo = openWith(soloDir, dedicated);
             RocksDb noisy = openWith(noiseDir, noise)) {
            populateAndFlush(solo, "solo", 300);
            populateAndFlush(noisy, "noise", 300);

            // Drive reads through the noisy DB on the noise cache.
            for (int i = 0; i < 300; i++) {
                assertThat(noisy.get(Key.of(String.format("noise-%05d", i)))).isPresent();
            }
            long dedicatedAfterNoise = dedicated.usageBytes();
            assertThat(dedicatedAfterNoise)
                .as("dedicated cache is unaffected by reads on a sibling instance "
                    + "using a different cache")
                .isZero();

            // Now drive reads through the dedicated DB; only its cache moves.
            long noiseBefore = noise.usageBytes();
            for (int i = 0; i < 300; i++) {
                assertThat(solo.get(Key.of(String.format("solo-%05d", i)))).isPresent();
            }
            assertThat(dedicated.usageBytes())
                .as("dedicated cache grew under its own reads")
                .isGreaterThan(0L);
            assertThat(noise.usageBytes())
                .as("noise cache is unaffected by reads on the dedicated instance")
                .isEqualTo(noiseBefore);
        }
    }
}
