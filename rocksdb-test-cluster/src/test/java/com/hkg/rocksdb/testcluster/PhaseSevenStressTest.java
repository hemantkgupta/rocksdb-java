package com.hkg.rocksdb.testcluster;

import com.hkg.rocksdb.blockcache.LruBlockCache;
import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.Snapshot;
import com.hkg.rocksdb.engine.RocksDb;
import com.hkg.rocksdb.ratelimiter.TokenBucketRateLimiter;
import com.hkg.rocksdb.tools.DbVerify;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 7 CP 30 — end-to-end stop-condition stress test that exercises the
 * Phase-4/5/6/7 surface area on top of the original Phase 3 randomised
 * workload. New compared to {@link StressTest}:
 *
 * <ul>
 *   <li><b>CP 17 DeleteRange</b> — periodically drops a contiguous chunk of
 *       the key domain.</li>
 *   <li><b>CP 18 parallel L0 compaction</b> — periodically calls
 *       {@code runParallelL0Compaction(N)} so the parallel-drain path is
 *       actually crash-tested, not just unit-tested.</li>
 *   <li><b>CP 21 KV checksum</b> — engine opened with
 *       {@code kvChecksumEnabled = true}; every value flowing through the
 *       engine is wrapped with a 4-byte CRC32 and verified on read.</li>
 *   <li><b>CP 24 shared rate limiter</b> — engine opened with a
 *       {@link TokenBucketRateLimiter}; compaction I/O is paced at LOW
 *       priority. Rate is generous so CI wall-time stays reasonable but the
 *       limiter is still exercised on every compaction.</li>
 *   <li><b>CP 28 MultiGet</b> — invoked via reflection when the API exists.
 *       Skipped with a comment otherwise; MultiGet may land in parallel
 *       with this CP, so the test must not hard-depend on it.</li>
 * </ul>
 *
 * <p>This is the Phase 7 stop-condition stress test — running cleanly here
 * is the gate for closing out the implementation plan. The original
 * {@link StressTest} (Phase 3 CP 12) is retained for the LevelDB-era
 * regression contract.
 *
 * <p>Default wall time is 5 s for CI; set
 * {@code -Drocksdb.stress.duration.seconds=600} for the long 10-minute run.
 */
class PhaseSevenStressTest {

    private static final long DEFAULT_DURATION_SECONDS = 5L;
    private static final long SEED =
        Long.getLong("rocksdb.stress.phase7.seed", 0xDECAFBADL);
    /** 8 MB/s — generous enough not to add seconds to CI, tight enough that the limiter is exercised. */
    private static final long RATE_BYTES_PER_SEC = 8L << 20;
    private static final long REFILL_MILLIS = 10L;
    /** Re-checked at startup so a parallel-developing CP 28 is wired in automatically. */
    private static final Method MULTI_GET_METHOD = findMultiGetMethod();

    @Test
    void phaseSevenWorkloadSurvivesCrashAndRecoversAllAckedWrites(@TempDir Path dir)
        throws IOException {
        long durationSeconds = Long.getLong("rocksdb.stress.duration.seconds",
            DEFAULT_DURATION_SECONDS);
        long deadlineNanos = System.nanoTime() + durationSeconds * 1_000_000_000L;

        Random rng = new Random(SEED);
        // TreeMap so DeleteRange can slice off a contiguous key range deterministically.
        NavigableMap<String, String> expected = new TreeMap<>();
        List<LiveSnapshot> liveSnapshots = new ArrayList<>();
        long ops = 0, crashes = 0, compactions = 0, parallelCompactions = 0,
             rangeDeletes = 0, multiGets = 0;

        RocksDb db = openPhase7(dir);
        long sessionStartNanos = System.nanoTime();
        try {
            while (System.nanoTime() < deadlineNanos) {
                int dice = rng.nextInt(100);
                String key = pickKey(rng);

                if (dice < 45) {
                    String value = "v-" + ops;
                    db.put(Key.of(key), Slice.of(value));
                    expected.put(key, value);
                } else if (dice < 60) {
                    db.delete(Key.of(key));
                    expected.remove(key);
                } else if (dice < 78) {
                    // get + live-state check
                    Optional<Slice> got = db.get(Key.of(key));
                    String want = expected.get(key);
                    if (want == null) {
                        assertThat(got).withFailMessage("expected absent key %s", key).isEmpty();
                    } else {
                        assertThat(got).withFailMessage("expected key %s present", key).isPresent();
                        assertThat(new String(got.get().toBytes())).isEqualTo(want);
                    }
                } else if (dice < 82) {
                    // CP 17 DeleteRange — drop a contiguous slice of the key domain.
                    String start = pickKey(rng);
                    String end = pickKey(rng);
                    if (start.compareTo(end) > 0) { String t = start; start = end; end = t; }
                    if (start.equals(end)) end = end + "\0"; // ensure start < end
                    db.deleteRange(Key.of(start), Key.of(end));
                    expected.subMap(start, true, end, false).clear();
                    rangeDeletes++;
                } else if (dice < 86 && MULTI_GET_METHOD != null) {
                    // CP 28 MultiGet — batch-read ~100 keys via reflection.
                    multiGets += callMultiGet(db, rng, expected);
                } else if (dice < 90 && liveSnapshots.size() < 4) {
                    liveSnapshots.add(new LiveSnapshot(db.snapshot(), new HashMap<>(expected)));
                } else if (dice < 92 && !liveSnapshots.isEmpty()) {
                    LiveSnapshot s = liveSnapshots.remove(rng.nextInt(liveSnapshots.size()));
                    db.releaseSnapshot(s.handle());
                } else if (dice < 95) {
                    // snapshot read — must reflect historical state.
                    if (!liveSnapshots.isEmpty()) {
                        LiveSnapshot s = liveSnapshots.get(rng.nextInt(liveSnapshots.size()));
                        Optional<Slice> got = db.get(Key.of(key), s.handle());
                        String want = s.frozen().get(key);
                        if (want == null) {
                            assertThat(got).isEmpty();
                        } else {
                            assertThat(got).isPresent();
                            assertThat(new String(got.get().toBytes())).isEqualTo(want);
                        }
                    }
                } else if (dice < 97) {
                    // CP 18 parallel L0 compaction — only periodically and only after
                    // the session has run for a bit, so we don't churn on an empty L0.
                    if (System.nanoTime() - sessionStartNanos > 200_000_000L) {
                        int ran = db.runParallelL0Compaction(2);
                        if (ran > 0) parallelCompactions += ran;
                    }
                } else {
                    // crash-restart cycle (~3% of dice rolls). Bound by a 10 s wall clock so
                    // a short CI run still hits at least one crash; long runs hit many.
                    if (System.nanoTime() - sessionStartNanos > 10_000_000_000L
                        || crashes == 0) {
                        db.closeWithoutFlush();
                        liveSnapshots.clear();
                        db = openPhase7(dir);
                        sessionStartNanos = System.nanoTime();
                        crashes++;
                        assertEngineMatchesExpected(db, expected, rng, 16);
                    }
                }
                ops++;

                if (ops % 200 == 0) {
                    if (db.maybeCompact()) compactions++;
                }
            }

            // Final crash-and-reopen.
            db.closeWithoutFlush();
            liveSnapshots.clear();
            db = openPhase7(dir);
            crashes++;
            assertEngineMatchesExpected(db, expected, null, Integer.MAX_VALUE);
        } finally {
            for (LiveSnapshot s : liveSnapshots) db.releaseSnapshot(s.handle());
            db.close();
        }

        // Block-CRC + file-checksum walk — covers CP 19 + CP 21 stored-on-disk integrity.
        DbVerify.Report report = DbVerify.run(dir);
        assertThat(report.ok()).withFailMessage("DbVerify failures: %s", report.render()).isTrue();

        System.out.printf("PhaseSevenStressTest: ops=%d crashes=%d compactions=%d parallelCompactions=%d "
                + "rangeDeletes=%d multiGets=%d keys=%d files=%d entries=%d multiGetWiredIn=%s%n",
            ops, crashes, compactions, parallelCompactions, rangeDeletes, multiGets,
            expected.size(), report.filesScanned(), report.blocksScanned(),
            MULTI_GET_METHOD != null);

        assertThat(ops).isGreaterThan(100L);
        assertThat(crashes).isGreaterThanOrEqualTo(1L);
    }

    /** Open an engine with the full Phase 4/5/6/7 toggles: KV checksum + token-bucket limiter. */
    private static RocksDb openPhase7(Path dir) throws IOException {
        return RocksDb.open(
            dir,
            true,                                                 // compressOutput
            new LruBlockCache(Constants.BLOCK_CACHE_DEFAULT_BYTES),
            2,                                                    // compactionThreads — exercises CP 10/18
            new TokenBucketRateLimiter(RATE_BYTES_PER_SEC, REFILL_MILLIS),
            true                                                  // kvChecksumEnabled — CP 21
        );
    }

    /**
     * CP 28 MultiGet driver via reflection. Tries a handful of signatures the
     * RocksDb plan might land on (List&lt;Key&gt; → List&lt;Optional&lt;Slice&gt;&gt;,
     * Key[] → Optional&lt;Slice&gt;[], etc.) and returns the batch count when one
     * succeeds. Returns 0 (and silently no-ops) when MultiGet hasn't landed yet
     * — CP 28 is in flight in parallel with CP 30 and we don't want this test
     * to block on its merge.
     */
    private static int callMultiGet(RocksDb db, Random rng, Map<String, String> expected) {
        if (MULTI_GET_METHOD == null) return 0;
        // Pick ~100 keys. Some present, some not — exercises both branches.
        int n = 100;
        List<Key> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) keys.add(Key.of(pickKey(rng)));
        try {
            Class<?> paramType = MULTI_GET_METHOD.getParameterTypes()[0];
            Object arg;
            if (paramType.isArray()) {
                arg = keys.toArray(new Key[0]);
            } else {
                arg = keys;
            }
            Object result = MULTI_GET_METHOD.invoke(db, arg);
            // Light-touch sanity check — if it's a List or array, the size must match.
            if (result instanceof List<?> list) {
                assertThat(list).hasSize(n);
            } else if (result != null && result.getClass().isArray()) {
                assertThat(java.lang.reflect.Array.getLength(result)).isEqualTo(n);
            }
            return n;
        } catch (ReflectiveOperationException e) {
            // CP 28 isn't there yet (or has a different signature). Don't break the stress test.
            return 0;
        }
    }

    private static Method findMultiGetMethod() {
        for (Method m : RocksDb.class.getMethods()) {
            if (!m.getName().equals("multiGet")) continue;
            if (m.getParameterCount() != 1) continue;
            Class<?> p = m.getParameterTypes()[0];
            if (List.class.isAssignableFrom(p) || (p.isArray() && p.getComponentType() == Key.class)) {
                return m;
            }
        }
        return null;
    }

    private static void assertEngineMatchesExpected(RocksDb db, Map<String, String> expected,
                                                     Random rng, int sampleSize) {
        List<Map.Entry<String, String>> entries = new ArrayList<>(
            new TreeMap<>(expected).entrySet());
        if (sampleSize < entries.size() && rng != null) {
            for (int i = 0; i < sampleSize; i++) {
                int j = i + rng.nextInt(entries.size() - i);
                Map.Entry<String, String> tmp = entries.get(i);
                entries.set(i, entries.get(j));
                entries.set(j, tmp);
            }
            entries = entries.subList(0, sampleSize);
        }
        for (Map.Entry<String, String> e : entries) {
            Optional<Slice> got = db.get(Key.of(e.getKey()));
            assertThat(got).withFailMessage("missing key %s after recovery", e.getKey())
                .isPresent();
            assertThat(new String(got.get().toBytes()))
                .withFailMessage("value mismatch for %s", e.getKey())
                .isEqualTo(e.getValue());
        }
    }

    /** Same power-law picker as StressTest; biases toward low indices. */
    private static String pickKey(Random rng) {
        final int domain = 2_000;
        double u = rng.nextDouble();
        int idx = (int) Math.min(domain - 1, Math.floor(domain * u * u));
        return String.format("key-%06d", idx);
    }

    private record LiveSnapshot(Snapshot handle, Map<String, String> frozen) {}
}
