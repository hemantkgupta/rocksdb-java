package com.hkg.rocksdb.testcluster;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import com.hkg.rocksdb.common.Snapshot;
import com.hkg.rocksdb.engine.RocksDb;
import com.hkg.rocksdb.tools.DbVerify;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 CP 12 — randomised end-to-end stress test. Drives a mix of
 * put / delete / get / snapshot / releaseSnapshot operations on an open
 * engine, periodically crashing (close-without-flush) and reopening, with
 * occasional manual compaction passes. At the end of the run it asserts:
 *
 * <ul>
 *   <li>Every acked write is readable from the recovered engine.</li>
 *   <li>Held snapshots returned values consistent with the engine's state
 *       at snapshot time, even after subsequent overwrites/deletes.</li>
 *   <li>{@link DbVerify} reports no block-CRC corruption.</li>
 * </ul>
 *
 * <p>Default wall-time is 5 s so it runs in CI. Set the system property
 * {@code -Drocksdb.stress.duration.seconds=600} for the full 10-minute
 * mode the implementation plan §11.3 calls out.
 */
class StressTest {

    private static final long DEFAULT_DURATION_SECONDS = 5L;
    private static final long SEED =
        Long.getLong("rocksdb.stress.seed", 0xC0FFEEL);

    /**
     * One main stress driver. The state machine the test maintains:
     * <ul>
     *   <li>{@code expected}: every acked Put/Delete is reflected here.
     *       After a crash-reopen, the engine must agree with this map for
     *       every key.</li>
     *   <li>{@code liveSnapshots}: open snapshot handles with a frozen
     *       view of {@code expected} captured at snapshot time.</li>
     * </ul>
     */
    @Test
    void randomisedWorkloadSurvivesCrashAndRecoversAllAckedWrites(@TempDir Path dir)
        throws IOException {
        long durationSeconds = Long.getLong("rocksdb.stress.duration.seconds",
            DEFAULT_DURATION_SECONDS);
        long deadlineNanos = System.nanoTime() + durationSeconds * 1_000_000_000L;

        Random rng = new Random(SEED);
        Map<String, String> expected = new HashMap<>();
        // Snapshots are bound to a single engine session — kill them on crash.
        List<LiveSnapshot> liveSnapshots = new ArrayList<>();
        long ops = 0;
        long crashes = 0;
        long compactions = 0;

        RocksDb db = RocksDb.open(dir);
        try {
            while (System.nanoTime() < deadlineNanos) {
                int dice = rng.nextInt(100);
                String key = pickKey(rng);

                if (dice < 50) {
                    // put
                    String value = "v-" + ops;
                    db.put(Key.of(key), Slice.of(value));
                    expected.put(key, value);
                } else if (dice < 70) {
                    // delete
                    db.delete(Key.of(key));
                    expected.remove(key);
                } else if (dice < 92) {
                    // get + check against expected (live state, post-snapshot writes visible)
                    Optional<Slice> got = db.get(Key.of(key));
                    String want = expected.get(key);
                    if (want == null) {
                        assertThat(got).withFailMessage("expected absent key %s", key).isEmpty();
                    } else {
                        assertThat(got).withFailMessage("expected key %s present", key).isPresent();
                        assertThat(new String(got.get().toBytes())).isEqualTo(want);
                    }
                } else if (dice < 95 && liveSnapshots.size() < 4) {
                    // snapshot
                    liveSnapshots.add(new LiveSnapshot(db.snapshot(), new HashMap<>(expected)));
                } else if (dice < 97 && !liveSnapshots.isEmpty()) {
                    // release snapshot
                    LiveSnapshot s = liveSnapshots.remove(rng.nextInt(liveSnapshots.size()));
                    db.releaseSnapshot(s.handle());
                } else if (dice < 99) {
                    // snapshot read — must reflect the frozen view at snapshot time
                    if (!liveSnapshots.isEmpty()) {
                        LiveSnapshot s = liveSnapshots.get(rng.nextInt(liveSnapshots.size()));
                        Optional<Slice> got = db.get(Key.of(key), s.handle());
                        String want = s.frozen().get(key);
                        if (want == null) {
                            // The historical state had this key absent. A live tombstone could
                            // still be visible if the put happened *before* snapshot but was
                            // overwritten by a delete *after*. Engine must return the snapshot's
                            // historical value: absent.
                            assertThat(got).isEmpty();
                        } else {
                            assertThat(got).isPresent();
                            assertThat(new String(got.get().toBytes())).isEqualTo(want);
                        }
                    }
                } else {
                    // crash-and-restart cycle
                    db.closeWithoutFlush();
                    // Snapshots do not survive a crash.
                    liveSnapshots.clear();
                    db = RocksDb.open(dir);
                    crashes++;
                    // Sanity-check a few random keys are recovered.
                    assertEngineMatchesExpected(db, expected, rng, 16);
                }
                ops++;

                // Occasional manual compaction so L0 doesn't run away.
                if (ops % 200 == 0) {
                    if (db.maybeCompact()) compactions++;
                }
            }

            // Final crash-and-reopen to prove durability of every acked write.
            db.closeWithoutFlush();
            liveSnapshots.clear();
            db = RocksDb.open(dir);
            crashes++;

            assertEngineMatchesExpected(db, expected, null, Integer.MAX_VALUE);
        } finally {
            for (LiveSnapshot s : liveSnapshots) db.releaseSnapshot(s.handle());
            db.close();
        }

        // Block-CRC walk over every referenced SSTable.
        DbVerify.Report report = DbVerify.run(dir);
        assertThat(report.ok()).withFailMessage("DbVerify reported failures: %s", report.render())
            .isTrue();

        // Telemetry — useful if the test ever flakes.
        System.out.printf("StressTest: ops=%d crashes=%d compactions=%d expectedKeys=%d "
                + "files=%d entries=%d%n",
            ops, crashes, compactions, expected.size(),
            report.filesScanned(), report.blocksScanned());

        // Sanity floors — if any of these are zero, the test isn't actually stressing the engine.
        // Kept loose so the test survives CI load + the per-CP overhead growth (Phase 5 added
        // file-checksum + KV-checksum work that shows up in the per-op latency).
        assertThat(ops).isGreaterThan(100L);
        assertThat(crashes).isGreaterThanOrEqualTo(1L);
    }

    /** Verify a sample (or all) of {@code expected} against the engine. */
    private static void assertEngineMatchesExpected(RocksDb db, Map<String, String> expected,
                                                     Random rng, int sampleSize) {
        // Iterate deterministically — TreeMap gives sorted-key order regardless of insertion order.
        List<Map.Entry<String, String>> entries = new ArrayList<>(
            new TreeMap<>(expected).entrySet());
        if (sampleSize < entries.size() && rng != null) {
            // Fisher–Yates partial shuffle: pick `sampleSize` distinct entries pseudo-randomly
            // without a non-transitive comparator (which TimSort rejects).
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

    /**
     * Power-law key picker: {@code idx = floor(N * U^alpha)} with alpha = 2
     * biases toward low indices, matching the hot-key pattern Chrome
     * IndexedDB workloads exhibit.
     */
    private static String pickKey(Random rng) {
        final int domain = 2_000;
        double u = rng.nextDouble();
        int idx = (int) Math.min(domain - 1, Math.floor(domain * u * u));
        return String.format("key-%06d", idx);
    }

    private record LiveSnapshot(Snapshot handle, Map<String, String> frozen) {}
}
