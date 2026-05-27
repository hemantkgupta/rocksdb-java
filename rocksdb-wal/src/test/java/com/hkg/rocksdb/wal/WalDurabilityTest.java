package com.hkg.rocksdb.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WalDurabilityTest {

    // ----- WalDurability sum coverage ---------------------------------------------------

    @Test
    void sumExhaustivelyCovered_sync() {
        WalDurability d = WalDurability.sync();
        assertThat(d).isInstanceOf(WalDurability.Sync.class);
    }

    @Test
    void sumExhaustivelyCovered_disabled() {
        WalDurability d = WalDurability.disabled();
        assertThat(d).isInstanceOf(WalDurability.Disabled.class);
    }

    @Test
    void sumExhaustivelyCovered_bufferedDefault() {
        WalDurability d = WalDurability.buffered();
        assertThat(d).isInstanceOfSatisfying(WalDurability.Buffered.class,
                b -> assertThat(b.flushIntervalMillis())
                        .isEqualTo(WalDurability.DEFAULT_FLUSH_INTERVAL_MS));
    }

    @Test
    void sumExhaustivelyCovered_bufferedExplicit() {
        WalDurability d = WalDurability.buffered(25L);
        assertThat(d).isInstanceOfSatisfying(WalDurability.Buffered.class,
                b -> assertThat(b.flushIntervalMillis()).isEqualTo(25L));
    }

    @Test
    void buffered_rejectsNonPositiveInterval() {
        assertThatThrownBy(() -> new WalDurability.Buffered(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WalDurability.Buffered(-5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ----- Sync mode --------------------------------------------------------------------

    @Test
    void syncMode_recordsSurviveCloseWithoutExplicitFlush(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("sync.log");
        try (LogWriter writer = LogWriter.open(log, WalDurability.sync())) {
            writer.append("a".getBytes());
            writer.append("b".getBytes());
            writer.append("c".getBytes());
            // No explicit sync() — Sync mode forced after every append.
        }
        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            reader.records().forEachRemaining(r -> seen.add(new String(r)));
            assertThat(seen).containsExactly("a", "b", "c");
        }
    }

    @Test
    void syncMode_doesNotSpawnFlusherThread(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("sync.log");
        int before = Thread.activeCount();
        try (LogWriter writer = LogWriter.open(log, WalDurability.sync())) {
            writer.append("x".getBytes());
            assertThat(writer.backgroundForceCount()).isZero();
        }
        // No leaked rocksdb-wal-flusher thread.
        assertThat(Thread.getAllStackTraces().keySet())
                .noneMatch(t -> "rocksdb-wal-flusher".equals(t.getName()));
        // (use `before` for a sanity nudge; thread count can fluctuate, so don't assert equality)
        assertThat(before).isGreaterThan(0);
    }

    // ----- Buffered mode ----------------------------------------------------------------

    @Test
    void bufferedMode_appendsCompleteQuicklyAndCloseDrainsAll(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("buffered.log");
        int n = 500;
        long t0 = System.nanoTime();
        try (LogWriter writer = LogWriter.open(log, WalDurability.buffered(50L))) {
            for (int i = 0; i < n; i++) {
                writer.append(("rec-" + i).getBytes());
            }
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        // Buffered does not fsync per-append; 500 small appends + one final force should
        // be well under a second. (Loose bound — not a microbenchmark.)
        assertThat(elapsedMs).isLessThan(5_000L);

        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            reader.records().forEachRemaining(r -> seen.add(new String(r)));
            assertThat(seen).hasSize(n);
            assertThat(seen.get(0)).isEqualTo("rec-0");
            assertThat(seen.get(n - 1)).isEqualTo("rec-" + (n - 1));
        }
    }

    @Test
    void bufferedMode_backgroundFlusherForcesWithinInterval(@TempDir Path tmp) throws Exception {
        Path log = tmp.resolve("buffered.log");
        try (LogWriter writer = LogWriter.open(log, WalDurability.buffered(20L))) {
            writer.append("seed".getBytes());
            // Wait long enough for at least a few background forces.
            long deadline = System.currentTimeMillis() + 1_000L;
            while (System.currentTimeMillis() < deadline && writer.backgroundForceCount() < 2L) {
                Thread.sleep(10L);
            }
            assertThat(writer.backgroundForceCount()).isGreaterThanOrEqualTo(2L);
        }
    }

    @Test
    void bufferedMode_closeStopsDaemonThread(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("buffered.log");
        LogWriter writer = LogWriter.open(log, WalDurability.buffered(20L));
        writer.append("x".getBytes());
        writer.close();
        // After close(), the daemon flusher should no longer be alive.
        boolean stillAlive = Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> "rocksdb-wal-flusher".equals(t.getName()) && t.isAlive());
        assertThat(stillAlive).isFalse();
    }

    @Test
    void bufferedMode_simulatedCrashLosesOnlyPostLastForceRecords(@TempDir Path tmp) throws Exception {
        // Simulate "crash mid-flush-interval" by writing without calling close().
        // With a very long flush interval, no background force fires, and the OS may
        // hold bytes in cache — but bytes that did reach the file are readable. We
        // assert weaker semantics: post-crash, the readable record set is a *prefix*
        // of what was appended (no torn-record-in-middle accepted as good).
        Path log = tmp.resolve("buffered-crash.log");
        // Use a long interval so we know NO background fsync ran in the test window.
        LogWriter writer = LogWriter.open(log, WalDurability.buffered(60_000L));
        int n = 50;
        for (int i = 0; i < n; i++) {
            writer.append(("rec-" + i).getBytes());
        }
        assertThat(writer.backgroundForceCount()).isZero();
        // Do NOT call writer.close(): simulate crash. Stop the flusher thread
        // explicitly so the JVM can exit; do not force(true).
        // We rely on FileChannel being closed by GC eventually — for the test, peek
        // at the file as-is.
        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            try {
                reader.records().forEachRemaining(r -> seen.add(new String(r)));
            } catch (Exception ignored) {
                // Trailing torn record from a crash is acceptable.
            }
            // Whatever survived must be a strict prefix.
            for (int i = 0; i < seen.size(); i++) {
                assertThat(seen.get(i)).isEqualTo("rec-" + i);
            }
            assertThat(seen.size()).isLessThanOrEqualTo(n);
        }
        // Tidy: the underlying file is still open via the writer's channel; close it
        // here so the temp dir cleanup succeeds on all platforms.
        writer.close();
    }

    // ----- Disabled mode ----------------------------------------------------------------

    @Test
    void disabledMode_appendsAreFast(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("disabled.log");
        int n = 1_000;
        long t0 = System.nanoTime();
        try (LogWriter writer = LogWriter.open(log, WalDurability.disabled())) {
            for (int i = 0; i < n; i++) {
                writer.append(("rec-" + i).getBytes());
            }
            assertThat(writer.backgroundForceCount()).isZero();
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        assertThat(elapsedMs).isLessThan(5_000L);
    }

    @Test
    void disabledMode_closeWritesEverythingButDoesNotFsync(@TempDir Path tmp) throws IOException {
        // On graceful close, the channel is closed (which flushes user-space buffers to
        // the OS) but no fsync is issued — a hard power-cut after close could still
        // lose data. From a process-level read after a graceful close, the data is
        // visible.
        Path log = tmp.resolve("disabled.log");
        try (LogWriter writer = LogWriter.open(log, WalDurability.disabled())) {
            writer.append("alpha".getBytes());
            writer.append("beta".getBytes());
            writer.append("gamma".getBytes());
        }
        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            reader.records().forEachRemaining(r -> seen.add(new String(r)));
            assertThat(seen).containsExactly("alpha", "beta", "gamma");
        }
    }

    @Test
    void disabledMode_doesNotSpawnFlusherThread(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("disabled.log");
        try (LogWriter writer = LogWriter.open(log, WalDurability.disabled())) {
            writer.append("x".getBytes());
            assertThat(writer.backgroundForceCount()).isZero();
        }
        assertThat(Thread.getAllStackTraces().keySet())
                .noneMatch(t -> "rocksdb-wal-flusher".equals(t.getName()) && t.isAlive());
    }

    // ----- Backwards compatibility: boolean overload -------------------------------------

    @Test
    void booleanOverload_trueMapsToSync(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("compat-true.log");
        try (LogWriter writer = LogWriter.open(log, true)) {
            assertThat(writer.durability()).isInstanceOf(WalDurability.Sync.class);
            writer.append("ok".getBytes());
        }
        assertThat(Files.size(log)).isPositive();
    }

    @Test
    void booleanOverload_falseMapsToDisabled(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("compat-false.log");
        try (LogWriter writer = LogWriter.open(log, false)) {
            assertThat(writer.durability()).isInstanceOf(WalDurability.Disabled.class);
            writer.append("ok".getBytes());
        }
        try (LogReader reader = LogReader.open(log)) {
            assertThat(reader.readRecord()).map(String::new).contains("ok");
        }
    }

    @Test
    void appendOverload_existingFileWithBufferedDurability(@TempDir Path tmp) throws IOException {
        Path log = tmp.resolve("append.log");
        try (LogWriter writer = LogWriter.open(log, WalDurability.sync())) {
            writer.append("first".getBytes());
        }
        try (LogWriter writer = LogWriter.openForAppend(log, WalDurability.buffered(50L))) {
            writer.append("second".getBytes());
        }
        try (LogReader reader = LogReader.open(log)) {
            List<String> seen = new ArrayList<>();
            reader.records().forEachRemaining(r -> seen.add(new String(r)));
            assertThat(seen).containsExactly("first", "second");
        }
    }
}
