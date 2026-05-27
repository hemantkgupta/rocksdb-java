package com.hkg.rocksdb.errorhandling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ReadOnlyModeLatch}.
 *
 * <p>The latch is a one-way flip with an explicit operator-driven resume; each
 * lifecycle transition gets its own test. The concurrent test is a smoke
 * exercise rather than a stress test — it confirms that the volatile
 * publication contract holds under contention.
 */
final class ReadOnlyModeLatchTest {

    @Test
    void freshLatchIsNotTrippedAndReasonIsNull() {
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        assertThat(latch.isTripped()).isFalse();
        assertThat(latch.reason()).isNull();
        assertThatCode(latch::ensureWritable).doesNotThrowAnyException();
    }

    @Test
    void tripFlipsLatchAndStoresReason() {
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        latch.trip("block CRC mismatch in foo.sst");
        assertThat(latch.isTripped()).isTrue();
        assertThat(latch.reason()).isEqualTo("block CRC mismatch in foo.sst");
    }

    @Test
    void ensureWritableThrowsAfterTrip() {
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        latch.trip("EIO");
        assertThatThrownBy(latch::ensureWritable)
                .isInstanceOf(ReadOnlyModeException.class)
                .hasMessageContaining("read-only")
                .hasMessageContaining("EIO");
    }

    @Test
    void readOnlyModeExceptionCarriesReason() {
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        latch.trip("manifest tail truncated");
        try {
            latch.ensureWritable();
        } catch (ReadOnlyModeException e) {
            assertThat(e.reason()).isEqualTo("manifest tail truncated");
            return;
        }
        throw new AssertionError("expected ReadOnlyModeException");
    }

    @Test
    void attemptResumeClearsTheLatch() {
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        latch.trip("transient corruption");
        latch.attemptResume();
        assertThat(latch.isTripped()).isFalse();
        assertThat(latch.reason()).isNull();
        assertThatCode(latch::ensureWritable).doesNotThrowAnyException();
    }

    @Test
    void doubleTripIsIdempotent() {
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        latch.trip("first");
        latch.trip("first");
        assertThat(latch.isTripped()).isTrue();
        assertThat(latch.reason()).isEqualTo("first");
    }

    @Test
    void subsequentTripUpdatesReasonLatestWins() {
        // Per the ADR — operators tracing a cascade want to see the most
        // recent cause, so a follow-up trip overwrites the reason.
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        latch.trip("first cause");
        latch.trip("second cause");
        assertThat(latch.reason()).isEqualTo("second cause");
    }

    @Test
    void tripAcceptsNullReason() {
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        latch.trip(null);
        assertThat(latch.isTripped()).isTrue();
        assertThat(latch.reason()).isNull();
        assertThatThrownBy(latch::ensureWritable)
                .isInstanceOf(ReadOnlyModeException.class)
                .hasMessage("db is in read-only mode");
    }

    @Test
    void concurrentTripAndEnsureWritableIsSafe() throws InterruptedException {
        // Smoke test: many writers calling ensureWritable, while a tripper
        // flips the latch mid-flight. After the trip lands, every subsequent
        // ensureWritable must throw — no thread can observe stale "tripped
        // but no exception" state.
        ReadOnlyModeLatch latch = new ReadOnlyModeLatch();
        int readerThreads = 8;
        int iterationsPerReader = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(readerThreads + 1);
        try {
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger throwsAfterTrip = new AtomicInteger();
            AtomicInteger okBeforeTrip = new AtomicInteger();
            AtomicInteger anomalies = new AtomicInteger();
            Future<?>[] readers = new Future<?>[readerThreads];
            for (int i = 0; i < readerThreads; i++) {
                readers[i] = pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int j = 0; j < iterationsPerReader; j++) {
                        boolean wasTripped = latch.isTripped();
                        try {
                            latch.ensureWritable();
                            if (wasTripped) {
                                // Either we observed tripped=true and then it was
                                // cleared (impossible — no resumer in this test)
                                // or the volatile read saw an obsolete value.
                                anomalies.incrementAndGet();
                            } else {
                                okBeforeTrip.incrementAndGet();
                            }
                        } catch (ReadOnlyModeException ok) {
                            throwsAfterTrip.incrementAndGet();
                        }
                    }
                });
            }
            Future<?> tripper = pool.submit(() -> {
                try {
                    start.await();
                    Thread.sleep(5L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                latch.trip("concurrent trip");
            });
            start.countDown();
            tripper.get(5L, TimeUnit.SECONDS);
            for (Future<?> r : readers) {
                r.get(5L, TimeUnit.SECONDS);
            }
            assertThat(latch.isTripped()).isTrue();
            assertThat(latch.reason()).isEqualTo("concurrent trip");
            assertThat(anomalies.get())
                    .as("ensureWritable must throw whenever isTripped returns true")
                    .isZero();
            assertThat(throwsAfterTrip.get() + okBeforeTrip.get())
                    .isEqualTo(readerThreads * iterationsPerReader);
        } catch (java.util.concurrent.ExecutionException
                | java.util.concurrent.TimeoutException e) {
            throw new AssertionError("concurrent test failed", e);
        } finally {
            pool.shutdownNow();
        }
    }
}
