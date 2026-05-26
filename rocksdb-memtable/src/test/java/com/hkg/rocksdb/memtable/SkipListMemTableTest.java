package com.hkg.rocksdb.memtable;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SkipListMemTableTest {

    @Test
    void putAndGet_returnsTheValue() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("foo"), Slice.of("bar"), new SequenceNumber(1L));
        Optional<SkipListMemTable.MemTableLookup> hit = mt.get(Key.of("foo"));
        assertThat(hit).isPresent();
        assertThat(hit.get().isDeletion()).isFalse();
        assertThat(hit.get().value()).isEqualTo(Slice.of("bar"));
    }

    @Test
    void getMissingKey_returnsEmpty() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("foo"), Slice.of("bar"), new SequenceNumber(1L));
        assertThat(mt.get(Key.of("missing"))).isEmpty();
    }

    @Test
    void overwrite_returnsNewestVersion() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v1"), new SequenceNumber(1L));
        mt.put(Key.of("k"), Slice.of("v2"), new SequenceNumber(2L));
        mt.put(Key.of("k"), Slice.of("v3"), new SequenceNumber(3L));
        assertThat(mt.get(Key.of("k")).orElseThrow().value()).isEqualTo(Slice.of("v3"));
    }

    @Test
    void delete_makesGetReturnTombstone() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v"), new SequenceNumber(1L));
        mt.delete(Key.of("k"), new SequenceNumber(2L));
        SkipListMemTable.MemTableLookup hit = mt.get(Key.of("k")).orElseThrow();
        assertThat(hit.isDeletion()).isTrue();
    }

    @Test
    void resurrection_afterDelete() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v1"), new SequenceNumber(1L));
        mt.delete(Key.of("k"), new SequenceNumber(2L));
        mt.put(Key.of("k"), Slice.of("v3"), new SequenceNumber(3L));
        SkipListMemTable.MemTableLookup hit = mt.get(Key.of("k")).orElseThrow();
        assertThat(hit.isDeletion()).isFalse();
        assertThat(hit.value()).isEqualTo(Slice.of("v3"));
    }

    @Test
    void snapshotRead_seesOlderVersion() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v1"), new SequenceNumber(1L));
        mt.put(Key.of("k"), Slice.of("v2"), new SequenceNumber(2L));
        mt.put(Key.of("k"), Slice.of("v3"), new SequenceNumber(3L));
        // Snapshot at sequence 2 must NOT see v3.
        SkipListMemTable.MemTableLookup atSeq2 = mt.get(Key.of("k"), new SequenceNumber(2L)).orElseThrow();
        assertThat(atSeq2.value()).isEqualTo(Slice.of("v2"));
        SkipListMemTable.MemTableLookup atSeq1 = mt.get(Key.of("k"), new SequenceNumber(1L)).orElseThrow();
        assertThat(atSeq1.value()).isEqualTo(Slice.of("v1"));
    }

    @Test
    void snapshotRead_beforeAnyWriteReturnsEmpty() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v2"), new SequenceNumber(2L));
        assertThat(mt.get(Key.of("k"), new SequenceNumber(1L))).isEmpty();
    }

    @Test
    void freeze_rejectsFurtherWrites() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v"), new SequenceNumber(1L));
        mt.freeze();
        assertThat(mt.isFrozen()).isTrue();
        assertThatThrownBy(() -> mt.put(Key.of("k2"), Slice.of("v2"), new SequenceNumber(2L)))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> mt.delete(Key.of("k"), new SequenceNumber(3L)))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void freeze_keepsReadsWorking() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v"), new SequenceNumber(1L));
        mt.freeze();
        assertThat(mt.get(Key.of("k")).orElseThrow().value()).isEqualTo(Slice.of("v"));
    }

    @Test
    void iterator_yieldsScanOrder() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("b"), Slice.of("v_b"), new SequenceNumber(1L));
        mt.put(Key.of("a"), Slice.of("v_a1"), new SequenceNumber(2L));
        mt.put(Key.of("a"), Slice.of("v_a2"), new SequenceNumber(3L));
        mt.delete(Key.of("c"), new SequenceNumber(4L));

        List<String> seen = new ArrayList<>();
        mt.iterator().forEachRemaining(e ->
            seen.add(new String(e.internalKey().userKey().bytes()) + "@" + e.internalKey().sequence().value())
        );
        // userKey ASC, sequence DESC for the same userKey.
        assertThat(seen).containsExactly("a@3", "a@2", "b@1", "c@4");
    }

    @Test
    void size_countsEveryInternalEntry() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v1"), new SequenceNumber(1L));
        mt.put(Key.of("k"), Slice.of("v2"), new SequenceNumber(2L));
        mt.delete(Key.of("k"), new SequenceNumber(3L));
        assertThat(mt.size()).isEqualTo(3);
    }

    @Test
    void approximateBytes_growsWithWrites() {
        SkipListMemTable mt = new SkipListMemTable();
        long before = mt.approximateBytes();
        mt.put(Key.of("aaaa"), Slice.of("bbbb"), new SequenceNumber(1L));
        long after = mt.approximateBytes();
        assertThat(after).isGreaterThan(before);
    }

    @Test
    void concurrentPuts_allLand() throws InterruptedException {
        SkipListMemTable mt = new SkipListMemTable();
        int threads = 8;
        int writesPerThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        java.util.concurrent.atomic.AtomicLong seqGen = new java.util.concurrent.atomic.AtomicLong(1L);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < writesPerThread; i++) {
                        long seq = seqGen.getAndIncrement();
                        mt.put(Key.of("t" + tid + "_k" + i),
                               Slice.of("v" + i),
                               new SequenceNumber(seq));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();
        assertThat(mt.size()).isEqualTo(threads * writesPerThread);
    }
}
