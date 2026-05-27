package com.hkg.rocksdb.memtable;

import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.KeyLookup;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.Slice;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CP 17 — MemTable-level RangeTombstone semantics. The engine-level
 * {@code DeleteRangeTest} covers the end-to-end contract; these tests
 * pin down the MemTable's own RT bookkeeping.
 */
class RangeTombstoneMemTableTest {

    @Test
    void rtCoversKeyInRangeMakesItTombstoned() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v"), new SequenceNumber(1));
        mt.deleteRange(Key.of("k"), Key.of("l"), new SequenceNumber(2));
        assertThat(mt.lookup(Key.of("k"), null)).isInstanceOf(KeyLookup.Tombstoned.class);
    }

    @Test
    void laterPutBeatsEarlierRangeTombstone() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v1"), new SequenceNumber(1));
        mt.deleteRange(Key.of("k"), Key.of("l"), new SequenceNumber(2));
        mt.put(Key.of("k"), Slice.of("v2"), new SequenceNumber(3));
        KeyLookup r = mt.lookup(Key.of("k"), null);
        assertThat(r).isInstanceOf(KeyLookup.Found.class);
        assertThat(new String(((KeyLookup.Found) r).value().toBytes())).isEqualTo("v2");
    }

    @Test
    void rtOutsideKeyRangeLeavesIt() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v"), new SequenceNumber(1));
        mt.deleteRange(Key.of("a"), Key.of("b"), new SequenceNumber(2));
        assertThat(mt.lookup(Key.of("k"), null)).isInstanceOf(KeyLookup.Found.class);
    }

    @Test
    void snapshotBeforeRtSeesOldValue() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.put(Key.of("k"), Slice.of("v"), new SequenceNumber(10));
        mt.deleteRange(Key.of("k"), Key.of("l"), new SequenceNumber(20));
        // Snapshot at seq 15: post-Put, pre-RT.
        KeyLookup r = mt.lookup(Key.of("k"), new SequenceNumber(15));
        assertThat(r).isInstanceOf(KeyLookup.Found.class);
    }

    @Test
    void rangeTombstoneCountTracksAddRange() {
        SkipListMemTable mt = new SkipListMemTable();
        assertThat(mt.rangeTombstoneCount()).isZero();
        mt.deleteRange(Key.of("a"), Key.of("b"), new SequenceNumber(1));
        mt.deleteRange(Key.of("c"), Key.of("d"), new SequenceNumber(2));
        assertThat(mt.rangeTombstoneCount()).isEqualTo(2);
    }

    @Test
    void maxCoveringRtSeqAtOrBeforeReturnsHighestVisible() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.deleteRange(Key.of("a"), Key.of("z"), new SequenceNumber(5));
        mt.deleteRange(Key.of("k"), Key.of("p"), new SequenceNumber(10));
        mt.deleteRange(Key.of("a"), Key.of("z"), new SequenceNumber(20));
        // Snapshot at seq 15: sees the seq=10 + seq=5 RTs but not seq=20.
        assertThat(mt.maxCoveringRtSeqAtOrBefore(Key.of("m"), 15L)).isEqualTo(10L);
        // Snapshot at seq 25: sees all three; the seq=20 RT wins.
        assertThat(mt.maxCoveringRtSeqAtOrBefore(Key.of("m"), 25L)).isEqualTo(20L);
        // Key outside coverage: -1.
        assertThat(mt.maxCoveringRtSeqAtOrBefore(Key.of("Z"), 25L)).isEqualTo(-1L);
    }

    @Test
    void freezeRejectsRangeTombstoneWrites() {
        SkipListMemTable mt = new SkipListMemTable();
        mt.deleteRange(Key.of("a"), Key.of("b"), new SequenceNumber(1));
        mt.freeze();
        try {
            mt.deleteRange(Key.of("c"), Key.of("d"), new SequenceNumber(2));
            org.junit.jupiter.api.Assertions.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // ok
        }
    }
}
