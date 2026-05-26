package com.hkg.rocksdb.common;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class InternalKeyTest {

    @Test
    void newerSequenceSortsFirst_forSameUserKey() {
        Key k = Key.of("foo");
        InternalKey older = new InternalKey(k, new SequenceNumber(1L), ValueType.VALUE);
        InternalKey newer = new InternalKey(k, new SequenceNumber(5L), ValueType.VALUE);
        assertThat(newer.compareTo(older)).isNegative();
        assertThat(older.compareTo(newer)).isPositive();
    }

    @Test
    void userKeyOrderingDominatesSequence() {
        InternalKey aHighSeq = new InternalKey(Key.of("a"), new SequenceNumber(99L), ValueType.VALUE);
        InternalKey bLowSeq = new InternalKey(Key.of("b"), new SequenceNumber(0L), ValueType.VALUE);
        // "a" < "b" so aHighSeq comes first regardless of sequence
        assertThat(aHighSeq.compareTo(bLowSeq)).isNegative();
    }

    @Test
    void treeSetOrderingNewestFirstPerUserKey() {
        TreeSet<InternalKey> set = new TreeSet<>();
        Key k = Key.of("foo");
        set.add(new InternalKey(k, new SequenceNumber(1L), ValueType.VALUE));
        set.add(new InternalKey(k, new SequenceNumber(3L), ValueType.VALUE));
        set.add(new InternalKey(k, new SequenceNumber(2L), ValueType.DELETION));
        List<Long> sequences = set.stream().map(ik -> ik.sequence().value()).toList();
        assertThat(sequences).containsExactly(3L, 2L, 1L);
    }

    @Test
    void deletionAndValueWithSameSequence_tieBreakDeterministically() {
        Key k = Key.of("foo");
        InternalKey value = new InternalKey(k, new SequenceNumber(1L), ValueType.VALUE);
        InternalKey deletion = new InternalKey(k, new SequenceNumber(1L), ValueType.DELETION);
        // RocksDb orders by packed trailer DESC. Value (tag=1) packs higher than Deletion
        // (tag=0) at the same sequence, so Value sorts BEFORE Deletion. This is what makes
        // a probe at (userKey, S, VALUE) find a same-sequence tombstone via ceilingEntry.
        assertThat(value.compareTo(deletion)).isNegative();
        assertThat(deletion.compareTo(value)).isPositive();
    }
}
