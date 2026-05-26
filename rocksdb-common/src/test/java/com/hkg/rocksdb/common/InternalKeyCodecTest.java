package com.hkg.rocksdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InternalKeyCodecTest {

    @Test
    void roundTripsInternalKey() {
        InternalKey ik = new InternalKey(Key.of("hello"), new SequenceNumber(42L), ValueType.VALUE);
        byte[] bytes = InternalKeyCodec.encode(ik);
        InternalKey back = InternalKeyCodec.decode(bytes);
        assertThat(back).isEqualTo(ik);
    }

    @Test
    void rawCompareMatchesInternalKeyOrdering() {
        InternalKey older = new InternalKey(Key.of("aaa"), new SequenceNumber(1L), ValueType.VALUE);
        InternalKey newer = new InternalKey(Key.of("aaa"), new SequenceNumber(10L), ValueType.VALUE);
        byte[] olderBytes = InternalKeyCodec.encode(older);
        byte[] newerBytes = InternalKeyCodec.encode(newer);
        // newer (higher seq) sorts BEFORE older — descending sequence.
        assertThat(InternalKeyCodec.compareInternalBytes(newerBytes, olderBytes)).isNegative();
        assertThat(InternalKeyCodec.compareInternalBytes(olderBytes, newerBytes)).isPositive();
        assertThat(newer.compareTo(older)).isNegative();
    }

    @Test
    void rawCompareSortsByUserKeyAscending() {
        InternalKey a = new InternalKey(Key.of("aaa"), new SequenceNumber(99L), ValueType.VALUE);
        InternalKey b = new InternalKey(Key.of("bbb"), new SequenceNumber(1L), ValueType.VALUE);
        byte[] aB = InternalKeyCodec.encode(a);
        byte[] bB = InternalKeyCodec.encode(b);
        assertThat(InternalKeyCodec.compareInternalBytes(aB, bB)).isNegative();
    }

    @Test
    void userKeyAndTagExtraction() {
        InternalKey ik = new InternalKey(Key.of("abc"), new SequenceNumber(7L), ValueType.DELETION);
        byte[] bytes = InternalKeyCodec.encode(ik);
        assertThat(InternalKeyCodec.userKeyOf(bytes)).isEqualTo("abc".getBytes());
        assertThat(InternalKeyCodec.sequenceOf(bytes)).isEqualTo(7L);
        assertThat(InternalKeyCodec.tagOf(bytes)).isEqualTo(ValueType.DELETION.tag());
    }
}
