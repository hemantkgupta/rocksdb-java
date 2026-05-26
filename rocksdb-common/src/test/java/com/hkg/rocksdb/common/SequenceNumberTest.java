package com.hkg.rocksdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SequenceNumberTest {

    @Test
    void next_incrementsByOne() {
        SequenceNumber s = new SequenceNumber(42L);
        assertThat(SequenceNumber.next(s).value()).isEqualTo(43L);
    }

    @Test
    void negativeRejected() {
        assertThatThrownBy(() -> new SequenceNumber(-1L))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void above56BitsRejected() {
        long overflow = (1L << 56);
        assertThatThrownBy(() -> new SequenceNumber(overflow))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void max56BitsAccepted() {
        SequenceNumber max = new SequenceNumber((1L << 56) - 1L);
        assertThat(max.value()).isEqualTo((1L << 56) - 1L);
    }

    @Test
    void compareTo_ordersByValue() {
        SequenceNumber a = new SequenceNumber(1L);
        SequenceNumber b = new SequenceNumber(2L);
        assertThat(a.compareTo(b)).isNegative();
        assertThat(b.compareTo(a)).isPositive();
        assertThat(a.compareTo(new SequenceNumber(1L))).isZero();
    }
}
