package com.hkg.rocksdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeyTest {

    @Test
    void compareTo_lexicographic() {
        assertThat(Key.of("a").compareTo(Key.of("b"))).isNegative();
        assertThat(Key.of("b").compareTo(Key.of("a"))).isPositive();
        assertThat(Key.of("abc").compareTo(Key.of("abc"))).isZero();
    }

    @Test
    void compareTo_shorterKeyIsLess_whenPrefixMatches() {
        assertThat(Key.of("abc").compareTo(Key.of("abcd"))).isNegative();
        assertThat(Key.of("abcd").compareTo(Key.of("abc"))).isPositive();
    }

    @Test
    void compareTo_treatsBytesAsUnsigned() {
        Key low = Key.of(new byte[] {0x01});
        Key high = Key.of(new byte[] {(byte) 0x80});
        // 0x80 > 0x01 when compared as unsigned bytes — but as signed bytes 0x80 is negative.
        // The compareTo must use unsigned interpretation (matches RocksDb and bytewise lex order).
        assertThat(low.compareTo(high)).isNegative();
        assertThat(high.compareTo(low)).isPositive();
    }

    @Test
    void equalsAndHashCode() {
        Key a = Key.of("hello");
        Key b = Key.of("hello");
        Key c = Key.of("world");
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
    }
}
