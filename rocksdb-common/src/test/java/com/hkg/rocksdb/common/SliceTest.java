package com.hkg.rocksdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SliceTest {

    @Test
    void of_byteArray_roundTrip() {
        byte[] in = {1, 2, 3, 4};
        Slice s = Slice.of(in);
        assertThat(s.length()).isEqualTo(4);
        assertThat(s.toBytes()).containsExactly(1, 2, 3, 4);
    }

    @Test
    void of_string_roundTripUtf8() {
        Slice s = Slice.of("abc");
        assertThat(s.length()).isEqualTo(3);
        assertThat(s.get(0)).isEqualTo((byte) 'a');
        assertThat(s.get(1)).isEqualTo((byte) 'b');
        assertThat(s.get(2)).isEqualTo((byte) 'c');
    }

    @Test
    void equalsCoversWindow_notFullBacking() {
        byte[] backing = {0, 1, 2, 3, 4, 5};
        Slice a = new Slice(backing, 1, 3);   // {1,2,3}
        Slice b = Slice.of(new byte[] {1, 2, 3});
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void notEqual_whenWindowDiffers() {
        Slice a = Slice.of(new byte[] {1, 2, 3});
        Slice b = Slice.of(new byte[] {1, 2, 4});
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void notEqual_whenLengthDiffers() {
        Slice a = Slice.of(new byte[] {1, 2, 3});
        Slice b = Slice.of(new byte[] {1, 2});
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void get_outOfRange_throws() {
        Slice s = Slice.of(new byte[] {1, 2, 3});
        assertThatThrownBy(() -> s.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> s.get(3)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void toBytes_returnsCopy() {
        Slice s = Slice.of(new byte[] {1, 2, 3});
        byte[] copy = s.toBytes();
        copy[0] = 99;
        // Mutating the copy must not affect the slice.
        assertThat(s.get(0)).isEqualTo((byte) 1);
    }

    @Test
    void constructor_rejectsInvalidWindow() {
        byte[] backing = {1, 2, 3};
        assertThatThrownBy(() -> new Slice(backing, -1, 1))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Slice(backing, 0, 4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Slice(backing, 2, 2))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
