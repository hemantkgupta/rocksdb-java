package com.hkg.rocksdb.sstable;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VarIntTest {

    @Test
    void roundTripsSingleBytes() {
        for (int v = 0; v < 128; v++) {
            ByteBuffer buf = ByteBuffer.allocate(10);
            VarInt.writeVarInt(buf, v);
            buf.flip();
            assertThat(VarInt.readVarInt(buf)).isEqualTo(v);
        }
    }

    @Test
    void roundTripsMultiByteValues() {
        int[] values = {128, 255, 16384, 1_000_000, Integer.MAX_VALUE};
        for (int v : values) {
            ByteBuffer buf = ByteBuffer.allocate(10);
            VarInt.writeVarInt(buf, v);
            buf.flip();
            assertThat(VarInt.readVarInt(buf)).isEqualTo(v);
        }
    }

    @Test
    void roundTripsLongValues() {
        long[] values = {0L, 1L, 1L << 35, Long.MAX_VALUE};
        for (long v : values) {
            ByteBuffer buf = ByteBuffer.allocate(10);
            VarInt.writeVarLong(buf, v);
            buf.flip();
            assertThat(VarInt.readVarLong(buf)).isEqualTo(v);
        }
    }

    @Test
    void truncatedVarIntThrows() {
        ByteBuffer buf = ByteBuffer.allocate(2);
        buf.put((byte) 0x80);
        buf.put((byte) 0x80);
        buf.flip();
        assertThatThrownBy(() -> VarInt.readVarInt(buf))
            .isInstanceOf(SsTableFormatException.class);
    }
}
