package com.hkg.rocksdb.sstable;

import java.nio.ByteBuffer;

/**
 * Length-prefixed varint encoding used throughout the SSTable format —
 * 7 bits of payload per byte, MSB indicates "more bytes follow." The same
 * encoding RocksDb and Protobuf use; we hand-roll it to keep the no-deps rule.
 */
public final class VarInt {

    private VarInt() {}

    public static void writeVarInt(ByteBuffer buf, int v) {
        writeVarLong(buf, ((long) v) & 0xFFFFFFFFL);
    }

    public static int readVarInt(ByteBuffer buf) {
        long v = readVarLong(buf);
        if (v < 0 || v > 0xFFFFFFFFL) {
            throw new SsTableFormatException("varint value out of int range: " + v);
        }
        return (int) v;
    }

    public static void writeVarLong(ByteBuffer buf, long v) {
        while ((v & ~0x7FL) != 0L) {
            buf.put((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        buf.put((byte) (v & 0x7F));
    }

    public static long readVarLong(ByteBuffer buf) {
        long result = 0L;
        int shift = 0;
        while (true) {
            if (!buf.hasRemaining()) {
                throw new SsTableFormatException("varint truncated");
            }
            byte b = buf.get();
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 63) {
                throw new SsTableFormatException("varint too long");
            }
        }
    }
}
