package com.hkg.rocksdb.sstable;

import com.hkg.rocksdb.bloom.BloomFilter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Serialisation of a {@link BloomFilter} as the SSTable filter meta-block.
 * Layout (LE): {@code numHashes:4 | numBits:4 | bitsLen:4 | bits[bitsLen]}.
 *
 * <p>RocksDb's original filter block is byte-identical for compatibility
 * with C++ readers; this port stores the same three parameters but with a
 * simpler framing — there is no on-disk compatibility goal here.
 */
public final class BloomBlock {

    private BloomBlock() {}

    public static byte[] encode(BloomFilter filter) {
        byte[] bits = filter.rawBits();
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 4 + bits.length).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(filter.numHashes());
        buf.putInt(filter.numBits());
        buf.putInt(bits.length);
        buf.put(bits);
        return buf.array();
    }

    public static BloomFilter decode(byte[] data) {
        if (data.length < 12) {
            throw new SsTableFormatException("bloom block too short: " + data.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int numHashes = buf.getInt();
        int numBits = buf.getInt();
        int bitsLen = buf.getInt();
        if (bitsLen < 0 || bitsLen > buf.remaining()) {
            throw new SsTableFormatException("bloom bitsLen invalid: " + bitsLen);
        }
        byte[] bits = new byte[bitsLen];
        buf.get(bits);
        return BloomFilter.fromRaw(bits, numBits, numHashes);
    }
}
