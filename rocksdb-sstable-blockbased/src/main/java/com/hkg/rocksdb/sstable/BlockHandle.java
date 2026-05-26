package com.hkg.rocksdb.sstable;

import java.nio.ByteBuffer;

/**
 * (offset, length) locator for a block within an SSTable file. Encoded as
 * two varlongs; padded to {@link #MAX_ENCODED_LENGTH} bytes in the footer.
 */
public record BlockHandle(long offset, long length) {

    /** Maximum encoded length — two 10-byte varlongs. */
    public static final int MAX_ENCODED_LENGTH = 20;

    public void writeTo(ByteBuffer buf) {
        VarInt.writeVarLong(buf, offset);
        VarInt.writeVarLong(buf, length);
    }

    public static BlockHandle readFrom(ByteBuffer buf) {
        long offset = VarInt.readVarLong(buf);
        long length = VarInt.readVarLong(buf);
        return new BlockHandle(offset, length);
    }
}
