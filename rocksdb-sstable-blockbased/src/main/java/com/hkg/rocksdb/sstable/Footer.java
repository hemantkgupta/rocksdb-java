package com.hkg.rocksdb.sstable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The 48-byte fixed-size SSTable footer:
 * <pre>
 *   metaIndexHandle (varlong+varlong, padded to 20 B)
 *   indexHandle     (varlong+varlong, padded to 20 B)
 *   magic           (8 B LE = 0xDB4775248B80FB57)
 * </pre>
 * Magic lifted verbatim from the RocksDb source so DB files written by a
 * faithful port are byte-identical at the footer.
 */
public record Footer(BlockHandle metaIndexHandle, BlockHandle indexHandle) {

    public static final int ENCODED_LENGTH = 2 * BlockHandle.MAX_ENCODED_LENGTH + 8;
    public static final long MAGIC = 0xdb4775248b80fb57L;

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(ENCODED_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        int start = buf.position();
        metaIndexHandle.writeTo(buf);
        while (buf.position() - start < BlockHandle.MAX_ENCODED_LENGTH) {
            buf.put((byte) 0);
        }
        int idxStart = buf.position();
        indexHandle.writeTo(buf);
        while (buf.position() - idxStart < BlockHandle.MAX_ENCODED_LENGTH) {
            buf.put((byte) 0);
        }
        buf.putLong(MAGIC);
        return buf.array();
    }

    public static Footer decode(byte[] data) {
        if (data.length != ENCODED_LENGTH) {
            throw new SsTableFormatException(
                "footer length=" + data.length + " expected=" + ENCODED_LENGTH);
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        BlockHandle meta = BlockHandle.readFrom(buf);
        buf.position(BlockHandle.MAX_ENCODED_LENGTH);
        BlockHandle index = BlockHandle.readFrom(buf);
        buf.position(2 * BlockHandle.MAX_ENCODED_LENGTH);
        long magic = buf.getLong();
        if (magic != MAGIC) {
            throw new SsTableFormatException(
                "footer magic mismatch: got=0x" + Long.toHexString(magic));
        }
        return new Footer(meta, index);
    }
}
