package com.hkg.rocksdb.integrity;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * CP 21 — KV checksum: a 4-byte CRC32 computed over {@code (userKey || value)}
 * and appended to the value bytes at engine {@code put()} time. The wrapped
 * value flows opaque-byte-by-opaque-byte through every storage layer (WAL,
 * MemTable, SSTable, block cache, compaction) and is verified + stripped at
 * the engine's read boundary.
 *
 * <p>This is the FAST 2021 §5 end-to-end checksum that catches what
 * block-CRC32 cannot:
 * <ul>
 *   <li>Bit-flip in MemTable (RAM bit rot) — detected at read time.</li>
 *   <li>Block cache poisoning — same.</li>
 *   <li>In-flight corruption between the SSTable reader and the engine API.</li>
 * </ul>
 *
 * <p>The 40% replication-propagation number §5 cites is exactly this class
 * of failure: corruption above the disk-layer integrity boundary slips past
 * block-CRC32 and gets replicated as "correct" data. The KV checksum closes
 * that gap.
 *
 * <p>Format: {@code wrapped = value || crc32(userKey || value):4 LE}.
 * Per-value overhead is a flat 4 bytes. The sequence number is deliberately
 * NOT in the CRC: it lets the read path verify without threading the seq of
 * the original write through every layer's lookup API. The trade-off is
 * that two writes with the same (userKey, value) at different seqs hash
 * identically — accepted, because the goal is catching bit-flips, not
 * detecting replay.
 */
public final class KvChecksumCodec {

    private KvChecksumCodec() {}

    /** Number of trailing checksum bytes appended to every wrapped value. */
    public static final int CHECKSUM_BYTES = 4;

    /**
     * Wrap a value with its KV checksum. Returns a new byte array of length
     * {@code value.length + 4}; the trailing 4 bytes are the CRC32 of
     * {@code (userKey || value)}, little-endian.
     */
    public static byte[] wrap(byte[] userKey, byte[] value) {
        byte[] out = new byte[value.length + CHECKSUM_BYTES];
        System.arraycopy(value, 0, out, 0, value.length);
        int crc = computeCrc(userKey, value);
        ByteBuffer.wrap(out, value.length, CHECKSUM_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(crc);
        return out;
    }

    /**
     * Verify a wrapped value's checksum and return the user-visible bytes.
     * Throws {@link KvChecksumMismatchException} if the trailing CRC does
     * not match the recomputed value (RAM bit rot, cache poisoning, or any
     * above-disk corruption that the block CRC missed).
     */
    public static byte[] unwrap(byte[] userKey, byte[] wrapped) {
        if (wrapped == null || wrapped.length < CHECKSUM_BYTES) {
            throw new KvChecksumMismatchException(
                "wrapped value too short to carry KV checksum (len="
                    + (wrapped == null ? "null" : wrapped.length) + ")");
        }
        int valueLen = wrapped.length - CHECKSUM_BYTES;
        int storedCrc = ByteBuffer.wrap(wrapped, valueLen, CHECKSUM_BYTES)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt();
        byte[] value = Arrays.copyOf(wrapped, valueLen);
        int actualCrc = computeCrc(userKey, value);
        if (storedCrc != actualCrc) {
            throw new KvChecksumMismatchException(
                "KV checksum mismatch for key (len=" + userKey.length + ")"
                    + ": stored=" + Integer.toHexString(storedCrc)
                    + " actual=" + Integer.toHexString(actualCrc));
        }
        return value;
    }

    private static int computeCrc(byte[] userKey, byte[] value) {
        CRC32 crc = new CRC32();
        crc.update(userKey, 0, userKey.length);
        crc.update(value, 0, value.length);
        return (int) crc.getValue();
    }
}
