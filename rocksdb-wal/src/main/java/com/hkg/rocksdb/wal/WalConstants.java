package com.hkg.rocksdb.wal;

/**
 * WAL framing constants. Match RocksDb's log format exactly:
 * <pre>
 *   block := record* trailer?
 *   record := crc32(4 LE) | length(2 LE) | type(1) | data[length]
 *   blockSize = 32 KiB
 *   headerSize = 7 bytes
 * </pre>
 * If a record's header doesn't fit in the remaining bytes of the current
 * block, the remainder is zero-padded and the record starts at the next
 * block boundary. Records longer than {@code blockSize - headerSize} are
 * split into FIRST + MIDDLE* + LAST fragments.
 */
public final class WalConstants {

    private WalConstants() {}

    /** RocksDb log block size — 32 KiB. */
    public static final int BLOCK_SIZE = 32 * 1024;

    /** Per-record header size — 4 (CRC) + 2 (length) + 1 (type) = 7 bytes. */
    public static final int HEADER_SIZE = 7;

    /** Maximum payload bytes per fragment. */
    public static final int MAX_PAYLOAD_PER_FRAGMENT = BLOCK_SIZE - HEADER_SIZE;
}
