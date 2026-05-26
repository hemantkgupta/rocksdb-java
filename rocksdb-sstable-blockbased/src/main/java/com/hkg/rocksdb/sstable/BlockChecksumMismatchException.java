package com.hkg.rocksdb.sstable;

/**
 * Thrown when a block's CRC32 trailer doesn't match the recomputed checksum.
 * Per the impl plan, this is a panic-class error — the engine has no
 * read-only-mode latch and the embedding application must restart.
 */
public final class BlockChecksumMismatchException extends SsTableFormatException {
    public BlockChecksumMismatchException(String message) { super(message); }
}
