package com.hkg.rocksdb.wal;

/**
 * Thrown when a WAL record fails CRC verification, has an unknown record-type
 * tag, or violates the fragment-ordering invariant (e.g., a MIDDLE without
 * a preceding FIRST).
 *
 * <p>The reader does NOT throw this on EOF or torn-write at end-of-file; those
 * are reported as normal end-of-stream conditions. Corruption is reserved for
 * genuine data damage that the caller cannot recover from without intervention.
 */
public class WalCorruptionException extends RuntimeException {
    public WalCorruptionException(String message) { super(message); }
    public WalCorruptionException(String message, Throwable cause) { super(message, cause); }
}
