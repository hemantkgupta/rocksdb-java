package com.hkg.rocksdb.manifest;

/**
 * Thrown when the MANIFEST file or the CURRENT pointer is unreadable. Per
 * the impl plan, this is a non-recoverable condition — RocksDb has no
 * {@code attemptResume}, and the engine refuses to open the DB.
 */
public final class ManifestCorruptionException extends RuntimeException {
    public ManifestCorruptionException(String message) { super(message); }
    public ManifestCorruptionException(String message, Throwable cause) { super(message, cause); }
}
