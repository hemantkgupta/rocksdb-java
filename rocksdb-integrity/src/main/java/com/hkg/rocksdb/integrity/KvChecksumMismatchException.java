package com.hkg.rocksdb.integrity;

/**
 * CP 21 — thrown when {@link KvChecksumCodec#unwrap unwrap} detects a value
 * whose stored 4-byte trailing CRC does not match the recomputed checksum.
 * The exception is the engine's signal that some layer above the disk-level
 * block CRC32 corrupted the value bytes — RAM bit rot in MemTable, block
 * cache poisoning, or in-flight corruption between SSTable reader and the
 * caller.
 *
 * <p>In CP 22 this exception will be classified by
 * {@code SeverityClassifier} as {@code HARD_ERROR_READ_ONLY}, tripping the
 * engine's read-only mode latch.
 */
public class KvChecksumMismatchException extends RuntimeException {

    public KvChecksumMismatchException(String message) {
        super(message);
    }
}
