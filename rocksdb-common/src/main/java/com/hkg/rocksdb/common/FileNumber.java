package com.hkg.rocksdb.common;

/**
 * Monotonically increasing identifier assigned to each on-disk file an
 * engine writes (SSTables, WALs, MANIFESTs). The {@link #value()} is the
 * raw 64-bit integer; helper methods produce RocksDb's canonical filename
 * patterns:
 *
 * <ul>
 *   <li>{@code 000123.sst} — SSTable</li>
 *   <li>{@code 000123.log} — WAL</li>
 *   <li>{@code MANIFEST-000123} — MANIFEST log</li>
 * </ul>
 */
public record FileNumber(long value) {

    public FileNumber {
        if (value < 0L) {
            throw new IllegalArgumentException("file number must be non-negative, got " + value);
        }
    }

    public String tableFileName() {
        return String.format("%06d.sst", value);
    }

    public String logFileName() {
        return String.format("%06d.log", value);
    }

    public String manifestFileName() {
        return String.format("MANIFEST-%06d", value);
    }
}
