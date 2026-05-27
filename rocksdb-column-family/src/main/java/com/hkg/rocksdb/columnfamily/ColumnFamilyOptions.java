package com.hkg.rocksdb.columnfamily;

import java.util.Objects;

/**
 * Per-CF configuration. RocksDB's real {@code ColumnFamilyOptions} carries
 * dozens of knobs; this minimal form ships only what the engine actually
 * branches on today — the {@link CompactionStyle} choice plus a FIFO-only
 * size cap that the leveled / tiered paths ignore.
 *
 * <p>Future CPs add: memtable size override, bloom-bits-per-key override,
 * user-defined-timestamp size, and the other §12 constants becoming per-CF.
 */
public record ColumnFamilyOptions(CompactionStyle compactionStyle,
                                    long fifoMaxBytes) {

    public ColumnFamilyOptions {
        Objects.requireNonNull(compactionStyle, "compactionStyle");
        if (fifoMaxBytes < 0L) {
            throw new IllegalArgumentException("fifoMaxBytes must be non-negative, got " + fifoMaxBytes);
        }
    }

    /** Default options: Leveled compaction, FIFO cap unused. */
    public static ColumnFamilyOptions defaults() {
        return new ColumnFamilyOptions(CompactionStyle.LEVELED, 0L);
    }

    public static ColumnFamilyOptions leveled() {
        return new ColumnFamilyOptions(CompactionStyle.LEVELED, 0L);
    }

    public static ColumnFamilyOptions tiered() {
        return new ColumnFamilyOptions(CompactionStyle.TIERED, 0L);
    }

    public static ColumnFamilyOptions fifo(long maxBytes) {
        return new ColumnFamilyOptions(CompactionStyle.FIFO, maxBytes);
    }
}
