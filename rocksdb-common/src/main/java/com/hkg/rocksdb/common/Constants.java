package com.hkg.rocksdb.common;

/**
 * Compile-time constants for the RocksDb engine. Matches §12 of the
 * implementation plan. RocksDb famously has no runtime configurability;
 * these defaults are the only knobs that exist, and changing them is a
 * recompile.
 */
public final class Constants {

    private Constants() {}

    /** MemTable size at which a flush is triggered (4 MiB). */
    public static final int MEMTABLE_FLUSH_THRESHOLD_BYTES = 4 * 1024 * 1024;

    /** Target SSTable block size for the block-based table format (4 KiB). */
    public static final int BLOCK_SIZE_BYTES = 4 * 1024;

    /** Number of L0 files at which compaction is triggered. */
    public static final int L0_FILE_COUNT_TRIGGER = 4;

    /** Target byte size of L1 — each higher level is K× larger. */
    public static final long LEVEL_SIZE_BASE_BYTES = 10L * 1024L * 1024L;

    /** Per-level size multiplier (the "K" of leveled compaction). */
    public static final int LEVEL_SIZE_MULTIPLIER = 10;

    /** Maximum number of on-disk levels. */
    public static final int MAX_LEVEL_COUNT = 7;

    /** Bloom filter bits per key (~1% FPR at 10 bits/key). */
    public static final int BLOOM_BITS_PER_KEY = 10;

    /** Target bloom filter false-positive rate. */
    public static final double BLOOM_FILTER_FPP = 0.01;

    /** Default block cache size (8 MiB). */
    public static final long BLOCK_CACHE_DEFAULT_BYTES = 8L * 1024L * 1024L;

    /** Whether WAL writes call fsync per record by default. */
    public static final boolean WAL_SYNC_DEFAULT = true;

    /** Compression codec identifier for SSTable data blocks. */
    public static final String COMPRESSION_TYPE_DEFLATE = "deflate";

    /** Maximum legal sequence number (2^56 - 1). */
    public static final long MAX_SEQUENCE_NUMBER = (1L << 56) - 1L;

    /** MANIFEST log rotation threshold (4 MiB). */
    public static final long MANIFEST_ROTATION_BYTES = 4L * 1024L * 1024L;

    /** SSTable target file size used during compaction output (2 MiB). */
    public static final long SST_FILE_TARGET_SIZE_BYTES = 2L * 1024L * 1024L;

    /** L0 file count above which writes slow down. */
    public static final int L0_SLOWDOWN_WRITES_TRIGGER = 8;

    /** L0 file count above which writes stop until compaction catches up. */
    public static final int L0_STOP_WRITES_TRIGGER = 12;

    /**
     * SSTable size at which the index is written as a two-level structure
     * (ADR-0014, Phase 7 CP 29). Files at or below this threshold use a
     * single index block; files above it emit multiple bottom-level index
     * blocks plus a top-level index block, with only the top-level pinned
     * in memory by the reader.
     */
    public static final long TWO_LEVEL_INDEX_THRESHOLD = 32L * 1024L * 1024L;

    /**
     * Target size of a single bottom-level index block in the two-level
     * layout (4 KiB — same as a data block, so block-cache residency
     * accounts for index blocks the same way it does data blocks).
     */
    public static final int INDEX_BLOCK_TARGET_BYTES = 4 * 1024;
}
