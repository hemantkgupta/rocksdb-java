package com.hkg.rocksdb.columnfamily;

/**
 * Per-CF choice of compaction algorithm. RocksDB ships four; this enum names
 * each. The actual picker implementation lives in the corresponding sibling
 * module ({@code rocksdb-compaction-leveled}, {@code -tiered}, {@code -fifo}).
 *
 * <ul>
 *   <li>{@link #LEVELED} — LevelDB-style fixed level sizes (K=10 fan-out).
 *       The single style available in leveldb-java.</li>
 *   <li>{@link #DYNAMIC_LEVELED} — level sizes computed from L_max. Worst-case
 *       space-amp ~13% vs ~90% for plain Leveled (FAST 2021 Table 4). The
 *       default for new CFs once CP 15 lands.</li>
 *   <li>{@link #TIERED} — size-tiered / universal. ~5× write-amp, ~45% space
 *       overhead. Good for write-heavy ingest, logging.</li>
 *   <li>{@link #FIFO} — no rewrites at all; just drop the oldest L0 file when
 *       the cap is reached. Cache-of-recent-data variant.</li>
 * </ul>
 */
public enum CompactionStyle {
    LEVELED,
    DYNAMIC_LEVELED,
    TIERED,
    FIFO
}
