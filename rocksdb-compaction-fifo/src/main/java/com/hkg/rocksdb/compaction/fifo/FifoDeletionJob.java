package com.hkg.rocksdb.compaction.fifo;

import com.hkg.rocksdb.common.FileNumber;

/**
 * An order to delete one SSTable under FIFO compaction.
 *
 * <p>FIFO is the "cache of recent data" compaction style described in §2.2
 * and §6.3 of the RocksDB plan: there is no merge step, no overlap
 * computation, and no output file. When the total DB size exceeds the
 * configured cap, the oldest L0 SSTable is unlinked. Because no bytes are
 * ever rewritten, FIFO's write amplification is ~2× (one WAL write plus
 * one L0 flush).
 *
 * <p>This is deliberately NOT a {@code CompactionJob}: the leveled / tiered
 * pickers emit a job with inputs and overlapping outputs that the engine
 * passes through a merging iterator. FIFO emits a deletion order — the
 * engine removes {@code file} from the MANIFEST and unlinks the file.
 *
 * @param file       the SSTable to delete (lives in L0 — FIFO has no other levels)
 * @param sizeBytes  size of the deleted file, useful for callers that want to
 *                   track reclaimed bytes against the configured cap
 */
public record FifoDeletionJob(FileNumber file, long sizeBytes) {

    public FifoDeletionJob {
        if (file == null) {
            throw new IllegalArgumentException("file must not be null");
        }
        if (sizeBytes < 0L) {
            throw new IllegalArgumentException("sizeBytes must be non-negative: " + sizeBytes);
        }
    }
}
