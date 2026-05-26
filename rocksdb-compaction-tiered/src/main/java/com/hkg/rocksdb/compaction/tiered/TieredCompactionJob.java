package com.hkg.rocksdb.compaction.tiered;

import com.hkg.rocksdb.manifest.FileMetadata;

import java.util.List;
import java.util.Objects;

/**
 * One unit of work for the tiered (size-tiered / universal) compactor: a
 * chosen group of similarly-sized SSTables that the worker will merge into
 * one larger output file.
 *
 * <p>Tiered compaction has no leveled separation — every run logically lives
 * at L0 (the on-disk {@code levels[][]} structure is reused, but interpreted
 * as size-tiers rather than the leveled-style hierarchy). Hence there is no
 * {@code outputLevel} distinct from {@code inputLevel}; the merged output is
 * written back to the same conceptual L0, just as a single larger run.
 *
 * <p>This is the structural counterpart to
 * {@link com.hkg.rocksdb.compaction.CompactionJob} (which models the leveled
 * shape {@code inputLevel -> inputLevel + 1}) — kept in this module because
 * the {@code outputLevel == inputLevel + 1} invariant of that record would
 * be a lie for tiered.
 */
public record TieredCompactionJob(List<FileMetadata> inputs) {

    public TieredCompactionJob {
        Objects.requireNonNull(inputs, "inputs");
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs must not be empty");
        }
        // defensive copy so callers can't mutate the chosen group after the fact.
        inputs = List.copyOf(inputs);
    }

    /** Total bytes across the chosen group — used by the picker to score jobs. */
    public long totalBytes() {
        long total = 0L;
        for (FileMetadata fm : inputs) {
            total += fm.sizeBytes();
        }
        return total;
    }
}
