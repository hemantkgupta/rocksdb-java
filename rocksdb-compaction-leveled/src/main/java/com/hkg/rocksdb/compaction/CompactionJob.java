package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.manifest.FileMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One unit of work for the {@link Compactor}: a chosen set of files at
 * {@code inputLevel} together with any overlapping files in
 * {@code outputLevel} that must be rewritten alongside them.
 */
public record CompactionJob(int inputLevel,
                             int outputLevel,
                             List<FileMetadata> inputs,
                             List<FileMetadata> overlapping) {

    public CompactionJob {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(overlapping, "overlapping");
        if (inputLevel < 0) {
            throw new IllegalArgumentException("inputLevel must be >= 0: " + inputLevel);
        }
        if (outputLevel != inputLevel + 1) {
            throw new IllegalArgumentException(
                "outputLevel must be inputLevel + 1 (got " + inputLevel + " -> " + outputLevel + ")");
        }
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs must not be empty");
        }
    }

    /** Convenience — all input + overlapping files in one list. */
    public List<FileMetadata> allInputs() {
        List<FileMetadata> all = new ArrayList<>(inputs.size() + overlapping.size());
        all.addAll(inputs);
        all.addAll(overlapping);
        return all;
    }
}
