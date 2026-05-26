package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;

import java.util.Objects;

/**
 * Per-SSTable metadata as carried by a {@link Version}. The smallest and
 * largest internal keys bound the file's contents — used by the read path
 * to determine which SSTable in L1+ to probe for a given user key.
 */
public record FileMetadata(FileNumber fileNumber,
                            long sizeBytes,
                            InternalKey smallestKey,
                            InternalKey largestKey) {

    public FileMetadata {
        Objects.requireNonNull(fileNumber, "fileNumber");
        Objects.requireNonNull(smallestKey, "smallestKey");
        Objects.requireNonNull(largestKey, "largestKey");
        if (sizeBytes < 0L) {
            throw new IllegalArgumentException("sizeBytes must be non-negative: " + sizeBytes);
        }
    }
}
