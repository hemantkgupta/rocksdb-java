package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.integrity.FileChecksum;

import java.util.Objects;

/**
 * Per-SSTable metadata as carried by a {@link Version}. The smallest and
 * largest internal keys bound the file's contents — used by the read path
 * to determine which SSTable in L1+ to probe for a given user key.
 *
 * <p>{@code fileChecksum} is the §5 above-block integrity value (CP 19,
 * ADR 0003): XXH64 over every byte of the SSTable file, computed once at
 * write time and verified by {@code db-verify}. It is nullable on purpose
 * — pre-CP-19 MANIFEST records carry no checksum, and the decoder must
 * still produce a usable {@code FileMetadata}.
 */
public record FileMetadata(FileNumber fileNumber,
                            long sizeBytes,
                            InternalKey smallestKey,
                            InternalKey largestKey,
                            FileChecksum fileChecksum) {

    public FileMetadata {
        Objects.requireNonNull(fileNumber, "fileNumber");
        Objects.requireNonNull(smallestKey, "smallestKey");
        Objects.requireNonNull(largestKey, "largestKey");
        if (sizeBytes < 0L) {
            throw new IllegalArgumentException("sizeBytes must be non-negative: " + sizeBytes);
        }
        // fileChecksum intentionally allowed to be null — see javadoc.
    }

    /**
     * Legacy 4-arg constructor — equivalent to passing {@code null} for the
     * checksum. Retained so call sites that don't yet compute the file
     * checksum compile unchanged; new write paths should compute and pass
     * a non-null {@link FileChecksum}.
     */
    public FileMetadata(FileNumber fileNumber,
                         long sizeBytes,
                         InternalKey smallestKey,
                         InternalKey largestKey) {
        this(fileNumber, sizeBytes, smallestKey, largestKey, null);
    }
}
