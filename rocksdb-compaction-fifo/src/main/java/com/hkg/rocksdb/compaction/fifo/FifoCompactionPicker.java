package com.hkg.rocksdb.compaction.fifo;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * FIFO compaction picker — "cache of recent data" style (§2.2, §6.3, CP 12
 * of the RocksDB plan).
 *
 * <p>Unlike leveled or tiered, FIFO never rewrites an SSTable. Every flush
 * lands in L0; the picker watches the total bytes across L0 and, when the
 * configured cap is exceeded, returns the OLDEST file for deletion. File
 * numbers are monotonically increasing per the {@link FileNumber} contract,
 * so the smallest file number identifies the oldest file. Write amplification
 * stays at ~2× (one WAL write plus one L0 flush, never rewritten); space
 * amplification is unbounded in the worst case, which is acceptable for the
 * intended workload (ephemeral cache).
 *
 * <p>FIFO has no level structure beyond L0 in this implementation — the
 * picker reads {@code version.level(0)} only. Higher levels, if populated by
 * some other code path, are intentionally ignored: they are not part of the
 * FIFO model.
 */
public final class FifoCompactionPicker {

    private final long maxBytes;

    /**
     * @param maxBytes the cap above which the oldest file is dropped; must be
     *                 non-negative. A cap of zero means "delete on every tick
     *                 if any file exists" — useful only for tests.
     */
    public FifoCompactionPicker(long maxBytes) {
        if (maxBytes < 0L) {
            throw new IllegalArgumentException("maxBytes must be non-negative: " + maxBytes);
        }
        this.maxBytes = maxBytes;
    }

    public long maxBytes() {
        return maxBytes;
    }

    /**
     * Return a deletion job for the single oldest L0 file if and only if the
     * total L0 byte count is over the cap. Empty otherwise.
     */
    public Optional<FifoDeletionJob> pick(Version version) {
        List<FifoDeletionJob> jobs = pickMultiple(version, Set.of(), 1);
        return jobs.isEmpty() ? Optional.empty() : Optional.of(jobs.get(0));
    }

    /**
     * Return up to {@code max} deletion jobs, oldest-first, stopping once the
     * cap is restored. Files whose number is in {@code excluded} are skipped
     * (mirrors the leveled picker's contract: another worker may already be
     * acting on a file). The "size restored" check accounts for both
     * already-excluded files and the bytes proposed for deletion in this pass
     * — we don't keep nominating files past the point where the cap is met.
     *
     * <p>The accounting is intentionally simple: total bytes minus excluded
     * minus picked. Excluded files are treated as not-yet-deleted (still
     * counted against the cap) because the caller may abort the in-flight
     * deletion; this is the conservative direction and matches what the
     * RocksDB plan expects (drop more rather than fewer when racing).
     */
    public List<FifoDeletionJob> pickMultiple(Version version,
                                              Set<FileNumber> excluded,
                                              int max) {
        if (max <= 0) {
            return List.of();
        }
        List<FileMetadata> l0 = version.level(0);
        if (l0.isEmpty()) {
            return List.of();
        }
        long totalBytes = 0L;
        for (FileMetadata fm : l0) {
            totalBytes += fm.sizeBytes();
        }
        if (totalBytes <= maxBytes) {
            return List.of();
        }

        // oldest-first: lower fileNumber == older.
        List<FileMetadata> oldestFirst = new ArrayList<>(l0);
        oldestFirst.sort(Comparator.comparingLong(fm -> fm.fileNumber().value()));

        Set<FileNumber> skip = new HashSet<>(excluded);
        List<FifoDeletionJob> picked = new ArrayList<>();
        long projectedTotal = totalBytes;

        for (FileMetadata fm : oldestFirst) {
            if (picked.size() >= max) break;
            if (projectedTotal <= maxBytes) break;
            if (skip.contains(fm.fileNumber())) continue;
            picked.add(new FifoDeletionJob(fm.fileNumber(), fm.sizeBytes()));
            projectedTotal -= fm.sizeBytes();
        }
        return picked;
    }
}
