package com.hkg.rocksdb.compaction.tiered;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Size-tiered (Cassandra/HBase-style "universal") compaction picker.
 *
 * <p>The leveled picker keys off per-level {@code size / target} scores; the
 * tiered picker thinks in <em>runs</em> instead. A run is an SSTable
 * produced by one earlier compaction (or by the initial flush). When
 * {@link #MIN_MERGE_WIDTH} or more runs of similar size accumulate, the
 * picker selects that group to be merged into one larger output file.
 *
 * <p>"Similar size" is defined by {@link #SIZE_RATIO}: starting from the
 * smallest unconsidered file and growing the candidate group one file at a
 * time (sorted by size ascending), a candidate joins the group only while
 * {@code candidateSize <= currentGroupTotal * SIZE_RATIO}. This is the
 * standard size-tiered grouping rule (cf. LeveledDB's old Universal
 * compaction in RocksDB and Cassandra's STCS).
 *
 * <p>All files live in L0 conceptually — tiered does not use the leveled
 * hierarchy. The on-disk {@code Version.levels[][]} structure is reused but
 * we only look at L0; higher slots are unused under this style.
 *
 * <p>Reference numbers from §8 of the implementation plan: ~5× write-amp
 * vs ~16× for leveled, ~45% space overhead. The picker does not enforce
 * these — they are emergent properties of the algorithm.
 */
public final class TieredCompactionPicker {

    /** Minimum number of similar-sized files required before a merge fires. */
    public static final int MIN_MERGE_WIDTH = 4;

    /**
     * Size-similarity factor. A candidate file joins the current group iff
     * {@code candidateBytes <= groupTotalBytes * SIZE_RATIO}.
     */
    public static final double SIZE_RATIO = 1.5;

    /**
     * Convenience single-job entry point — mirrors {@code LeveledCompactionPicker.pick}.
     * Returns empty when no group of {@link #MIN_MERGE_WIDTH} similar-sized
     * files exists.
     */
    public Optional<TieredCompactionJob> pick(Version version) {
        List<TieredCompactionJob> jobs = pickMultiple(version, Set.of(), 1);
        return jobs.isEmpty() ? Optional.empty() : Optional.of(jobs.get(0));
    }

    /**
     * Pick up to {@code maxJobs} non-overlapping size-tiered merge jobs given
     * a set of file numbers that must NOT be referenced (already being
     * compacted by another worker). The returned jobs share no file numbers
     * with {@code excludedFiles} or with each other.
     *
     * <p>The CP 10 contract is reused verbatim here so the same multi-worker
     * scheduler can drive either style — the picker, not the scheduler,
     * enforces non-overlap.
     *
     * <p>Selection is score-based: among all qualifying groups, the one with
     * the largest backlog (highest {@code totalBytes()}) wins each slot.
     * Ties are broken deterministically by the smallest file number in the
     * group, so the same {@link Version} always produces the same picks.
     */
    public List<TieredCompactionJob> pickMultiple(Version version,
                                                  Set<FileNumber> excludedFiles,
                                                  int maxJobs) {
        if (maxJobs <= 0 || version.levels().isEmpty()) {
            return List.of();
        }

        Set<FileNumber> taken = new HashSet<>(excludedFiles);
        List<TieredCompactionJob> picked = new ArrayList<>(maxJobs);

        for (int slot = 0; slot < maxJobs; slot++) {
            // All qualifying groups for the current taken-set, then keep the largest.
            List<TieredCompactionJob> candidates = enumerateGroups(version, taken);
            if (candidates.isEmpty()) break;

            candidates.sort(Comparator
                .comparingLong(TieredCompactionJob::totalBytes).reversed()
                .thenComparingLong(TieredCompactionPicker::smallestFileNumber));

            TieredCompactionJob winner = candidates.get(0);
            for (FileMetadata fm : winner.inputs()) {
                taken.add(fm.fileNumber());
            }
            picked.add(winner);
        }
        return picked;
    }

    /**
     * Enumerate every qualifying size-tiered group in L0, skipping files in
     * {@code taken}. Each returned group has {@code >= MIN_MERGE_WIDTH} files
     * whose sizes obey {@link #SIZE_RATIO}. Groups may share files between
     * each other (the caller is expected to pick one, mark its files taken,
     * and re-enumerate for the next slot — see {@link #pickMultiple}).
     */
    static List<TieredCompactionJob> enumerateGroups(Version version, Set<FileNumber> taken) {
        List<FileMetadata> l0 = version.level(0);
        if (l0.size() < MIN_MERGE_WIDTH) {
            return List.of();
        }

        // Work on a size-ascending copy so the size-ratio check has a clean ordering.
        List<FileMetadata> sorted = new ArrayList<>(l0.size());
        for (FileMetadata fm : l0) {
            if (!taken.contains(fm.fileNumber())) sorted.add(fm);
        }
        if (sorted.size() < MIN_MERGE_WIDTH) {
            return List.of();
        }
        sorted.sort(Comparator
            .comparingLong(FileMetadata::sizeBytes)
            .thenComparingLong(fm -> fm.fileNumber().value()));

        List<TieredCompactionJob> groups = new ArrayList<>();
        // Try every start index — distinct starts can grow into distinct groups when
        // the size distribution has gaps. For uniformly-sized files the start-0 group
        // typically subsumes the rest; the picker's tie-breaker picks the largest.
        for (int start = 0; start + MIN_MERGE_WIDTH <= sorted.size(); start++) {
            List<FileMetadata> group = new ArrayList<>();
            long groupTotal = 0L;
            for (int j = start; j < sorted.size(); j++) {
                FileMetadata candidate = sorted.get(j);
                // Growth rule: candidate joins iff its size fits the current tier.
                // Special-case an empty group (j == start) — always accept the seed.
                if (group.isEmpty()
                    || (double) candidate.sizeBytes() <= (double) groupTotal * SIZE_RATIO) {
                    group.add(candidate);
                    groupTotal += candidate.sizeBytes();
                } else {
                    break;
                }
            }
            if (group.size() >= MIN_MERGE_WIDTH) {
                groups.add(new TieredCompactionJob(Collections.unmodifiableList(new ArrayList<>(group))));
            }
        }
        return groups;
    }

    private static long smallestFileNumber(TieredCompactionJob job) {
        long min = Long.MAX_VALUE;
        for (FileMetadata fm : job.inputs()) {
            long n = fm.fileNumber().value();
            if (n < min) min = n;
        }
        return min;
    }
}
