package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Score-based compaction picker.
 *
 * <ul>
 *   <li>L0 score = {@code fileCount / L0_FILE_COUNT_TRIGGER}.</li>
 *   <li>L_n score (n &gt; 0) = {@code totalBytes / target(n)} where
 *       {@code target(n) = LEVEL_SIZE_BASE_BYTES * LEVEL_SIZE_MULTIPLIER^(n-1)}.</li>
 * </ul>
 *
 * <p>Picks the level with the highest score &gt; 1.0; returns
 * {@link Optional#empty()} if no level is over its target.
 *
 * <p>For L=0 we take all L0 files as inputs (overlapping range bookkeeping).
 * For L&gt;0 we take the first file in that level — a minimal alternative
 * to RocksDb's per-level round-robin {@code compactPointer}.
 */
public final class LeveledCompactionPicker {

    public Optional<CompactionJob> pick(Version version) {
        List<CompactionJob> jobs = pickMultiple(version, Set.of(), 1);
        return jobs.isEmpty() ? Optional.empty() : Optional.of(jobs.get(0));
    }

    /**
     * Pick up to {@code maxJobs} non-overlapping compaction jobs given a set
     * of file numbers that must NOT be referenced (because another worker is
     * already compacting them). The returned jobs share no file numbers with
     * {@code excludedFiles} or with each other.
     *
     * <p>This is the CP 10 entry point: the multi-threaded compaction
     * scheduler asks for several jobs at once, hands one to each worker, and
     * trusts the picker to have enforced non-overlap. If only one level is
     * over its score and its inputs would conflict with an excluded file,
     * the returned list is shorter than {@code maxJobs} — including empty.
     */
    public List<CompactionJob> pickMultiple(Version version,
                                             Set<FileNumber> excludedFiles,
                                             int maxJobs) {
        if (version.levels().size() < 2 || maxJobs <= 0) {
            return List.of();
        }
        Set<FileNumber> taken = new HashSet<>(excludedFiles);
        List<CompactionJob> picked = new ArrayList<>(maxJobs);

        // Each iteration looks for the highest-score level whose chosen file is not
        // already taken (by an in-flight job or a previously picked job in this pass).
        // If a level's chosen file conflicts, we try the next file in that level
        // — supports parallel L0/L1 compaction where two workers consume disjoint
        // L0 subsets at the same time.
        outer:
        for (int slot = 0; slot < maxJobs; slot++) {
            int maxLevel = Constants.MAX_LEVEL_COUNT;
            int bestLevel = -1;
            double bestScore = 1.0;
            CompactionJob bestJob = null;

            for (int level = 0; level < maxLevel - 1; level++) {
                double score = scoreFor(version, level);
                if (score <= bestScore) continue;
                CompactionJob candidate = buildJobAvoiding(version, level, taken);
                if (candidate == null) continue;
                bestScore = score;
                bestLevel = level;
                bestJob = candidate;
            }
            if (bestJob == null) break outer;
            for (FileMetadata fm : bestJob.allInputs()) {
                taken.add(fm.fileNumber());
            }
            picked.add(bestJob);
        }
        return picked;
    }

    public static double scoreFor(Version version, int level) {
        if (level == 0) {
            return (double) version.level(0).size() / (double) Constants.L0_FILE_COUNT_TRIGGER;
        }
        long target = targetForLevel(level);
        return (double) version.levelSizeBytes(level) / (double) target;
    }

    public static long targetForLevel(int level) {
        if (level <= 0) return Long.MAX_VALUE;
        long target = Constants.LEVEL_SIZE_BASE_BYTES;
        for (int i = 1; i < level; i++) {
            target *= Constants.LEVEL_SIZE_MULTIPLIER;
        }
        return target;
    }

    static CompactionJob buildJob(Version v, int level) {
        List<FileMetadata> inputs = (level == 0)
            ? new ArrayList<>(v.level(0))
            : List.of(v.level(level).get(0));
        List<FileMetadata> overlapping = findOverlapping(v.level(level + 1), inputs);
        return new CompactionJob(level, level + 1, inputs, overlapping);
    }

    /**
     * Build a job for {@code level} that avoids any file in {@code taken}.
     * <ul>
     *   <li>For L0 — gather all L0 files NOT in {@code taken} as one job; if
     *       that subset is empty, return null. The overlapping L1 files are
     *       computed from that subset's key range; if any of them is taken,
     *       return null (another worker is already compacting that L1 file).</li>
     *   <li>For L>0 — find the first file in {@code level} NOT in {@code taken}
     *       whose L+1 overlapping files are also all free; return null if no
     *       such file exists.</li>
     * </ul>
     */
    static CompactionJob buildJobAvoiding(Version v, int level, Set<FileNumber> taken) {
        if (level == 0) {
            List<FileMetadata> freeL0 = new ArrayList<>();
            for (FileMetadata fm : v.level(0)) {
                if (!taken.contains(fm.fileNumber())) freeL0.add(fm);
            }
            if (freeL0.isEmpty()) return null;
            List<FileMetadata> overlapping = findOverlapping(v.level(1), freeL0);
            for (FileMetadata fm : overlapping) {
                if (taken.contains(fm.fileNumber())) return null;
            }
            return new CompactionJob(0, 1, freeL0, overlapping);
        }
        for (FileMetadata candidate : v.level(level)) {
            if (taken.contains(candidate.fileNumber())) continue;
            List<FileMetadata> inputs = List.of(candidate);
            List<FileMetadata> overlapping = findOverlapping(v.level(level + 1), inputs);
            boolean clean = true;
            for (FileMetadata fm : overlapping) {
                if (taken.contains(fm.fileNumber())) { clean = false; break; }
            }
            if (clean) {
                return new CompactionJob(level, level + 1, inputs, overlapping);
            }
        }
        return null;
    }

    static List<FileMetadata> findOverlapping(List<FileMetadata> level, List<FileMetadata> inputs) {
        if (level.isEmpty() || inputs.isEmpty()) {
            return List.of();
        }
        byte[] minUser = inputs.get(0).smallestKey().userKey().bytes();
        byte[] maxUser = inputs.get(0).largestKey().userKey().bytes();
        for (FileMetadata fm : inputs) {
            byte[] s = fm.smallestKey().userKey().bytes();
            byte[] l = fm.largestKey().userKey().bytes();
            if (lexCompare(s, minUser) < 0) minUser = s;
            if (lexCompare(l, maxUser) > 0) maxUser = l;
        }
        List<FileMetadata> out = new ArrayList<>();
        for (FileMetadata fm : level) {
            byte[] s = fm.smallestKey().userKey().bytes();
            byte[] l = fm.largestKey().userKey().bytes();
            // Overlap test: !(l < min || s > max)
            if (lexCompare(l, minUser) >= 0 && lexCompare(s, maxUser) <= 0) {
                out.add(fm);
            }
        }
        return out;
    }

    static int lexCompare(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int d = (a[i] & 0xff) - (b[i] & 0xff);
            if (d != 0) return d;
        }
        return a.length - b.length;
    }
}
