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
 * Score-based picker for the <em>Dynamic Leveled</em> compaction style.
 *
 * <p>The classical LevelDB-derived {@link LeveledCompactionPicker} computes
 * per-level size targets bottom-up from a fixed base and a constant
 * multiplier ({@code target(L_n) = LEVEL_SIZE_BASE_BYTES * K^(n-1)}). FAST
 * 2021 §3 shows this produces worst-case ~90% space amplification: when the
 * actual bottom-level size lands just over its static target, the next-up
 * level is provisioned for {@code 1/K} of that target but the real data
 * above is only {@code 1/K} of the <em>actual</em> bottom, leaving levels
 * 9× over-provisioned in aggregate.
 *
 * <p>Dynamic Leveled inverts the computation:
 * <ol>
 *   <li>{@code lastNonEmptyLevel} = highest level with files.</li>
 *   <li>{@code target(lastNonEmptyLevel) =
 *       max(actualBytes(lastNonEmptyLevel), LEVEL_SIZE_BASE_BYTES)}.</li>
 *   <li>For {@code L < lastNonEmptyLevel}:
 *       {@code target(L) = target(L+1) / LEVEL_SIZE_MULTIPLIER}.</li>
 *   <li>If {@code target(L) < LEVEL_SIZE_BASE_BYTES} for {@code L > 0}, the
 *       level is "below threshold" — its score is measured against
 *       {@code LEVEL_SIZE_BASE_BYTES} so micro-DBs don't divide by tiny
 *       numbers.</li>
 *   <li>L0 score remains {@code fileCount / L0_FILE_COUNT_TRIGGER}.</li>
 * </ol>
 *
 * <p>Per FAST 2021 Table 4, this drops worst-case space overhead from ~90%
 * (LevelDB-static) to ~13% at the same write-amplification profile. This is
 * the default style for new column families — see ADR-0002.
 *
 * <p>Note on code duplication: the {@code findOverlapping} /
 * {@code lexCompare} / {@code buildJobAvoiding} helpers are copied from
 * {@link LeveledCompactionPicker} as private statics rather than extracted
 * into a shared interface. A later refactor (post-CP 18) will deduplicate.
 */
public final class DynamicLeveledCompactionPicker {

    public Optional<CompactionJob> pick(Version version) {
        List<CompactionJob> jobs = pickMultiple(version, Set.of(), 1);
        return jobs.isEmpty() ? Optional.empty() : Optional.of(jobs.get(0));
    }

    /**
     * Pick up to {@code maxJobs} non-overlapping compaction jobs. Mirrors
     * {@link LeveledCompactionPicker#pickMultiple} — see that javadoc for
     * the contract. The only difference here is the dynamic per-level
     * target computation feeding {@link #scoreFor}.
     */
    public List<CompactionJob> pickMultiple(Version version,
                                             Set<FileNumber> excludedFiles,
                                             int maxJobs) {
        if (version.levels().size() < 2 || maxJobs <= 0) {
            return List.of();
        }
        Set<FileNumber> taken = new HashSet<>(excludedFiles);
        List<CompactionJob> picked = new ArrayList<>(maxJobs);

        // Precompute targets once per pickMultiple call — they depend only on
        // the Version, not on the in-flight exclusion set.
        long[] targets = computeDynamicTargets(version);

        outer:
        for (int slot = 0; slot < maxJobs; slot++) {
            int maxLevel = Constants.MAX_LEVEL_COUNT;
            int bestLevel = -1;
            double bestScore = 1.0;
            CompactionJob bestJob = null;

            for (int level = 0; level < maxLevel - 1; level++) {
                double score = scoreFor(version, level, targets);
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

    /**
     * Compute the dynamic per-level size target for {@code level} given the
     * current Version. Convenience wrapper around {@link #computeDynamicTargets}
     * for callers that only need one level.
     */
    public static long dynamicTargetForLevel(Version v, int level) {
        if (level <= 0) return Long.MAX_VALUE;
        long[] targets = computeDynamicTargets(v);
        return targets[level];
    }

    /**
     * Compute targets for every level in one pass.
     *
     * <p>Returns an array of length {@link Constants#MAX_LEVEL_COUNT}.
     * Entry 0 is {@link Long#MAX_VALUE} (L0 is not size-scored). Entries
     * above {@code lastNonEmptyLevel} are also {@link Long#MAX_VALUE} — these
     * levels are "logically empty" and never picked on size.
     */
    static long[] computeDynamicTargets(Version v) {
        int maxLevel = Constants.MAX_LEVEL_COUNT;
        long[] targets = new long[maxLevel];
        for (int i = 0; i < maxLevel; i++) targets[i] = Long.MAX_VALUE;

        int lastNonEmpty = -1;
        for (int level = 1; level < maxLevel; level++) {
            if (level < v.levels().size() && !v.level(level).isEmpty()) {
                lastNonEmpty = level;
            }
        }
        if (lastNonEmpty <= 0) {
            // No level >= 1 has files. Nothing to size — picker will fall back
            // to L0-count scoring only.
            return targets;
        }

        long bottomBytes = v.levelSizeBytes(lastNonEmpty);
        long bottomTarget = Math.max(bottomBytes, Constants.LEVEL_SIZE_BASE_BYTES);
        targets[lastNonEmpty] = bottomTarget;

        // Back-propagate by the multiplier.
        for (int level = lastNonEmpty - 1; level >= 1; level--) {
            long parent = targets[level + 1];
            long t = parent / Constants.LEVEL_SIZE_MULTIPLIER;
            // Below-threshold floor: never score against a target smaller than
            // LEVEL_SIZE_BASE_BYTES (prevents division-by-tiny on small DBs).
            targets[level] = Math.max(t, Constants.LEVEL_SIZE_BASE_BYTES);
        }
        return targets;
    }

    public static double scoreFor(Version version, int level) {
        return scoreFor(version, level, computeDynamicTargets(version));
    }

    static double scoreFor(Version version, int level, long[] targets) {
        if (level == 0) {
            return (double) version.level(0).size() / (double) Constants.L0_FILE_COUNT_TRIGGER;
        }
        long target = targets[level];
        if (target == Long.MAX_VALUE) {
            // Logically empty (above lastNonEmptyLevel). Never picked on size.
            return 0.0;
        }
        return (double) version.levelSizeBytes(level) / (double) target;
    }

    // ---- Helpers copied from LeveledCompactionPicker. ----
    // Duplication is acceptable for now; a later refactor (after CP 18 lands)
    // will hoist these into a shared package-private helper class.

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
