package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
        if (version.levels().size() < 2) {
            return Optional.empty();
        }
        double bestScore = 1.0;
        int bestLevel = -1;
        int maxLevel = Constants.MAX_LEVEL_COUNT;
        for (int level = 0; level < maxLevel - 1; level++) {
            double score = scoreFor(version, level);
            if (score > bestScore) {
                bestScore = score;
                bestLevel = level;
            }
        }
        if (bestLevel < 0) {
            return Optional.empty();
        }
        return Optional.of(buildJob(version, bestLevel));
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
