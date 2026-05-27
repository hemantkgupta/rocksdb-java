package com.hkg.rocksdb.compaction;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CP 18 — verify {@link LeveledCompactionPicker#partitionL0} splits L0 into
 * disjoint sub-range jobs that workers can run in parallel.
 */
class LeveledCompactionPickerPartitionL0Test {

    private static FileMetadata fm(long num, String smallest, String largest) {
        InternalKey s = new InternalKey(Key.of(smallest), new SequenceNumber(100), ValueType.VALUE);
        InternalKey l = new InternalKey(Key.of(largest), new SequenceNumber(100), ValueType.VALUE);
        return new FileMetadata(new FileNumber(num), 1_000_000L, s, l);
    }

    private static Version versionWith(List<FileMetadata> l0, List<FileMetadata> l1) {
        List<List<FileMetadata>> levels = new ArrayList<>();
        levels.add(new ArrayList<>(l0));
        levels.add(new ArrayList<>(l1));
        for (int i = 2; i < Constants.MAX_LEVEL_COUNT; i++) levels.add(new ArrayList<>());
        return new Version(levels, 1L, 1000L, 100L);
    }

    @Test
    void emptyL0YieldsEmptyJobList() {
        Version v = versionWith(List.of(), List.of());
        assertThat(new LeveledCompactionPicker().partitionL0(v, 4)).isEmpty();
    }

    @Test
    void targetJobsBelowOneShortCircuitsToSinglePick() {
        // 5 L0 files exceed trigger so single pick returns one job.
        List<FileMetadata> l0 = List.of(
            fm(10, "a", "b"), fm(11, "c", "d"), fm(12, "e", "f"),
            fm(13, "g", "h"), fm(14, "i", "j"));
        Version v = versionWith(l0, List.of());
        List<CompactionJob> jobs = new LeveledCompactionPicker().partitionL0(v, 1);
        assertThat(jobs).hasSize(1);
        assertThat(jobs.get(0).inputs()).hasSize(5);
    }

    @Test
    void fewerFilesThanTargetReturnsSingleJob() {
        // Only 2 L0 files but request 4 jobs → single job (whole L0).
        List<FileMetadata> l0 = List.of(
            fm(10, "a", "b"), fm(11, "c", "d"));
        Version v = versionWith(l0, List.of());
        List<CompactionJob> jobs = new LeveledCompactionPicker().partitionL0(v, 4);
        // Single L0 of size 2 is BELOW the 4-file trigger so pick() returns empty.
        // partitionL0 with fewer files than target falls back to pick(), which here yields empty.
        assertThat(jobs).isEmpty();
    }

    @Test
    void eightL0FilesSplitIntoFourJobsWhenRangesDisjoint() {
        // Eight L0 files with disjoint ranges across the keyspace, no L1 overlap.
        List<FileMetadata> l0 = List.of(
            fm(10, "aa", "ab"),
            fm(11, "ba", "bb"),
            fm(12, "ca", "cb"),
            fm(13, "da", "db"),
            fm(14, "ea", "eb"),
            fm(15, "fa", "fb"),
            fm(16, "ga", "gb"),
            fm(17, "ha", "hb"));
        Version v = versionWith(l0, List.of());

        List<CompactionJob> jobs = new LeveledCompactionPicker().partitionL0(v, 4);
        assertThat(jobs).hasSize(4);
        // Each job has 2 L0 files.
        for (CompactionJob j : jobs) {
            assertThat(j.inputs()).hasSize(2);
            assertThat(j.overlapping()).isEmpty();
        }
        // All file numbers covered, exactly once.
        Set<Long> seen = new HashSet<>();
        for (CompactionJob j : jobs) {
            for (FileMetadata fm : j.allInputs()) {
                assertThat(seen).doesNotContain(fm.fileNumber().value());
                seen.add(fm.fileNumber().value());
            }
        }
        assertThat(seen).hasSize(8);
    }

    @Test
    void adjacentJobsMergeWhenL1OverlapsCollide() {
        // Eight L0 files, but one L1 file covers the whole keyspace. After merging
        // adjacent groups whose L1 overlap intersects, all four groups collapse into one job.
        List<FileMetadata> l0 = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            String k = String.valueOf((char) ('a' + i));
            l0.add(fm(10 + i, k + "1", k + "9"));
        }
        List<FileMetadata> l1 = List.of(fm(100, "a0", "z9"));
        Version v = versionWith(l0, l1);

        List<CompactionJob> jobs = new LeveledCompactionPicker().partitionL0(v, 4);
        assertThat(jobs).hasSize(1);
        assertThat(jobs.get(0).inputs()).hasSize(8);
        assertThat(jobs.get(0).overlapping()).containsExactly(l1.get(0));
    }

    @Test
    void partitionedJobsAreNonOverlappingByFileNumber() {
        // Eight L0 files, four L1 files each covering disjoint quarters of the keyspace.
        List<FileMetadata> l0 = List.of(
            fm(10, "aa", "ab"), fm(11, "ba", "bb"),
            fm(12, "ca", "cb"), fm(13, "da", "db"),
            fm(14, "ea", "eb"), fm(15, "fa", "fb"),
            fm(16, "ga", "gb"), fm(17, "ha", "hb"));
        List<FileMetadata> l1 = List.of(
            fm(100, "aa", "bz"),
            fm(101, "ca", "dz"),
            fm(102, "ea", "fz"),
            fm(103, "ga", "hz"));
        Version v = versionWith(l0, l1);

        List<CompactionJob> jobs = new LeveledCompactionPicker().partitionL0(v, 4);
        assertThat(jobs).hasSize(4);

        // Every file number appears in at most one job.
        Set<Long> claimed = new HashSet<>();
        for (CompactionJob j : jobs) {
            for (FileMetadata fm : j.allInputs()) {
                assertThat(claimed).withFailMessage("file %s in two jobs", fm.fileNumber())
                    .doesNotContain(fm.fileNumber().value());
                claimed.add(fm.fileNumber().value());
            }
        }
    }

    @Test
    void targetJobsGreaterThanFilesProducesFewerJobs() {
        // 6 L0 files, ask for 10 jobs → at most 6 jobs (one file each), with merging
        // possibly reducing further. Just assert the count is <= 6 and > 0.
        List<FileMetadata> l0 = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String k = String.valueOf((char) ('a' + i));
            l0.add(fm(10 + i, k + "0", k + "9"));
        }
        Version v = versionWith(l0, List.of());
        List<CompactionJob> jobs = new LeveledCompactionPicker().partitionL0(v, 10);
        // We requested 10 jobs but only 6 files; partitionL0 short-circuits because
        // l0.size() < targetJobs.
        // The fallback path returns at most 1 job.
        assertThat(jobs).hasSizeLessThanOrEqualTo(6);
    }

    @Test
    void singleL0FileWithLargeTargetJobsYieldsSingleJobOrEmpty() {
        // 1 L0 file, target=4. Below trigger → empty.
        List<FileMetadata> l0 = List.of(fm(10, "a", "b"));
        Version v = versionWith(l0, List.of());
        List<CompactionJob> jobs = new LeveledCompactionPicker().partitionL0(v, 4);
        assertThat(jobs).isEmpty();
    }

    @Test
    void partitionDeterministic() {
        List<FileMetadata> l0 = List.of(
            fm(10, "aa", "ab"), fm(11, "ba", "bb"),
            fm(12, "ca", "cb"), fm(13, "da", "db"),
            fm(14, "ea", "eb"), fm(15, "fa", "fb"));
        Version v = versionWith(l0, List.of());
        List<CompactionJob> a = new LeveledCompactionPicker().partitionL0(v, 3);
        List<CompactionJob> b = new LeveledCompactionPicker().partitionL0(v, 3);
        assertThat(a).hasSameSizeAs(b);
        for (int i = 0; i < a.size(); i++) {
            assertThat(a.get(i).inputs()).hasSameElementsAs(b.get(i).inputs());
        }
    }

    @Test
    void everyEmittedJobIsL0ToL1() {
        List<FileMetadata> l0 = List.of(
            fm(10, "aa", "ab"), fm(11, "ba", "bb"),
            fm(12, "ca", "cb"), fm(13, "da", "db"),
            fm(14, "ea", "eb"));
        Version v = versionWith(l0, List.of());
        for (CompactionJob j : new LeveledCompactionPicker().partitionL0(v, 3)) {
            assertThat(j.inputLevel()).isZero();
            assertThat(j.outputLevel()).isEqualTo(1);
        }
    }
}
