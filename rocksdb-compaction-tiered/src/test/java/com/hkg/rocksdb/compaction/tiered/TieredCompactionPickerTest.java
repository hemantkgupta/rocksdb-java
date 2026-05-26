package com.hkg.rocksdb.compaction.tiered;

import com.hkg.rocksdb.common.Constants;
import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;
import com.hkg.rocksdb.manifest.VersionEdit;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the size-tiered picker: grouping by similar size, the
 * {@code MIN_MERGE_WIDTH} gate, score-based selection across multiple
 * qualifying groups, the CP-10 exclusion contract, and pick determinism.
 */
class TieredCompactionPickerTest {

    private static FileMetadata fm(long num, long sizeBytes) {
        // Tiered grouping is size-only — the user-key range is irrelevant to the
        // picker, but FileMetadata demands valid InternalKeys, so we supply a
        // single-character range parameterised by file number.
        String uk = String.valueOf((char) ('a' + (int) (num % 26)));
        return new FileMetadata(new FileNumber(num), sizeBytes,
            new InternalKey(Key.of(uk), new SequenceNumber(num), ValueType.VALUE),
            new InternalKey(Key.of(uk + "z"), new SequenceNumber(num), ValueType.VALUE));
    }

    private static Version versionWithL0(List<FileMetadata> l0) {
        List<List<FileMetadata>> levels = new ArrayList<>(Constants.MAX_LEVEL_COUNT);
        levels.add(new ArrayList<>(l0));
        for (int i = 1; i < Constants.MAX_LEVEL_COUNT; i++) levels.add(new ArrayList<>());
        return new Version(levels, 1L, 1000L, 1000L);
    }

    @Test
    void emptyVersionReturnsNoJob() {
        Version v = Version.empty();
        assertThat(new TieredCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void belowMinMergeWidthReturnsNoJob() {
        // MIN_MERGE_WIDTH = 4; three files cannot qualify.
        Version v = Version.empty();
        for (int i = 1; i <= 3; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        assertThat(new TieredCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void atMinMergeWidthSimilarFilesProducesJob() {
        Version v = Version.empty();
        for (int i = 1; i <= TieredCompactionPicker.MIN_MERGE_WIDTH; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        Optional<TieredCompactionJob> job = new TieredCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputs()).hasSize(TieredCompactionPicker.MIN_MERGE_WIDTH);
    }

    @Test
    void picksAllAvailableSimilarFiles() {
        // 6 similar files — the picker should consume all of them, not just 4.
        Version v = Version.empty();
        for (int i = 1; i <= 6; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        Optional<TieredCompactionJob> job = new TieredCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputs()).hasSize(6);
    }

    @Test
    void verySmallAndVeryLargeFilesNotGroupedTogether() {
        // 3 tiny files + 1 huge file: with SIZE_RATIO = 1.5 the huge file cannot
        // join the tiny tier, and 3 tinies are below MIN_MERGE_WIDTH, so no job.
        Version v = Version.empty();
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(1L, 1024L)));
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(2L, 1024L)));
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(3L, 1024L)));
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(4L, 1024L * 1024L * 1024L))); // 1 GiB
        assertThat(new TieredCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void mixedSizesPicksOnlyTheSimilarTier() {
        // 5 small files (1 KiB) + 2 huge files (1 GiB). The small tier is the
        // only one with >= MIN_MERGE_WIDTH similar files — it must be picked.
        Version v = Version.empty();
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(100L, 1024L * 1024L * 1024L)));
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(101L, 2L * 1024L * 1024L * 1024L)));

        Optional<TieredCompactionJob> job = new TieredCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputs()).hasSize(5);
        for (FileMetadata fm : job.get().inputs()) {
            assertThat(fm.sizeBytes()).isEqualTo(1024L);
        }
    }

    @Test
    void sizeRatioRespectedWhenGrowingGroup() {
        // 4 small files (1 KiB each, total 4 KiB) + one medium file (1 MiB).
        // The medium file fails the SIZE_RATIO test against the small tier
        // (1 MiB > 4 KiB * 1.5), so the picked group is exactly the 4 smalls.
        Version v = Version.empty();
        for (int i = 1; i <= 4; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(50L, 1024L * 1024L)));

        Optional<TieredCompactionJob> job = new TieredCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputs()).hasSize(4);
        assertThat(job.get().totalBytes()).isEqualTo(4L * 1024L);
    }

    @Test
    void excludedFilesAreHonored() {
        // 5 similar files; exclude one. The picker should still produce a job
        // covering the remaining 4 (== MIN_MERGE_WIDTH exactly).
        Version v = Version.empty();
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        List<TieredCompactionJob> jobs = new TieredCompactionPicker()
            .pickMultiple(v, Set.of(new FileNumber(3L)), 1);
        assertThat(jobs).hasSize(1);
        for (FileMetadata fm : jobs.get(0).inputs()) {
            assertThat(fm.fileNumber().value()).isNotEqualTo(3L);
        }
        assertThat(jobs.get(0).inputs()).hasSize(4);
    }

    @Test
    void excludedFilesBelowThresholdReturnsNoJob() {
        // 5 similar files; exclude two. Only 3 remain — below MIN_MERGE_WIDTH.
        Version v = Version.empty();
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        Set<FileNumber> taken = new HashSet<>();
        taken.add(new FileNumber(1L));
        taken.add(new FileNumber(2L));
        assertThat(new TieredCompactionPicker().pickMultiple(v, taken, 4)).isEmpty();
    }

    @Test
    void allFilesExcludedReturnsEmpty() {
        Version v = Version.empty();
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        Set<FileNumber> taken = new HashSet<>();
        for (FileNumber fn : v.allFileNumbers()) taken.add(fn);
        assertThat(new TieredCompactionPicker().pickMultiple(v, taken, 4)).isEmpty();
    }

    @Test
    void scoreBasedPickingFavorsLargerBacklog() {
        // Two distinct size-tiers each with 4 files: small (1 KiB) and large (1 MiB).
        // Both groups qualify; the large-tier group wins because its totalBytes is bigger.
        Version v = Version.empty();
        for (int i = 1; i <= 4; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        for (int i = 10; i <= 13; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L * 1024L)));
        }

        Optional<TieredCompactionJob> job = new TieredCompactionPicker().pick(v);
        assertThat(job).isPresent();
        // Winner must be the big-tier group: 4 * 1 MiB = 4 MiB total.
        assertThat(job.get().totalBytes()).isEqualTo(4L * 1024L * 1024L);
        for (FileMetadata fm : job.get().inputs()) {
            assertThat(fm.sizeBytes()).isEqualTo(1024L * 1024L);
        }
    }

    @Test
    void deterministicPickAcrossInvocations() {
        Version v = Version.empty();
        for (int i = 1; i <= 6; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        TieredCompactionPicker p = new TieredCompactionPicker();
        TieredCompactionJob a = p.pick(v).orElseThrow();
        TieredCompactionJob b = p.pick(v).orElseThrow();
        // Same Version, same FileNumbers, same order.
        assertThat(a.inputs()).hasSameSizeAs(b.inputs());
        for (int i = 0; i < a.inputs().size(); i++) {
            assertThat(a.inputs().get(i).fileNumber())
                .isEqualTo(b.inputs().get(i).fileNumber());
        }
    }

    @Test
    void pickMultipleProducesNonOverlappingJobs() {
        // Two clearly-separated tiers, each with 4 files. The picker should be
        // able to emit two jobs that share no file numbers.
        Version v = Version.empty();
        for (int i = 1; i <= 4; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        for (int i = 10; i <= 13; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L * 1024L)));
        }
        List<TieredCompactionJob> jobs = new TieredCompactionPicker()
            .pickMultiple(v, Set.of(), 2);

        assertThat(jobs).hasSize(2);
        Set<FileNumber> seen = new HashSet<>();
        for (TieredCompactionJob j : jobs) {
            for (FileMetadata fm : j.inputs()) {
                assertThat(seen)
                    .withFailMessage("file %s claimed by two jobs", fm.fileNumber())
                    .doesNotContain(fm.fileNumber());
                seen.add(fm.fileNumber());
            }
        }
    }

    @Test
    void pickMultipleWithMaxOneMatchesSinglePick() {
        Version v = Version.empty();
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        List<TieredCompactionJob> multi = new TieredCompactionPicker().pickMultiple(v, Set.of(), 1);
        TieredCompactionJob single = new TieredCompactionPicker().pick(v).orElseThrow();

        assertThat(multi).hasSize(1);
        assertThat(multi.get(0).inputs()).hasSameSizeAs(single.inputs());
        for (int i = 0; i < single.inputs().size(); i++) {
            assertThat(multi.get(0).inputs().get(i).fileNumber())
                .isEqualTo(single.inputs().get(i).fileNumber());
        }
    }

    @Test
    void pickMultipleStopsWhenNoMoreQualifyingGroups() {
        // One tier of 4 files — the second slot has nothing left and the
        // returned list must be exactly one job (no padding, no errors).
        Version v = Version.empty();
        for (int i = 1; i <= 4; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 1024L)));
        }
        List<TieredCompactionJob> jobs = new TieredCompactionPicker()
            .pickMultiple(v, Set.of(), 4);
        assertThat(jobs).hasSize(1);
    }

    @Test
    void jobTotalBytesMatchesSumOfInputs() {
        Version v = Version.empty();
        for (int i = 1; i <= 4; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, 2048L)));
        }
        TieredCompactionJob job = new TieredCompactionPicker().pick(v).orElseThrow();
        assertThat(job.totalBytes()).isEqualTo(4L * 2048L);
    }
}
