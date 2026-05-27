package com.hkg.rocksdb.compaction;

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
 * Tests for {@link DynamicLeveledCompactionPicker}. The picker computes
 * per-level targets from the actual size of the bottom non-empty level
 * rather than from the LevelDB-static {@code base × K^(n-1)} formula, so
 * these tests focus on:
 * <ul>
 *   <li>The standard L0 / Lk &gt; 1 scoring contract still works.</li>
 *   <li>Intermediate-level targets shrink with the bottom level — the
 *       adversarial "bottom-level partially full" workload from FAST 2021 §3
 *       triggers compaction earlier than the static picker would.</li>
 *   <li>The "below threshold" floor prevents micro-DBs from picking levels
 *       whose actual bytes are well under the floor.</li>
 *   <li>Multi-job + exclusion semantics match the CP 10 contract.</li>
 * </ul>
 */
class DynamicLeveledCompactionPickerTest {

    private static FileMetadata fm(long n, String smallUserKey, String largeUserKey, long size) {
        return new FileMetadata(new FileNumber(n), size,
            new InternalKey(Key.of(smallUserKey), new SequenceNumber(n), ValueType.VALUE),
            new InternalKey(Key.of(largeUserKey), new SequenceNumber(n), ValueType.VALUE));
    }

    private static Version versionWithLevels(List<List<FileMetadata>> perLevel) {
        List<List<FileMetadata>> levels = new ArrayList<>();
        for (int i = 0; i < Constants.MAX_LEVEL_COUNT; i++) {
            levels.add(i < perLevel.size() ? new ArrayList<>(perLevel.get(i)) : new ArrayList<>());
        }
        return new Version(levels, 1L, 1000L, 100L);
    }

    @Test
    void emptyVersionReturnsNoJob() {
        Version v = Version.empty();
        assertThat(new DynamicLeveledCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void noPickWhenL0BelowTrigger() {
        Version v = Version.empty();
        for (int i = 1; i <= 3; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, "a", "z", 1024L)));
        }
        assertThat(new DynamicLeveledCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void picksL0WhenFileCountExceedsTrigger() {
        Version v = Version.empty();
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, "a", "z", 1024L)));
        }
        Optional<CompactionJob> job = new DynamicLeveledCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputLevel()).isEqualTo(0);
        assertThat(job.get().outputLevel()).isEqualTo(1);
        assertThat(job.get().inputs()).hasSize(5);
    }

    @Test
    void bottomLevelTargetEqualsItsActualBytesWhenAboveFloor() {
        // L3 holds 300MB (well above the 10MB floor) — dynamic target(3) == 300MB.
        long l3Bytes = 300L * 1024L * 1024L;
        Version v = versionWithLevels(List.of(
            List.of(),
            List.of(),
            List.of(),
            List.of(fm(30L, "a", "z", l3Bytes))));
        assertThat(DynamicLeveledCompactionPicker.dynamicTargetForLevel(v, 3)).isEqualTo(l3Bytes);
    }

    @Test
    void intermediateTargetsBackPropagateByMultiplier() {
        // lastNonEmpty=3 with 300MB. target(3)=300MB, target(2)=30MB,
        // target(1)=3MB but floored to LEVEL_SIZE_BASE_BYTES (10MB).
        long l3Bytes = 300L * 1024L * 1024L;
        Version v = versionWithLevels(List.of(
            List.of(),
            List.of(),
            List.of(),
            List.of(fm(30L, "a", "z", l3Bytes))));
        assertThat(DynamicLeveledCompactionPicker.dynamicTargetForLevel(v, 3)).isEqualTo(l3Bytes);
        assertThat(DynamicLeveledCompactionPicker.dynamicTargetForLevel(v, 2))
            .isEqualTo(l3Bytes / Constants.LEVEL_SIZE_MULTIPLIER);
        // 30MB / 10 = 3MB, floored up to 10MB.
        assertThat(DynamicLeveledCompactionPicker.dynamicTargetForLevel(v, 1))
            .isEqualTo(Constants.LEVEL_SIZE_BASE_BYTES);
    }

    @Test
    void adversarialBottomLevelTriggersIntermediateEarlierThanStatic() {
        // FAST 2021 §3 adversarial case: bottom level is ~30% of LevelDB-static
        // target. Static target(3) = base * K^2 = 10MB * 100 = 1GB.
        // Place L3 = 300MB (30% of static), L2 = 50MB.
        // Static target(2) = 100MB → L2 score = 0.5 (no pick).
        // Dynamic target(2) = 300MB / 10 = 30MB → L2 score = 1.67 (PICK).
        long l3Bytes = 300L * 1024L * 1024L;
        long l2Bytes = 50L * 1024L * 1024L;
        Version v = versionWithLevels(List.of(
            List.of(),
            List.of(),
            List.of(fm(20L, "a", "m", l2Bytes)),
            List.of(fm(30L, "a", "z", l3Bytes))));

        // Static picker would NOT pick L2.
        double staticL2 = (double) l2Bytes / (double) LeveledCompactionPicker.targetForLevel(2);
        assertThat(staticL2).isLessThan(1.0);

        // Dynamic picker DOES pick L2.
        Optional<CompactionJob> job = new DynamicLeveledCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputLevel()).isEqualTo(2);
    }

    @Test
    void smallIntermediateLevelNotPickedWhenBelowDynamicTarget() {
        // lastNonEmpty=3 with 300MB. dynamic target(1)=10MB (floored).
        // L1 holds just 5MB — below floor → score < 1 → not picked.
        long l3Bytes = 300L * 1024L * 1024L;
        long l1Bytes = 5L * 1024L * 1024L;
        Version v = versionWithLevels(List.of(
            List.of(),
            List.of(fm(10L, "a", "c", l1Bytes)),
            List.of(),
            List.of(fm(30L, "a", "z", l3Bytes))));
        // No level is over its dynamic target.
        assertThat(new DynamicLeveledCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void belowThresholdFloorPreventsTinyDbPick() {
        // lastNonEmpty=2 with 100KB (tiny). Without the floor, target(1) = 10KB
        // and any file at L1 would over-fire. With the floor at 10MB, L1 has
        // to actually hold >10MB to be picked.
        long l2Bytes = 100L * 1024L; // 100KB
        long l1Bytes = 1L * 1024L * 1024L; // 1MB — way below 10MB floor.
        Version v = versionWithLevels(List.of(
            List.of(),
            List.of(fm(10L, "a", "c", l1Bytes)),
            List.of(fm(20L, "d", "z", l2Bytes))));
        assertThat(DynamicLeveledCompactionPicker.dynamicTargetForLevel(v, 1))
            .isEqualTo(Constants.LEVEL_SIZE_BASE_BYTES);
        assertThat(new DynamicLeveledCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void levelsAboveLastNonEmptyHaveInfiniteTarget() {
        // lastNonEmpty=3. target(4), target(5), target(6) all MAX_VALUE.
        Version v = versionWithLevels(List.of(
            List.of(), List.of(), List.of(),
            List.of(fm(30L, "a", "z", 100L * 1024L * 1024L))));
        assertThat(DynamicLeveledCompactionPicker.dynamicTargetForLevel(v, 4))
            .isEqualTo(Long.MAX_VALUE);
        assertThat(DynamicLeveledCompactionPicker.dynamicTargetForLevel(v, 5))
            .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void determinismSameVersionSamePick() {
        // Same Version → same job (same level, same input file numbers).
        long l3Bytes = 300L * 1024L * 1024L;
        long l2Bytes = 50L * 1024L * 1024L;
        Version v = versionWithLevels(List.of(
            List.of(),
            List.of(),
            List.of(fm(20L, "a", "m", l2Bytes)),
            List.of(fm(30L, "a", "z", l3Bytes))));

        Optional<CompactionJob> j1 = new DynamicLeveledCompactionPicker().pick(v);
        Optional<CompactionJob> j2 = new DynamicLeveledCompactionPicker().pick(v);
        assertThat(j1).isPresent();
        assertThat(j2).isPresent();
        assertThat(j1.get().inputLevel()).isEqualTo(j2.get().inputLevel());
        List<Long> n1 = new ArrayList<>();
        for (FileMetadata fm : j1.get().inputs()) n1.add(fm.fileNumber().value());
        List<Long> n2 = new ArrayList<>();
        for (FileMetadata fm : j2.get().inputs()) n2.add(fm.fileNumber().value());
        assertThat(n1).isEqualTo(n2);
    }

    @Test
    void pickMultipleHonorsExcludedFiles() {
        // 5 L0 files, exclude one — picker covers the remaining four.
        List<FileMetadata> l0 = List.of(
            fm(10L, "a", "b", 1_000_000L),
            fm(11L, "c", "d", 1_000_000L),
            fm(12L, "e", "f", 1_000_000L),
            fm(13L, "g", "h", 1_000_000L),
            fm(14L, "i", "j", 1_000_000L));
        Version v = versionWithLevels(List.of(l0));

        List<CompactionJob> jobs = new DynamicLeveledCompactionPicker().pickMultiple(
            v, Set.of(new FileNumber(10L)), 4);

        assertThat(jobs).hasSize(1);
        for (FileMetadata fm : jobs.get(0).inputs()) {
            assertThat(fm.fileNumber().value()).isNotEqualTo(10L);
        }
        assertThat(jobs.get(0).inputs()).hasSize(4);
    }

    @Test
    void pickMultipleReturnsEmptyWhenAllFilesExcluded() {
        List<FileMetadata> l0 = List.of(
            fm(10L, "a", "b", 1_000_000L),
            fm(11L, "c", "d", 1_000_000L),
            fm(12L, "e", "f", 1_000_000L),
            fm(13L, "g", "h", 1_000_000L));
        Version v = versionWithLevels(List.of(l0));
        Set<FileNumber> taken = new HashSet<>();
        for (FileMetadata fm : l0) taken.add(fm.fileNumber());

        assertThat(new DynamicLeveledCompactionPicker().pickMultiple(v, taken, 4)).isEmpty();
    }

    @Test
    void pickMultipleProducesNonOverlappingJobs() {
        // Both L0 (5 files) and L2 (over dynamic target) should produce TWO disjoint jobs.
        List<FileMetadata> l0 = List.of(
            fm(10L, "a", "b", 1_000_000L),
            fm(11L, "c", "d", 1_000_000L),
            fm(12L, "e", "f", 1_000_000L),
            fm(13L, "g", "h", 1_000_000L),
            fm(14L, "i", "j", 1_000_000L));
        long l3Bytes = 300L * 1024L * 1024L;
        long l2Bytes = 50L * 1024L * 1024L;
        List<FileMetadata> l2 = List.of(fm(20L, "k", "m", l2Bytes));
        List<FileMetadata> l3 = List.of(fm(30L, "a", "z", l3Bytes));
        Version v = versionWithLevels(List.of(l0, List.of(), l2, l3));

        List<CompactionJob> jobs = new DynamicLeveledCompactionPicker().pickMultiple(v, Set.of(), 3);

        assertThat(jobs).isNotEmpty();
        Set<FileNumber> seen = new HashSet<>();
        for (CompactionJob j : jobs) {
            for (FileMetadata fm : j.allInputs()) {
                assertThat(seen).withFailMessage("file %s claimed twice", fm.fileNumber())
                    .doesNotContain(fm.fileNumber());
                seen.add(fm.fileNumber());
            }
        }
    }

    @Test
    void pickMultipleSingleJobMatchesPick() {
        List<FileMetadata> l0 = List.of(
            fm(10L, "a", "b", 1_000_000L),
            fm(11L, "c", "d", 1_000_000L),
            fm(12L, "e", "f", 1_000_000L),
            fm(13L, "g", "h", 1_000_000L),
            fm(14L, "i", "j", 1_000_000L));
        Version v = versionWithLevels(List.of(l0));

        List<CompactionJob> multi = new DynamicLeveledCompactionPicker().pickMultiple(v, Set.of(), 1);
        CompactionJob single = new DynamicLeveledCompactionPicker().pick(v).orElseThrow();

        assertThat(multi).hasSize(1);
        assertThat(multi.get(0).inputLevel()).isEqualTo(single.inputLevel());
        assertThat(multi.get(0).inputs()).hasSameSizeAs(single.inputs());
    }

    @Test
    void agreesWithStaticPickerWhenBottomMatchesStaticTarget() {
        // When the bottom level exactly matches its LevelDB-static target,
        // the dynamic per-level computation reproduces the static targets, so
        // both pickers see the same scores and pick the same level.
        //
        // Set L3 = static target(3) (1GB) and L1 = 2 * static target(1) (20MB).
        // Static: target(1)=10MB → L1 score = 2.0 → picks L1.
        // Dynamic: target(3)=1GB → target(2)=100MB → target(1)=10MB → L1 score = 2.0 → picks L1.
        long l3Bytes = LeveledCompactionPicker.targetForLevel(3);
        long l1Bytes = 2L * LeveledCompactionPicker.targetForLevel(1);
        Version v = versionWithLevels(List.of(
            List.of(),
            List.of(fm(10L, "a", "c", l1Bytes)),
            List.of(),
            List.of(fm(30L, "a", "z", l3Bytes))));

        Optional<CompactionJob> staticJob = new LeveledCompactionPicker().pick(v);
        Optional<CompactionJob> dynJob = new DynamicLeveledCompactionPicker().pick(v);
        assertThat(staticJob).isPresent();
        assertThat(dynJob).isPresent();
        assertThat(dynJob.get().inputLevel()).isEqualTo(staticJob.get().inputLevel());
    }
}
