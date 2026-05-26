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
 * Cover the picker's CP 10 contract: produce multiple non-overlapping
 * compaction jobs given a Version + an exclusion set of in-flight file
 * numbers. The single-job {@link LeveledCompactionPicker#pick(Version)}
 * helper is exercised elsewhere; this suite focuses on the multi-job
 * scheduler.
 */
class LeveledCompactionPickerPickMultipleTest {

    private static FileMetadata l0File(long num, String smallest, String largest) {
        InternalKey small = new InternalKey(Key.of(smallest), new SequenceNumber(100), ValueType.VALUE);
        InternalKey large = new InternalKey(Key.of(largest), new SequenceNumber(100), ValueType.VALUE);
        return new FileMetadata(new FileNumber(num), 1_000_000L, small, large);
    }

    private static Version versionWith(List<FileMetadata> l0, List<FileMetadata> l1) {
        List<List<FileMetadata>> levels = new ArrayList<>();
        levels.add(new ArrayList<>(l0));
        levels.add(new ArrayList<>(l1));
        for (int i = 2; i < Constants.MAX_LEVEL_COUNT; i++) levels.add(new ArrayList<>());
        // Bump lastSequence so the Version isn't considered empty.
        return new Version(levels, 1L, 1000L, 100L);
    }

    @Test
    void pickMultipleHonorsExcludedFiles() {
        // L0 has 5 files (above the count trigger). With file 10 excluded, the picker should
        // still produce a job covering the remaining four.
        List<FileMetadata> l0 = List.of(
            l0File(10, "a", "b"),
            l0File(11, "c", "d"),
            l0File(12, "e", "f"),
            l0File(13, "g", "h"),
            l0File(14, "i", "j"));
        Version v = versionWith(l0, List.of());

        List<CompactionJob> jobs = new LeveledCompactionPicker().pickMultiple(
            v, Set.of(new FileNumber(10)), 4);

        assertThat(jobs).hasSize(1);
        CompactionJob j = jobs.get(0);
        // Excluded file must NOT be in the inputs.
        for (FileMetadata fm : j.inputs()) {
            assertThat(fm.fileNumber().value()).isNotEqualTo(10L);
        }
        assertThat(j.inputs()).hasSize(4); // the four non-excluded L0 files
    }

    @Test
    void pickMultipleReturnsEmptyWhenAllFilesExcluded() {
        List<FileMetadata> l0 = List.of(
            l0File(10, "a", "b"),
            l0File(11, "c", "d"),
            l0File(12, "e", "f"),
            l0File(13, "g", "h"));
        Version v = versionWith(l0, List.of());
        Set<FileNumber> taken = new HashSet<>();
        for (FileMetadata fm : l0) taken.add(fm.fileNumber());

        List<CompactionJob> jobs = new LeveledCompactionPicker().pickMultiple(v, taken, 4);
        assertThat(jobs).isEmpty();
    }

    @Test
    void pickMultipleProducesNonOverlappingJobs() {
        // L0 over trigger AND L1 over its target — picker should produce TWO disjoint jobs.
        // Build L1 with enough bytes to be over target.
        List<FileMetadata> l0 = List.of(
            l0File(10, "a", "b"),
            l0File(11, "c", "d"),
            l0File(12, "e", "f"),
            l0File(13, "g", "h"));
        // L1 with 12 large non-overlapping files — each ~1MB, total over 10MB target.
        List<FileMetadata> l1 = new ArrayList<>();
        long target = LeveledCompactionPicker.targetForLevel(1);
        long perFileSize = (target / 5) + 1024;
        char a = 'k';
        for (int i = 0; i < 12; i++) {
            String key = String.valueOf((char) (a + i));
            InternalKey s = new InternalKey(Key.of(key), new SequenceNumber(50), ValueType.VALUE);
            InternalKey e = new InternalKey(Key.of(key + "z"), new SequenceNumber(50), ValueType.VALUE);
            l1.add(new FileMetadata(new FileNumber(50 + i), perFileSize, s, e));
        }
        Version v = versionWith(l0, l1);

        List<CompactionJob> jobs = new LeveledCompactionPicker().pickMultiple(v, Set.of(), 3);

        // Verify whatever jobs the picker emits, they share no file numbers.
        assertThat(jobs).isNotEmpty();
        Set<FileNumber> seen = new HashSet<>();
        for (CompactionJob j : jobs) {
            for (FileMetadata fm : j.allInputs()) {
                assertThat(seen).withFailMessage("file %s claimed by two jobs", fm.fileNumber())
                    .doesNotContain(fm.fileNumber());
                seen.add(fm.fileNumber());
            }
        }
    }

    @Test
    void pickMultipleSingleJobMatchesPick() {
        // pickMultiple with max=1 must agree with pick(). Need 5 L0 files to exceed the
        // file-count trigger; equality (4 files == trigger) leaves score == 1.0 and no pick.
        List<FileMetadata> l0 = List.of(
            l0File(10, "a", "b"),
            l0File(11, "c", "d"),
            l0File(12, "e", "f"),
            l0File(13, "g", "h"),
            l0File(14, "i", "j"));
        Version v = versionWith(l0, List.of());

        List<CompactionJob> multi = new LeveledCompactionPicker().pickMultiple(v, Set.of(), 1);
        CompactionJob single = new LeveledCompactionPicker().pick(v).orElseThrow();

        assertThat(multi).hasSize(1);
        assertThat(multi.get(0).inputs()).hasSameSizeAs(single.inputs());
        assertThat(multi.get(0).inputLevel()).isEqualTo(single.inputLevel());
    }

    @Test
    void pickMultipleReturnsEmptyOnQuietDb() {
        // No level is over its target — nothing to do.
        Version v = versionWith(List.of(l0File(10, "a", "b")), List.of());
        assertThat(new LeveledCompactionPicker().pickMultiple(v, Set.of(), 4)).isEmpty();
    }
}
