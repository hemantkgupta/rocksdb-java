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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LeveledCompactionPickerTest {

    private static FileMetadata fm(long n, String smallUserKey, String largeUserKey, long size) {
        return new FileMetadata(new FileNumber(n), size,
            new InternalKey(Key.of(smallUserKey), new SequenceNumber(n), ValueType.VALUE),
            new InternalKey(Key.of(largeUserKey), new SequenceNumber(n), ValueType.VALUE));
    }

    @Test
    void emptyVersionReturnsNoJob() {
        Version v = Version.empty();
        assertThat(new LeveledCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void picksL0WhenFileCountExceedsTrigger() {
        Version v = Version.empty();
        // L0_FILE_COUNT_TRIGGER = 4. Add 5 L0 files.
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, "a", "z", 1024L)));
        }
        Optional<CompactionJob> job = new LeveledCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputLevel()).isEqualTo(0);
        assertThat(job.get().outputLevel()).isEqualTo(1);
        assertThat(job.get().inputs()).hasSize(5);
    }

    @Test
    void noPickWhenBelowTrigger() {
        Version v = Version.empty();
        for (int i = 1; i <= 3; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, "a", "z", 1024L)));
        }
        // 3 < L0_FILE_COUNT_TRIGGER (4).
        assertThat(new LeveledCompactionPicker().pick(v)).isEmpty();
    }

    @Test
    void picksHigherLevelWhenItsSizeExceedsTarget() {
        Version v = Version.empty();
        long target1 = LeveledCompactionPicker.targetForLevel(1);
        // Push L1 over its target.
        v = v.applyEdit(new VersionEdit.NewFile(1, fm(10L, "a", "m", target1 + 1L)));
        Optional<CompactionJob> job = new LeveledCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().inputLevel()).isEqualTo(1);
    }

    @Test
    void findsOverlappingFilesInNextLevel() {
        Version v = Version.empty();
        // L0 file covering [c, f].
        for (int i = 1; i <= 5; i++) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm(i, "c", "f", 1024L)));
        }
        // L1 files: one inside [c..f] range, one outside.
        v = v.applyEdit(new VersionEdit.NewFile(1, fm(100L, "a", "b", 1024L)));      // no overlap
        v = v.applyEdit(new VersionEdit.NewFile(1, fm(101L, "d", "e", 1024L)));      // overlaps
        v = v.applyEdit(new VersionEdit.NewFile(1, fm(102L, "z", "zz", 1024L)));     // no overlap

        Optional<CompactionJob> job = new LeveledCompactionPicker().pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().overlapping()).hasSize(1);
        assertThat(job.get().overlapping().get(0).fileNumber().value()).isEqualTo(101L);
    }

    @Test
    void targetForLevelMatchesGeometricGrowth() {
        long t1 = LeveledCompactionPicker.targetForLevel(1);
        long t2 = LeveledCompactionPicker.targetForLevel(2);
        long t3 = LeveledCompactionPicker.targetForLevel(3);
        assertThat(t1).isEqualTo(Constants.LEVEL_SIZE_BASE_BYTES);
        assertThat(t2).isEqualTo(t1 * Constants.LEVEL_SIZE_MULTIPLIER);
        assertThat(t3).isEqualTo(t2 * Constants.LEVEL_SIZE_MULTIPLIER);
    }
}
