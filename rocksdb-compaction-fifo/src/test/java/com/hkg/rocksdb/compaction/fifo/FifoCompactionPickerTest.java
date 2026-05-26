package com.hkg.rocksdb.compaction.fifo;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.manifest.FileMetadata;
import com.hkg.rocksdb.manifest.Version;
import com.hkg.rocksdb.manifest.VersionEdit;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FifoCompactionPickerTest {

    private static FileMetadata fm(long n, long size) {
        // user key + sequence are irrelevant for FIFO — we only care about
        // fileNumber (age) and sizeBytes (cap accounting).
        return new FileMetadata(
            new FileNumber(n),
            size,
            new InternalKey(Key.of("a"), new SequenceNumber(n), ValueType.VALUE),
            new InternalKey(Key.of("z"), new SequenceNumber(n), ValueType.VALUE));
    }

    private static Version withL0Files(FileMetadata... files) {
        Version v = Version.empty();
        for (FileMetadata fm : files) {
            v = v.applyEdit(new VersionEdit.NewFile(0, fm));
        }
        return v;
    }

    @Test
    void emptyVersionReturnsEmpty() {
        FifoCompactionPicker picker = new FifoCompactionPicker(1024L);
        assertThat(picker.pick(Version.empty())).isEmpty();
    }

    @Test
    void totalUnderCapReturnsEmpty() {
        FifoCompactionPicker picker = new FifoCompactionPicker(10_000L);
        Version v = withL0Files(fm(1L, 1_000L), fm(2L, 2_000L), fm(3L, 3_000L));
        // 6,000 < 10,000 → nothing to drop.
        assertThat(picker.pick(v)).isEmpty();
        assertThat(picker.pickMultiple(v, Set.of(), 5)).isEmpty();
    }

    @Test
    void totalEqualToCapReturnsEmpty() {
        // boundary: cap is "over the limit", not "at the limit".
        FifoCompactionPicker picker = new FifoCompactionPicker(1_000L);
        Version v = withL0Files(fm(1L, 1_000L));
        assertThat(picker.pick(v)).isEmpty();
    }

    @Test
    void singleFileOverCapIsPicked() {
        FifoCompactionPicker picker = new FifoCompactionPicker(500L);
        Version v = withL0Files(fm(42L, 1_000L));
        Optional<FifoDeletionJob> job = picker.pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().file().value()).isEqualTo(42L);
        assertThat(job.get().sizeBytes()).isEqualTo(1_000L);
    }

    @Test
    void picksOldestWhenMultipleFilesAndCapExceeded() {
        FifoCompactionPicker picker = new FifoCompactionPicker(2_000L);
        // insertion order intentionally not monotonic by fileNumber, to make
        // sure the picker reads age from fileNumber not list order.
        Version v = withL0Files(fm(5L, 1_500L), fm(2L, 1_500L), fm(9L, 1_500L));
        // total = 4,500 > 2,000.
        Optional<FifoDeletionJob> job = picker.pick(v);
        assertThat(job).isPresent();
        assertThat(job.get().file().value()).isEqualTo(2L);
    }

    @Test
    void higherFileNumbersAreNewerAndNotChosenFirst() {
        FifoCompactionPicker picker = new FifoCompactionPicker(0L);
        Version v = withL0Files(fm(100L, 1L), fm(101L, 1L), fm(102L, 1L));
        // cap is 0 — every file is over. We want them in age order: 100, 101, 102.
        List<FifoDeletionJob> jobs = picker.pickMultiple(v, Set.of(), 3);
        assertThat(jobs).extracting(j -> j.file().value())
            .containsExactly(100L, 101L, 102L);
    }

    @Test
    void pickMultipleRespectsMax() {
        FifoCompactionPicker picker = new FifoCompactionPicker(0L);
        Version v = withL0Files(fm(1L, 1L), fm(2L, 1L), fm(3L, 1L), fm(4L, 1L));
        List<FifoDeletionJob> jobs = picker.pickMultiple(v, Set.of(), 2);
        assertThat(jobs).hasSize(2);
        assertThat(jobs).extracting(j -> j.file().value()).containsExactly(1L, 2L);
    }

    @Test
    void pickMultipleSkipsExcludedFiles() {
        FifoCompactionPicker picker = new FifoCompactionPicker(0L);
        Version v = withL0Files(fm(1L, 1L), fm(2L, 1L), fm(3L, 1L));
        // file 1 is already being deleted by another worker; skip it.
        List<FifoDeletionJob> jobs = picker.pickMultiple(
            v, Set.of(new FileNumber(1L)), 5);
        assertThat(jobs).extracting(j -> j.file().value()).containsExactly(2L, 3L);
    }

    @Test
    void pickMultipleStopsWhenCapRestored() {
        // total = 3,000; cap = 1,500. Dropping just file 1 (oldest, 1,000) leaves
        // 2,000 still over the cap. Dropping file 2 (1,000) too brings us to
        // 1,000 which is under the cap — so we stop after two even though
        // max = 10.
        FifoCompactionPicker picker = new FifoCompactionPicker(1_500L);
        Version v = withL0Files(fm(1L, 1_000L), fm(2L, 1_000L), fm(3L, 1_000L));
        List<FifoDeletionJob> jobs = picker.pickMultiple(v, Set.of(), 10);
        assertThat(jobs).extracting(j -> j.file().value()).containsExactly(1L, 2L);
    }

    @Test
    void capExceededByMoreThanOneFilePicksMultiple() {
        // headline failure scenario from the plan: cap exceeded 2× → multiple
        // files deleted in one tick; cap restored.
        FifoCompactionPicker picker = new FifoCompactionPicker(2_000L);
        Version v = withL0Files(
            fm(1L, 1_000L), fm(2L, 1_000L), fm(3L, 1_000L), fm(4L, 1_000L));
        // total = 4,000, cap = 2,000 → need to free at least 2,000.
        List<FifoDeletionJob> jobs = picker.pickMultiple(v, Set.of(), 10);
        assertThat(jobs).extracting(j -> j.file().value()).containsExactly(1L, 2L);
        long freed = jobs.stream().mapToLong(FifoDeletionJob::sizeBytes).sum();
        assertThat(freed).isGreaterThanOrEqualTo(2_000L);
    }

    @Test
    void pickMultipleWithMaxZeroReturnsEmpty() {
        FifoCompactionPicker picker = new FifoCompactionPicker(0L);
        Version v = withL0Files(fm(1L, 1L));
        assertThat(picker.pickMultiple(v, Set.of(), 0)).isEmpty();
    }

    @Test
    void higherLevelsAreIgnoredByFifo() {
        // FIFO model only knows about L0. Bytes in L1+ — however they got
        // there — must not influence the picker.
        FifoCompactionPicker picker = new FifoCompactionPicker(10_000L);
        Version v = Version.empty();
        // small L0, huge L1.
        v = v.applyEdit(new VersionEdit.NewFile(0, fm(1L, 500L)));
        v = v.applyEdit(new VersionEdit.NewFile(1, fm(100L, 1_000_000L)));
        assertThat(picker.pick(v)).isEmpty();
    }

    @Test
    void rejectsNegativeMaxBytes() {
        assertThatThrownBy(() -> new FifoCompactionPicker(-1L))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
