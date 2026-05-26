package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VersionTest {

    private static FileMetadata fm(long fileNum) {
        return new FileMetadata(new FileNumber(fileNum), 1024L,
            new InternalKey(Key.of("a"), new SequenceNumber(1L), ValueType.VALUE),
            new InternalKey(Key.of("z"), new SequenceNumber(1L), ValueType.VALUE));
    }

    @Test
    void emptyVersionHasNoFiles() {
        Version v = Version.empty();
        for (List<FileMetadata> level : v.levels()) {
            assertThat(level).isEmpty();
        }
        assertThat(v.lastSequence()).isEqualTo(0L);
        assertThat(v.logNumber()).isEqualTo(0L);
        assertThat(v.nextFileNumber()).isEqualTo(1L);
    }

    @Test
    void applyNewFileIsFunctional() {
        Version v0 = Version.empty();
        Version v1 = v0.applyEdit(new VersionEdit.NewFile(0, fm(10L)));
        // v0 is unchanged.
        assertThat(v0.level(0)).isEmpty();
        // v1 has the file in L0 and bumped nextFileNumber.
        assertThat(v1.level(0)).hasSize(1);
        assertThat(v1.level(0).get(0).fileNumber().value()).isEqualTo(10L);
        assertThat(v1.nextFileNumber()).isEqualTo(11L);
    }

    @Test
    void deleteFileRemovesEntry() {
        Version v0 = Version.empty()
            .applyEdit(new VersionEdit.NewFile(1, fm(7L)))
            .applyEdit(new VersionEdit.NewFile(1, fm(8L)));
        Version v1 = v0.applyEdit(new VersionEdit.DeleteFile(1, new FileNumber(7L)));
        assertThat(v1.level(1)).hasSize(1);
        assertThat(v1.level(1).get(0).fileNumber().value()).isEqualTo(8L);
    }

    @Test
    void setScalarsApply() {
        Version v = Version.empty()
            .applyEdit(new VersionEdit.SetLogNumber(5L))
            .applyEdit(new VersionEdit.SetLastSequence(99L))
            .applyEdit(new VersionEdit.SetNextFileNumber(50L));
        assertThat(v.logNumber()).isEqualTo(5L);
        assertThat(v.lastSequence()).isEqualTo(99L);
        assertThat(v.nextFileNumber()).isEqualTo(50L);
    }

    @Test
    void levelSizeBytesAggregates() {
        Version v = Version.empty()
            .applyEdit(new VersionEdit.NewFile(2, fm(1L)))
            .applyEdit(new VersionEdit.NewFile(2, fm(2L)));
        // Two files at 1024 each.
        assertThat(v.levelSizeBytes(2)).isEqualTo(2048L);
    }
}
