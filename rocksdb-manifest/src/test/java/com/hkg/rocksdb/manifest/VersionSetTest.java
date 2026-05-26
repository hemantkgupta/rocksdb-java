package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VersionSetTest {

    private static FileMetadata fm(long n) {
        return new FileMetadata(new FileNumber(n), 8192L,
            new InternalKey(Key.of("a"), new SequenceNumber(n), ValueType.VALUE),
            new InternalKey(Key.of("z"), new SequenceNumber(n), ValueType.VALUE));
    }

    @Test
    void createFreshWritesCurrentPointer(@TempDir Path dir) throws IOException {
        try (VersionSet vs = VersionSet.createFresh(dir)) {
            assertThat(Files.exists(dir.resolve("CURRENT"))).isTrue();
            String currentContent = Files.readString(dir.resolve("CURRENT")).trim();
            assertThat(currentContent).isEqualTo("MANIFEST-000001");
            assertThat(vs.current().nextFileNumber()).isEqualTo(1L);
        }
    }

    @Test
    void applyAndReopenPreservesState(@TempDir Path dir) throws IOException {
        try (VersionSet vs = VersionSet.createFresh(dir)) {
            vs.apply(List.of(
                new VersionEdit.NewFile(0, fm(10L)),
                new VersionEdit.NewFile(1, fm(11L)),
                new VersionEdit.SetLastSequence(50L),
                new VersionEdit.SetLogNumber(7L)));
        }
        try (VersionSet reopened = VersionSet.open(dir)) {
            assertThat(reopened.current().level(0)).hasSize(1);
            assertThat(reopened.current().level(1)).hasSize(1);
            assertThat(reopened.current().lastSequence()).isEqualTo(50L);
            assertThat(reopened.current().logNumber()).isEqualTo(7L);
        }
    }

    @Test
    void rotateProducesNewManifestAndDeletesOld(@TempDir Path dir) throws IOException {
        try (VersionSet vs = VersionSet.createFresh(dir, /* tiny rotation threshold */ 64L)) {
            vs.apply(List.of(new VersionEdit.NewFile(0, fm(10L))));
            vs.apply(List.of(new VersionEdit.NewFile(0, fm(11L))));
            vs.apply(List.of(new VersionEdit.NewFile(0, fm(12L))));
            // Force rotation.
            vs.rotateNow();
            // The old MANIFEST-000001 must be gone; a new one with a higher number must exist.
            assertThat(Files.exists(dir.resolve("MANIFEST-000001"))).isFalse();
            assertThat(vs.activeManifestNumber().value()).isGreaterThan(1L);
            assertThat(Files.exists(dir.resolve(vs.activeManifestNumber().manifestFileName()))).isTrue();
            String pointer = Files.readString(dir.resolve("CURRENT")).trim();
            assertThat(pointer).isEqualTo(vs.activeManifestNumber().manifestFileName());
        }
    }

    @Test
    void recoveryFromRotatedManifest(@TempDir Path dir) throws IOException {
        FileMetadata fm10 = fm(10L);
        FileMetadata fm11 = fm(11L);
        try (VersionSet vs = VersionSet.createFresh(dir)) {
            vs.apply(List.of(new VersionEdit.NewFile(0, fm10)));
            vs.apply(List.of(new VersionEdit.NewFile(1, fm11)));
            vs.apply(List.of(new VersionEdit.SetLastSequence(123L)));
            vs.rotateNow();
        }
        try (VersionSet reopened = VersionSet.open(dir)) {
            assertThat(reopened.current().level(0).stream().map(FileMetadata::fileNumber))
                .containsExactly(fm10.fileNumber());
            assertThat(reopened.current().level(1).stream().map(FileMetadata::fileNumber))
                .containsExactly(fm11.fileNumber());
            assertThat(reopened.current().lastSequence()).isEqualTo(123L);
        }
    }

    @Test
    void missingCurrentRefused(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir);
        assertThatThrownBy(() -> VersionSet.open(dir))
            .isInstanceOf(ManifestCorruptionException.class);
    }

    @Test
    void corruptCurrentRefused(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("CURRENT"), "not-a-manifest-name\n");
        assertThatThrownBy(() -> VersionSet.open(dir))
            .isInstanceOf(ManifestCorruptionException.class);
    }
}
