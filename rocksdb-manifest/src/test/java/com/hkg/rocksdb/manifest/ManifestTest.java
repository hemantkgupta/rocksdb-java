package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ManifestTest {

    @Test
    void appendAndReplayRoundTrip(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("MANIFEST-000001");
        FileMetadata fm = new FileMetadata(new FileNumber(10L), 4096L,
            new InternalKey(Key.of("a"), new SequenceNumber(1L), ValueType.VALUE),
            new InternalKey(Key.of("z"), new SequenceNumber(1L), ValueType.VALUE));
        try (Manifest m = Manifest.open(file)) {
            m.append(List.of(new VersionEdit.SetLogNumber(7L)));
            m.append(List.of(new VersionEdit.NewFile(0, fm)));
            m.append(List.of(new VersionEdit.SetLastSequence(42L)));
        }
        List<List<VersionEdit>> replayed = Manifest.replay(file);
        assertThat(replayed).hasSize(3);
        assertThat(replayed.get(0)).containsExactly(new VersionEdit.SetLogNumber(7L));
        assertThat(replayed.get(1)).containsExactly(new VersionEdit.NewFile(0, fm));
        assertThat(replayed.get(2)).containsExactly(new VersionEdit.SetLastSequence(42L));
    }

    @Test
    void replayEmptyManifest(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("MANIFEST-000002");
        try (Manifest m = Manifest.open(file)) {
            // Nothing.
        }
        assertThat(Manifest.replay(file)).isEmpty();
    }

    @Test
    void appendBatchedEdits(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("MANIFEST-000003");
        try (Manifest m = Manifest.open(file)) {
            m.append(List.of(
                new VersionEdit.SetLogNumber(1L),
                new VersionEdit.SetLastSequence(2L),
                new VersionEdit.SetNextFileNumber(3L)));
        }
        List<List<VersionEdit>> replayed = Manifest.replay(file);
        assertThat(replayed).hasSize(1);
        assertThat(replayed.get(0)).hasSize(3);
    }
}
