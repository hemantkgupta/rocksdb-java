package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VersionEditCodecTest {

    @Test
    void roundTripsEachEditType() {
        FileMetadata fm = new FileMetadata(
            new FileNumber(7L), 1024L,
            new InternalKey(Key.of("alpha"), new SequenceNumber(10L), ValueType.VALUE),
            new InternalKey(Key.of("zulu"), new SequenceNumber(5L), ValueType.VALUE));
        List<VersionEdit> edits = List.of(
            new VersionEdit.NewFile(0, fm),
            new VersionEdit.DeleteFile(1, new FileNumber(3L)),
            new VersionEdit.SetLogNumber(42L),
            new VersionEdit.SetNextFileNumber(99L),
            new VersionEdit.SetLastSequence(1024L));
        byte[] payload = VersionEditCodec.encode(edits);
        List<VersionEdit> decoded = VersionEditCodec.decode(payload);
        assertThat(decoded).isEqualTo(edits);
    }

    @Test
    void unknownTagRejected() {
        byte[] payload = new byte[] {0x77};
        assertThatThrownBy(() -> VersionEditCodec.decode(payload))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown");
    }

    @Test
    void emptyPayloadDecodesToEmptyList() {
        List<VersionEdit> decoded = VersionEditCodec.decode(new byte[0]);
        assertThat(decoded).isEmpty();
    }
}
