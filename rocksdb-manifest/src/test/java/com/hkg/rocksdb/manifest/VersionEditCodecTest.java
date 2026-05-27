package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.SequenceNumber;
import com.hkg.rocksdb.common.ValueType;
import com.hkg.rocksdb.integrity.FileChecksum;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
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

    // ---------- CP 19: file-checksum suffix ----------

    @Test
    void newFileWithChecksumRoundTrips() {
        FileMetadata fm = new FileMetadata(
            new FileNumber(11L), 2048L,
            new InternalKey(Key.of("alpha"), new SequenceNumber(20L), ValueType.VALUE),
            new InternalKey(Key.of("zulu"),  new SequenceNumber(15L), ValueType.VALUE),
            new FileChecksum(0xDEADBEEFCAFEBABEL));
        List<VersionEdit> edits = List.of(new VersionEdit.NewFile(2, fm));

        byte[] payload = VersionEditCodec.encode(edits);
        List<VersionEdit> decoded = VersionEditCodec.decode(payload);

        assertThat(decoded).hasSize(1);
        VersionEdit.NewFile nf = (VersionEdit.NewFile) decoded.get(0);
        assertThat(nf.metadata().fileChecksum())
            .isEqualTo(new FileChecksum(0xDEADBEEFCAFEBABEL));
    }

    @Test
    void mixedRecordWithAndWithoutChecksumRoundTrips() {
        FileMetadata withCk = new FileMetadata(
            new FileNumber(1L), 100L,
            new InternalKey(Key.of("aaa"), new SequenceNumber(1L), ValueType.VALUE),
            new InternalKey(Key.of("aab"), new SequenceNumber(2L), ValueType.VALUE),
            new FileChecksum(0x1234567890ABCDEFL));
        FileMetadata noCk = new FileMetadata(
            new FileNumber(2L), 200L,
            new InternalKey(Key.of("bbb"), new SequenceNumber(3L), ValueType.VALUE),
            new InternalKey(Key.of("bbc"), new SequenceNumber(4L), ValueType.VALUE));

        List<VersionEdit> edits = List.of(
            new VersionEdit.NewFile(0, withCk),
            new VersionEdit.SetLogNumber(42L),
            new VersionEdit.NewFile(1, noCk),
            new VersionEdit.SetLastSequence(99L));

        byte[] payload = VersionEditCodec.encode(edits);
        List<VersionEdit> decoded = VersionEditCodec.decode(payload);

        assertThat(decoded).hasSize(4);
        VersionEdit.NewFile a = (VersionEdit.NewFile) decoded.get(0);
        VersionEdit.NewFile b = (VersionEdit.NewFile) decoded.get(2);
        assertThat(a.metadata().fileChecksum()).isEqualTo(withCk.fileChecksum());
        assertThat(b.metadata().fileChecksum()).isNull();
        assertThat(decoded.get(1)).isEqualTo(new VersionEdit.SetLogNumber(42L));
        assertThat(decoded.get(3)).isEqualTo(new VersionEdit.SetLastSequence(99L));
    }

    @Test
    void preCp19PayloadWithoutSuffixStillDecodes() {
        // Synthesize a record body in the OLD v1 format: a NewFile edit
        // with no checksum suffix, then a SetLogNumber. A CP-19 reader
        // must distinguish "no suffix" from "suffix present" by peeking
        // the next byte — here the next byte is the next edit's tag
        // (0x12, well above 0x01), so the decoder must skip the suffix.
        FileMetadata fm = new FileMetadata(
            new FileNumber(7L), 1024L,
            new InternalKey(Key.of("alpha"), new SequenceNumber(10L), ValueType.VALUE),
            new InternalKey(Key.of("zulu"),  new SequenceNumber(5L),  ValueType.VALUE));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // ----- NewFile in legacy format (no suffix) -----
        out.write(0x10);
        java.nio.ByteBuffer scratch = java.nio.ByteBuffer.allocate(64)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
        VersionEditCodec.writeVarLong(scratch, 0L);              // level
        VersionEditCodec.writeVarLong(scratch, fm.fileNumber().value());
        VersionEditCodec.writeVarLong(scratch, fm.sizeBytes());
        scratch.flip(); out.write(scratch.array(), 0, scratch.limit());
        byte[] sm = com.hkg.rocksdb.common.InternalKeyCodec.encode(fm.smallestKey());
        byte[] lg = com.hkg.rocksdb.common.InternalKeyCodec.encode(fm.largestKey());
        scratch = java.nio.ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        VersionEditCodec.writeVarLong(scratch, sm.length);
        scratch.flip(); out.write(scratch.array(), 0, scratch.limit());
        out.write(sm, 0, sm.length);
        scratch = java.nio.ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        VersionEditCodec.writeVarLong(scratch, lg.length);
        scratch.flip(); out.write(scratch.array(), 0, scratch.limit());
        out.write(lg, 0, lg.length);
        // ----- SetLogNumber follows directly, NO checksum-suffix flag -----
        out.write(0x12);
        scratch = java.nio.ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        VersionEditCodec.writeVarLong(scratch, 555L);
        scratch.flip(); out.write(scratch.array(), 0, scratch.limit());

        List<VersionEdit> decoded = VersionEditCodec.decode(out.toByteArray());
        assertThat(decoded).hasSize(2);
        VersionEdit.NewFile nf = (VersionEdit.NewFile) decoded.get(0);
        assertThat(nf.metadata().fileChecksum()).isNull();
        assertThat(nf.metadata().fileNumber().value()).isEqualTo(7L);
        assertThat(decoded.get(1)).isEqualTo(new VersionEdit.SetLogNumber(555L));
    }

    @Test
    void preCp19TerminalNewFileWithoutSuffixDecodes() {
        // A legacy NewFile that is the LAST edit in the record: no trailing
        // byte at all → decoder must treat absence as "no checksum".
        FileMetadata fm = new FileMetadata(
            new FileNumber(3L), 64L,
            new InternalKey(Key.of("a"), new SequenceNumber(1L), ValueType.VALUE),
            new InternalKey(Key.of("b"), new SequenceNumber(2L), ValueType.VALUE));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x10);
        java.nio.ByteBuffer scratch = java.nio.ByteBuffer.allocate(64)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
        VersionEditCodec.writeVarLong(scratch, 1L);
        VersionEditCodec.writeVarLong(scratch, fm.fileNumber().value());
        VersionEditCodec.writeVarLong(scratch, fm.sizeBytes());
        scratch.flip(); out.write(scratch.array(), 0, scratch.limit());
        byte[] sm = com.hkg.rocksdb.common.InternalKeyCodec.encode(fm.smallestKey());
        byte[] lg = com.hkg.rocksdb.common.InternalKeyCodec.encode(fm.largestKey());
        scratch = java.nio.ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        VersionEditCodec.writeVarLong(scratch, sm.length);
        scratch.flip(); out.write(scratch.array(), 0, scratch.limit());
        out.write(sm, 0, sm.length);
        scratch = java.nio.ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        VersionEditCodec.writeVarLong(scratch, lg.length);
        scratch.flip(); out.write(scratch.array(), 0, scratch.limit());
        out.write(lg, 0, lg.length);

        List<VersionEdit> decoded = VersionEditCodec.decode(out.toByteArray());
        assertThat(decoded).hasSize(1);
        VersionEdit.NewFile nf = (VersionEdit.NewFile) decoded.get(0);
        assertThat(nf.metadata().fileChecksum()).isNull();
    }
}
