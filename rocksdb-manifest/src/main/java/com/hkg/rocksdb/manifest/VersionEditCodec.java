package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.common.FileNumber;
import com.hkg.rocksdb.common.InternalKey;
import com.hkg.rocksdb.common.InternalKeyCodec;
import com.hkg.rocksdb.integrity.FileChecksum;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a list of {@link VersionEdit}s into a single byte payload (the
 * MANIFEST record body) and decodes a MANIFEST record back into a list.
 *
 * <p>Wire format (LE):
 * <pre>
 *   while bytes remain:
 *     tag:1
 *     fields per tag:
 *       0x10 NEW_FILE         : varint(level), varint(fileNum), varint(sizeBytes),
 *                               varint(smallestLen), smallest, varint(largestLen), largest,
 *                               [hasChecksum:1, if 1 then checksum:8]   // CP 19 suffix
 *       0x11 DELETE_FILE      : varint(level), varint(fileNum)
 *       0x12 SET_LOG_NUMBER   : varlong
 *       0x13 SET_NEXT_FILENUM : varlong
 *       0x14 SET_LAST_SEQ     : varlong
 * </pre>
 *
 * <p><b>CP 19 — backward compat.</b> The {@code NEW_FILE} payload gained a
 * trailing {@code (hasChecksum,checksum)} suffix in CP 19. Older MANIFEST
 * records do not contain this suffix; the decoder distinguishes the two
 * cases by peeking the next byte after {@code largest}:
 * <ul>
 *   <li>Byte value 0 or 1 → CP-19 flag (read suffix).</li>
 *   <li>Byte value 0x10..0x14 → next edit's tag (older NewFile,
 *       no checksum).</li>
 *   <li>No remaining bytes → end of record (older NewFile, last edit).</li>
 * </ul>
 * This works because all defined edit tags are &ge; 0x10, so 0x00/0x01
 * cannot collide. A v1 reader (pre-CP-19) reading a CP-19 record would
 * fail at the spurious "tag 0x00/0x01" — acceptable since v1 is the
 * predecessor format and rocksdb-java has not been deployed yet.
 *
 * <p>An unknown tag in the middle of a record is a hard error — matching
 * RocksDb's "no skip-unknown" semantics.
 */
public final class VersionEditCodec {

    private VersionEditCodec() {}

    public static byte[] encode(List<VersionEdit> edits) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(64);
        ByteBuffer scratch = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
        for (VersionEdit edit : edits) {
            out.write(edit.tag() & 0xff);
            scratch.clear();
            if (edit instanceof VersionEdit.NewFile nf) {
                writeVarLong(scratch, nf.level());
                writeVarLong(scratch, nf.metadata().fileNumber().value());
                writeVarLong(scratch, nf.metadata().sizeBytes());
                byte[] sm = InternalKeyCodec.encode(nf.metadata().smallestKey());
                byte[] lg = InternalKeyCodec.encode(nf.metadata().largestKey());
                writeVarLong(scratch, sm.length);
                writeBuf(out, scratch);
                writeBytes(out, sm);
                scratch.clear();
                writeVarLong(scratch, lg.length);
                writeBuf(out, scratch);
                writeBytes(out, lg);
                // CP 19 suffix — always emit the flag so the decoder can
                // unambiguously detect presence (see class javadoc).
                FileChecksum fc = nf.metadata().fileChecksum();
                scratch.clear();
                if (fc == null) {
                    scratch.put((byte) 0);
                } else {
                    scratch.put((byte) 1);
                    scratch.putLong(fc.value());
                }
                writeBuf(out, scratch);
            } else if (edit instanceof VersionEdit.DeleteFile df) {
                writeVarLong(scratch, df.level());
                writeVarLong(scratch, df.fileNumber().value());
                writeBuf(out, scratch);
            } else if (edit instanceof VersionEdit.SetLogNumber sln) {
                writeVarLong(scratch, sln.logNumber());
                writeBuf(out, scratch);
            } else if (edit instanceof VersionEdit.SetNextFileNumber snfn) {
                writeVarLong(scratch, snfn.nextFileNumber());
                writeBuf(out, scratch);
            } else if (edit instanceof VersionEdit.SetLastSequence sls) {
                writeVarLong(scratch, sls.lastSequence());
                writeBuf(out, scratch);
            } else {
                throw new IllegalStateException("unreachable: sealed VersionEdit");
            }
        }
        return out.toByteArray();
    }

    public static List<VersionEdit> decode(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        List<VersionEdit> edits = new ArrayList<>();
        while (buf.hasRemaining()) {
            byte tag = buf.get();
            switch (tag) {
                case 0x10 -> {
                    int level = (int) readVarLong(buf);
                    long fileNum = readVarLong(buf);
                    long sizeBytes = readVarLong(buf);
                    int smLen = (int) readVarLong(buf);
                    byte[] sm = new byte[smLen];
                    buf.get(sm);
                    int lgLen = (int) readVarLong(buf);
                    byte[] lg = new byte[lgLen];
                    buf.get(lg);
                    InternalKey smallest = InternalKeyCodec.decode(sm);
                    InternalKey largest = InternalKeyCodec.decode(lg);
                    FileChecksum fc = maybeReadChecksumSuffix(buf);
                    edits.add(new VersionEdit.NewFile(level,
                        new FileMetadata(new FileNumber(fileNum), sizeBytes, smallest, largest, fc)));
                }
                case 0x11 -> {
                    int level = (int) readVarLong(buf);
                    long fileNum = readVarLong(buf);
                    edits.add(new VersionEdit.DeleteFile(level, new FileNumber(fileNum)));
                }
                case 0x12 -> edits.add(new VersionEdit.SetLogNumber(readVarLong(buf)));
                case 0x13 -> edits.add(new VersionEdit.SetNextFileNumber(readVarLong(buf)));
                case 0x14 -> edits.add(new VersionEdit.SetLastSequence(readVarLong(buf)));
                default -> throw new IllegalArgumentException(
                    "unknown VersionEdit tag: 0x" + Integer.toHexString(tag & 0xff));
            }
        }
        return edits;
    }

    /**
     * Peek the next byte after {@code largest}. If it's a CP-19 flag
     * (0x00 or 0x01), consume the suffix and return the checksum (or
     * null when the flag is 0). Otherwise leave the buffer untouched —
     * it's either the next edit's tag (an older NewFile with no checksum)
     * or end-of-record.
     */
    private static FileChecksum maybeReadChecksumSuffix(ByteBuffer buf) {
        if (!buf.hasRemaining()) return null;
        byte peek = buf.get(buf.position());
        if (peek != 0 && peek != 1) {
            // Looks like the next edit's tag — older format, no suffix.
            return null;
        }
        buf.get(); // consume the flag
        if (peek == 0) return null;
        long value = buf.getLong();
        return new FileChecksum(value);
    }

    private static void writeBuf(ByteArrayOutputStream out, ByteBuffer scratch) {
        scratch.flip();
        out.write(scratch.array(), 0, scratch.limit());
    }

    private static void writeBytes(ByteArrayOutputStream out, byte[] data) {
        out.write(data, 0, data.length);
    }

    static void writeVarLong(ByteBuffer buf, long v) {
        while ((v & ~0x7FL) != 0L) {
            buf.put((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        buf.put((byte) (v & 0x7F));
    }

    static long readVarLong(ByteBuffer buf) {
        long result = 0L;
        int shift = 0;
        while (true) {
            if (!buf.hasRemaining()) {
                throw new IllegalArgumentException("varint truncated");
            }
            byte b = buf.get();
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 63) throw new IllegalArgumentException("varint too long");
        }
    }
}
