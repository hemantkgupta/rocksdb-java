package com.hkg.rocksdb.sstable;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Block-level compression. RocksDb's reference uses Snappy; we use the JDK
 * {@link Deflater} as the pedagogical replacement (no JNI). The block-format
 * compression-type byte is preserved as the extension hook.
 */
public final class Compression {

    private Compression() {}

    public static final byte TYPE_NONE = 0x00;
    public static final byte TYPE_DEFLATE = 0x01;

    public static byte[] compressDeflate(byte[] input) {
        Deflater def = new Deflater(Deflater.DEFAULT_COMPRESSION, false);
        try {
            def.setInput(input);
            def.finish();
            ByteArrayOutputStream out = new ByteArrayOutputStream(input.length);
            byte[] buf = new byte[4096];
            while (!def.finished()) {
                int n = def.deflate(buf);
                if (n == 0) break;
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } finally {
            def.end();
        }
    }

    public static byte[] decompressDeflate(byte[] input) {
        Inflater inf = new Inflater(false);
        try {
            inf.setInput(input);
            ByteArrayOutputStream out = new ByteArrayOutputStream(input.length * 2);
            byte[] buf = new byte[4096];
            while (!inf.finished()) {
                int n = inf.inflate(buf);
                if (n == 0) {
                    if (inf.needsInput() || inf.needsDictionary()) break;
                    break;
                }
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } catch (DataFormatException e) {
            throw new SsTableFormatException("deflate decompression failed", e);
        } finally {
            inf.end();
        }
    }

    /** Decode according to the per-block compression-type tag. */
    public static byte[] decode(byte type, byte[] payload) {
        return switch (type) {
            case TYPE_NONE -> payload;
            case TYPE_DEFLATE -> decompressDeflate(payload);
            default -> throw new SsTableFormatException(
                "unknown compression type: 0x" + Integer.toHexString(type & 0xff));
        };
    }
}
