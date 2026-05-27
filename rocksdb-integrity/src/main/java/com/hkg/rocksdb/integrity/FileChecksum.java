package com.hkg.rocksdb.integrity;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * The file-level integrity value carried in {@code FileMetadata} and
 * persisted in MANIFEST. It is the XXH64 of every byte of an SSTable
 * file — computed once after {@code writer.finish()} and verified by
 * {@code db-verify} (and, in later CPs, by backup/restore).
 *
 * <p>This is the §5 "above-block" layer: block CRC32 catches mid-block
 * bit rot but cannot see a flipped byte in the SSTable footer padding,
 * a truncated tail, or whole-file mis-attribution. XXH64 over the
 * complete file does.
 */
public record FileChecksum(long value) {

    /** Stream {@code file} through {@link XxHash64} and return the digest. */
    public static FileChecksum compute(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        XxHash64 h = new XxHash64();
        byte[] buf = new byte[64 * 1024];
        try (InputStream in = Files.newInputStream(file)) {
            int n;
            while ((n = in.read(buf)) > 0) {
                h.update(buf, 0, n);
            }
        }
        return new FileChecksum(h.digest());
    }

    @Override
    public String toString() {
        return "FileChecksum(0x" + Long.toHexString(value) + ")";
    }
}
