package com.hkg.rocksdb.manifest;

import com.hkg.rocksdb.wal.LogReader;
import com.hkg.rocksdb.wal.LogWriter;
import com.hkg.rocksdb.wal.WalCorruptionException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Append-only MANIFEST log. Reuses the WAL physical framing (32 KiB blocks,
 * 7-byte CRC header, FULL/FIRST/MIDDLE/LAST chunking) so that the MANIFEST
 * inherits the same torn-write tolerance — though for the MANIFEST, a
 * torn record at the tail means the engine simply lost an edit, not data.
 *
 * <p>Each record's payload is one or more {@link VersionEdit}s encoded by
 * {@link VersionEditCodec}.
 */
public final class Manifest implements Closeable {

    private final LogWriter writer;
    private long bytesWritten;

    private Manifest(LogWriter writer, long initialBytes) {
        this.writer = writer;
        this.bytesWritten = initialBytes;
    }

    /** Open a new MANIFEST file (truncates any existing file at the path). */
    public static Manifest open(Path file) throws IOException {
        return new Manifest(LogWriter.open(file, true), 0L);
    }

    /** Open an existing MANIFEST file for appending — used after recovery. */
    public static Manifest openForAppend(Path file) throws IOException {
        long initial = java.nio.file.Files.exists(file) ? java.nio.file.Files.size(file) : 0L;
        return new Manifest(LogWriter.openForAppend(file, true), initial);
    }

    /** Append a batch of edits; the WAL writer fsyncs before returning. */
    public synchronized void append(List<VersionEdit> edits) throws IOException {
        byte[] payload = VersionEditCodec.encode(edits);
        long beforeBytes = writer.bytesWritten();
        writer.append(payload);
        long after = writer.bytesWritten();
        bytesWritten += (after - beforeBytes);
    }

    /** Replay every edit batch in {@code file}, in append order. */
    public static List<List<VersionEdit>> replay(Path file) throws IOException {
        try (LogReader reader = LogReader.open(file)) {
            List<List<VersionEdit>> all = new ArrayList<>();
            Iterator<byte[]> it = reader.records();
            try {
                while (it.hasNext()) {
                    byte[] payload = it.next();
                    all.add(VersionEditCodec.decode(payload));
                }
            } catch (RuntimeException ex) {
                // Wrap WAL-layer corruption into a manifest-layer exception so callers can
                // distinguish "manifest unreadable" from generic I/O errors.
                if (ex instanceof WalCorruptionException || ex.getCause() instanceof WalCorruptionException) {
                    throw new ManifestCorruptionException("MANIFEST corrupted: " + ex.getMessage(), ex);
                }
                throw ex;
            }
            return all;
        }
    }

    /** Number of bytes appended to this MANIFEST (since open or rotation). */
    public long bytesWritten() {
        return bytesWritten;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
