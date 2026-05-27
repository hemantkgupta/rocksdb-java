package com.hkg.rocksdb.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Append-only writer for the RocksDb WAL format. Each {@link #append(byte[])}
 * writes one logical record, fragmented across blocks if necessary. Durability
 * behaviour is governed by the supplied {@link WalDurability}:
 *
 * <ul>
 *   <li>{@link WalDurability.Sync} — fsync after every append, before the call returns.</li>
 *   <li>{@link WalDurability.Buffered} — no fsync on the hot path; a daemon thread
 *       fsyncs every {@code flushIntervalMillis}. {@link #close()} stops the thread and
 *       does a final force(true).</li>
 *   <li>{@link WalDurability.Disabled} — bytes are written to the channel but never
 *       fsynced. {@link #close()} closes the channel without forcing.</li>
 * </ul>
 *
 * <p>Not thread-safe with respect to concurrent {@code append} callers; the engine
 * serialises all WAL writes through a single writer instance. The Buffered background
 * flusher does call {@code channel.force(true)} concurrently with appends, which is
 * safe on a {@link FileChannel} — {@code force} does not mutate writer position.
 */
public final class LogWriter implements Closeable {

    private final FileChannel channel;
    private final WalDurability durability;
    private final BackgroundFlusher flusher; // null unless Buffered
    private int blockOffset;                  // bytes written into the current block
    private long bytesWritten;                // running total of bytes written via this writer
    private final CRC32 crc = new CRC32();

    private LogWriter(FileChannel channel, WalDurability durability, long initialPosition) {
        this.channel = channel;
        this.durability = durability;
        this.blockOffset = (int) (initialPosition % WalConstants.BLOCK_SIZE);
        this.bytesWritten = initialPosition;
        if (durability instanceof WalDurability.Buffered buf) {
            this.flusher = new BackgroundFlusher(channel, buf.flushIntervalMillis());
            this.flusher.start();
        } else {
            this.flusher = null;
        }
    }

    /**
     * Open a fresh WAL file for writing. Creates if missing; truncates if exists.
     * @param syncOnAppend if true, fsync the file after every {@link #append}.
     */
    public static LogWriter open(Path file, boolean syncOnAppend) throws IOException {
        return open(file, syncOnAppend ? WalDurability.sync() : WalDurability.disabled());
    }

    /** Open a fresh WAL file for writing under the given durability mode. */
    public static LogWriter open(Path file, WalDurability durability) throws IOException {
        Objects.requireNonNull(durability, "durability");
        OpenOption[] opts = new OpenOption[] {
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        };
        FileChannel ch = FileChannel.open(file, opts);
        return new LogWriter(ch, durability, 0L);
    }

    /** Open an existing WAL file in append mode, positioned at end-of-file. */
    public static LogWriter openForAppend(Path file, boolean syncOnAppend) throws IOException {
        return openForAppend(file, syncOnAppend ? WalDurability.sync() : WalDurability.disabled());
    }

    /** Open an existing WAL file in append mode under the given durability mode. */
    public static LogWriter openForAppend(Path file, WalDurability durability) throws IOException {
        Objects.requireNonNull(durability, "durability");
        FileChannel ch = FileChannel.open(file,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND);
        long pos = ch.size();
        return new LogWriter(ch, durability, pos);
    }

    /** The durability mode this writer was opened with. */
    public WalDurability durability() {
        return durability;
    }

    /**
     * Append one logical record. Fragments it across blocks if needed.
     * The record may be zero-length (an empty payload still writes one FULL fragment).
     */
    public void append(byte[] payload) throws IOException {
        Objects.requireNonNull(payload, "payload");
        int offset = 0;
        int remaining = payload.length;
        boolean isFirstFragment = true;

        do {
            int blockLeft = WalConstants.BLOCK_SIZE - blockOffset;
            if (blockLeft < WalConstants.HEADER_SIZE) {
                // Not enough room for even a header — zero-pad the rest of the block.
                if (blockLeft > 0) {
                    byte[] zeros = new byte[blockLeft];
                    writeFully(ByteBuffer.wrap(zeros));
                }
                blockOffset = 0;
                blockLeft = WalConstants.BLOCK_SIZE;
            }

            int availForPayload = blockLeft - WalConstants.HEADER_SIZE;
            int fragmentLen = Math.min(remaining, availForPayload);
            boolean isLastFragment = (fragmentLen == remaining);
            RecordType type;
            if (isFirstFragment && isLastFragment) {
                type = RecordType.FULL;
            } else if (isFirstFragment) {
                type = RecordType.FIRST;
            } else if (isLastFragment) {
                type = RecordType.LAST;
            } else {
                type = RecordType.MIDDLE;
            }

            writeFragment(type, payload, offset, fragmentLen);
            isFirstFragment = false;
            offset += fragmentLen;
            remaining -= fragmentLen;
        } while (remaining > 0);

        if (durability instanceof WalDurability.Sync) {
            channel.force(true);
        }
        // Buffered: background flusher will catch up.
        // Disabled: never force.
    }

    /** Force a fsync now (bypasses the configured mode). */
    public void sync() throws IOException {
        channel.force(true);
    }

    /** Total bytes written through this writer (including any zero padding). */
    public long bytesWritten() {
        return bytesWritten;
    }

    /**
     * For Buffered mode: number of background fsyncs performed so far. Returns 0 for
     * other modes. Exposed for tests.
     */
    public long backgroundForceCount() {
        return flusher == null ? 0L : flusher.forceCount.get();
    }

    @Override
    public void close() throws IOException {
        IOException flusherErr = null;
        if (flusher != null) {
            try {
                flusher.shutdown();
            } catch (IOException e) {
                flusherErr = e;
            }
        }
        try {
            // Disabled: do not force on close — honour the contract that the embedder
            // owns durability. Sync / Buffered: drain to disk before closing.
            if (!(durability instanceof WalDurability.Disabled)) {
                channel.force(true);
            }
        } finally {
            channel.close();
        }
        if (flusherErr != null) {
            throw flusherErr;
        }
    }

    private void writeFragment(RecordType type, byte[] payload, int offset, int length) throws IOException {
        // Header layout: crc(4 LE) | length(2 LE) | type(1)
        ByteBuffer header = ByteBuffer.allocate(WalConstants.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        // Compute CRC over type + payload (matches RocksDb).
        crc.reset();
        crc.update(type.tag());
        if (length > 0) {
            crc.update(payload, offset, length);
        }
        int crcVal = (int) crc.getValue();
        header.putInt(crcVal);
        header.putShort((short) length);
        header.put(type.tag());
        header.flip();
        writeFully(header);
        if (length > 0) {
            writeFully(ByteBuffer.wrap(payload, offset, length));
        }
        blockOffset += WalConstants.HEADER_SIZE + length;
    }

    private void writeFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int written = channel.write(buf);
            if (written < 0) {
                throw new IOException("channel reported EOF on write");
            }
            bytesWritten += written;
        }
    }

    /**
     * Daemon thread that fsyncs the WAL channel periodically for Buffered mode. The
     * thread is a daemon so it never blocks JVM shutdown; {@link #shutdown()} is the
     * orderly stop. Any IOException from a periodic force is swallowed and surfaced on
     * shutdown (the close path also does a final force which will re-raise).
     */
    private static final class BackgroundFlusher extends Thread {
        private final FileChannel channel;
        private final long intervalMillis;
        private volatile boolean running = true;
        private volatile IOException lastError; // most recent force failure, if any
        final AtomicLong forceCount = new AtomicLong();

        BackgroundFlusher(FileChannel channel, long intervalMillis) {
            super("rocksdb-wal-flusher");
            this.channel = channel;
            this.intervalMillis = intervalMillis;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (!running) {
                    return;
                }
                try {
                    channel.force(true);
                    forceCount.incrementAndGet();
                } catch (IOException e) {
                    lastError = e;
                }
            }
        }

        void shutdown() throws IOException {
            running = false;
            this.interrupt();
            try {
                this.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (lastError != null) {
                throw lastError;
            }
        }
    }
}
