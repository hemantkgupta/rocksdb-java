package com.hkg.rocksdb.wal;

/**
 * Per-write WAL durability mode. ADR 0004.
 *
 * <p>Three implementations, selected per {@code WriteOptions}:
 * <ul>
 *   <li>{@link Sync} — append + {@code FileChannel.force(true)} before ack. The WAL is the
 *       durability boundary. OLTP default.</li>
 *   <li>{@link Buffered} — append returns immediately; a background daemon thread fsyncs
 *       every {@code flushIntervalMillis} (default 100 ms). On crash, lose &le; that
 *       window of acked writes.</li>
 *   <li>{@link Disabled} — never fsync. Bytes are still written to the channel, but the
 *       embedder's higher-level log (consensus, replication) is the durability point.
 *       On crash, every post-last-natural-flush write is lost.</li>
 * </ul>
 *
 * <p>Mode is a process-side knob; nothing about it is recorded on disk. Sync and Buffered
 * WALs are byte-identical; only ack timing differs.
 */
public sealed interface WalDurability
        permits WalDurability.Sync, WalDurability.Buffered, WalDurability.Disabled {

    /** Default Buffered flush interval (ms). */
    long DEFAULT_FLUSH_INTERVAL_MS = 100L;

    /** Singleton convenience for Sync. */
    static Sync sync() {
        return Sync.INSTANCE;
    }

    /** Singleton convenience for Disabled. */
    static Disabled disabled() {
        return Disabled.INSTANCE;
    }

    /** Buffered with the default 100 ms flush interval. */
    static Buffered buffered() {
        return new Buffered(DEFAULT_FLUSH_INTERVAL_MS);
    }

    /** Buffered with an explicit flush interval. */
    static Buffered buffered(long flushIntervalMillis) {
        return new Buffered(flushIntervalMillis);
    }

    /** Sync after every append. */
    record Sync() implements WalDurability {
        public static final Sync INSTANCE = new Sync();
    }

    /**
     * Buffered: ack immediately, background fsync every {@code flushIntervalMillis}.
     * On crash, up to {@code flushIntervalMillis} of acked writes may be lost.
     */
    record Buffered(long flushIntervalMillis) implements WalDurability {
        public Buffered {
            if (flushIntervalMillis <= 0) {
                throw new IllegalArgumentException(
                        "flushIntervalMillis must be > 0, was " + flushIntervalMillis);
            }
        }
    }

    /**
     * Disabled: never fsync. Bytes are written to the channel but the OS may buffer them
     * arbitrarily long. The embedder must own a higher-level durability story.
     */
    record Disabled() implements WalDurability {
        public static final Disabled INSTANCE = new Disabled();
    }
}
