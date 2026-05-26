package com.hkg.rocksdb.ratelimiter;

/**
 * Caller priority class for {@link RateLimiter#request(long, Priority)}.
 *
 * <p>RocksDB exposes two classes because compaction throughput (background) and
 * foreground writes (latency-sensitive) want fundamentally different scheduling
 * treatment when they share a single budget:
 *
 * <ul>
 *   <li>{@link #HIGH} — foreground writers and WAL flushes. These callers
 *       preempt: when a HIGH request arrives, queued LOW waiters yield their
 *       reservations and let the HIGH caller take the next available tokens.
 *   <li>{@link #LOW} — background compaction I/O and file-deletion bytes. These
 *       callers consume the leftover bandwidth that HIGH did not use.
 * </ul>
 *
 * <p>FAST 2021 §5.1 motivates the split: a host-wide compaction budget that
 * starves foreground writes is worse than no budget at all, because it raises
 * tail latency without giving the operator a knob to back off.
 */
public enum Priority {
    HIGH,
    LOW
}
