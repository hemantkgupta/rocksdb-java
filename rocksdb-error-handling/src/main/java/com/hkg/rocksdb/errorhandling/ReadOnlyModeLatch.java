package com.hkg.rocksdb.errorhandling;

/**
 * One-way latch that flips the engine from writable to read-only when a
 * {@link Severity#HARD_ERROR_READ_ONLY} fault is detected.
 *
 * <p>Per FAST 2021 §5 and {@code docs/adr/0008-severity-based-error-handling.md},
 * a HARD error is the operator's signal to investigate before any more bytes
 * touch the bad data path. The latch encodes that contract:
 *
 * <ul>
 *   <li>{@link #trip(String)} flips the latch on; subsequent calls are
 *       idempotent but the latest reason wins for {@link #reason()} read-back
 *       (operators tracing a cascade want to see the most recent cause).
 *   <li>{@link #ensureWritable()} throws {@link ReadOnlyModeException} when
 *       tripped — the single guard every write path consults.
 *   <li>{@link #attemptResume()} clears the latch. It is named {@code attempt}
 *       to flag that the §5 design intent is operator-driven recovery: the
 *       method does not itself verify the underlying condition has cleared,
 *       only that the operator has chosen to retry. Auto-resume on a timer is
 *       explicitly rejected by the ADR.
 * </ul>
 *
 * <h2>Concurrency</h2>
 *
 * <p>The {@code tripped} flag is {@code volatile} so {@link #isTripped()} and
 * {@link #ensureWritable()} are lock-free fast paths. Mutations
 * ({@link #trip(String)}, {@link #attemptResume()}) synchronise on the
 * instance to keep the {@code tripped}/{@code reason} pair coherent — a
 * concurrent observer never sees {@code tripped=true} with a stale or absent
 * reason.
 */
public final class ReadOnlyModeLatch {

    private volatile boolean tripped;
    private volatile String reason;

    /**
     * Flip the latch on. Idempotent: tripping an already-tripped latch is
     * safe and updates {@link #reason()} so cascading failures leave the
     * latest cause visible.
     *
     * @param reason short human-readable description; may be {@code null}.
     */
    public synchronized void trip(String reason) {
        this.reason = reason;
        this.tripped = true;
    }

    /** @return {@code true} once {@link #trip(String)} has been called and not yet resumed. */
    public boolean isTripped() {
        return tripped;
    }

    /** @return last-known reason, or {@code null} if never tripped (or resumed). */
    public String reason() {
        return reason;
    }

    /**
     * Throw {@link ReadOnlyModeException} if the latch is tripped. The single
     * guard every write path consults; intentionally lock-free.
     */
    public void ensureWritable() {
        if (tripped) {
            throw new ReadOnlyModeException(reason);
        }
    }

    /**
     * Clear the latch. Operator-driven recovery only — see the class-level
     * note on why auto-resume on a timer is explicitly rejected.
     */
    public synchronized void attemptResume() {
        this.tripped = false;
        this.reason = null;
    }
}
