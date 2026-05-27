package com.hkg.rocksdb.errorhandling;

/**
 * Three-bucket severity classification for engine-thrown exceptions, per FAST
 * 2021 §5 and {@code docs/adr/0008-severity-based-error-handling.md}.
 *
 * <p>The engine's response to an error encodes a hypothesis about the error's
 * cause. Each bucket corresponds to a distinct response:
 *
 * <ul>
 *   <li>{@link #TRANSIENT_RETRYABLE} — the error is hypothesised to clear on
 *       its own (full disk that an operator is freeing, brief I/O blip on a
 *       remote {@code FileSystem}). The engine retries with backoff and
 *       surfaces the error to the caller only after N retries fail.
 *   <li>{@link #HARD_ERROR_READ_ONLY} — the error is hypothesised to indicate
 *       a real on-disk problem (block CRC mismatch, KV-checksum mismatch,
 *       {@code EIO} from the FS) but the engine can still safely serve reads.
 *       Writes are refused via the {@link ReadOnlyModeLatch}; only an explicit
 *       {@code db.attemptResume()} clears the latch.
 *   <li>{@link #FATAL_HALT} — the error is hypothesised to make the on-disk
 *       state uninterpretable (MANIFEST CRC mismatch, footer magic mismatch
 *       on the active SSTable index). The engine refuses to remain open; the
 *       embedding process must restart with the operator's triage.
 * </ul>
 */
public enum Severity {

    /** Retryable error; engine handles transparently within the retry budget. */
    TRANSIENT_RETRYABLE,

    /** Permanent error confined to one file/block; engine trips read-only. */
    HARD_ERROR_READ_ONLY,

    /** Engine state no longer interpretable; embedding process must restart. */
    FATAL_HALT
}
