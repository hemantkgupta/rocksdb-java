package com.hkg.rocksdb.errorhandling;

/**
 * Thrown by {@link ReadOnlyModeLatch#ensureWritable()} when the latch has been
 * tripped by a prior {@link Severity#HARD_ERROR_READ_ONLY} failure.
 *
 * <p>Extends {@link IllegalStateException} so that callers who do not want to
 * special-case the read-only contract still see a typed unchecked failure
 * rather than a wrapped {@code IOException}. The message carries the latch's
 * last-known {@linkplain ReadOnlyModeLatch#reason() reason}, matching the
 * RocksDB §5 convention of surfacing the originating cause through every layer
 * that re-raises the read-only fault.
 */
public class ReadOnlyModeException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    private final String reason;

    public ReadOnlyModeException(String reason) {
        super(formatMessage(reason));
        this.reason = reason;
    }

    /** Last-known reason the latch was tripped, or {@code null} if unknown. */
    public String reason() {
        return reason;
    }

    private static String formatMessage(String reason) {
        if (reason == null) {
            return "db is in read-only mode";
        }
        return "db is in read-only mode: " + reason;
    }
}
