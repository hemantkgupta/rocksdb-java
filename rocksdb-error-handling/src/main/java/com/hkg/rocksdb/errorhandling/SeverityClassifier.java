package com.hkg.rocksdb.errorhandling;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Maps a thrown {@link Throwable} to one of three {@link Severity} buckets, per
 * FAST 2021 §5 and {@code docs/adr/0008-severity-based-error-handling.md}.
 *
 * <p>This class is the <em>only</em> code path in the engine that mints a
 * {@link Severity}. The ADR's positive consequence — "the classifier is small
 * enough to audit in one screen" — is load-bearing: a misclassification of a
 * HARD as TRANSIENT silently masks corruption, and a misclassification of
 * TRANSIENT as HARD makes the engine unnecessarily fragile.
 *
 * <h2>Classification rules</h2>
 *
 * <ol>
 *   <li>The optional override map (a class-name-substring &rarr; severity
 *       mapping passed to the constructor) is consulted first. The first
 *       substring whose target class-name appears in the thrown class's full
 *       name (including superclasses by re-walking the chain) wins. Used for
 *       tests and for engine modules that want to plug in their own
 *       custom-exception classifications without taking a build-time
 *       dependency on this module's enum.
 *   <li>{@code java.io.IOException} with a message matching {@link
 *       #isNoSpaceMessage(String) the ENOSPC heuristics} &rarr;
 *       {@link Severity#TRANSIENT_RETRYABLE}.
 *   <li>The thrown class (or any of its superclasses) has a simple name
 *       containing {@code "BlockChecksumMismatchException"} &rarr;
 *       {@link Severity#HARD_ERROR_READ_ONLY}. Matched by class-name
 *       <em>string</em> rather than {@code instanceof} so this module can stay
 *       dependency-free of {@code rocksdb-sstable-blockbased} (which carries
 *       the actual exception type). Done in the same spirit as Spring's
 *       {@code AbstractFallbackSQLExceptionTranslator} — string matching keeps
 *       the modules decoupled.
 *   <li>The thrown class (or any of its superclasses) has a simple name
 *       containing {@code "SsTableFormatException"} and the message contains
 *       {@code "footer"} &rarr; {@link Severity#FATAL_HALT}. The footer is
 *       the load-bearing structural anchor of an SSTable; if it does not
 *       parse, recovery cannot proceed.
 *   <li>Any other {@link IOException} &rarr; {@link Severity#TRANSIENT_RETRYABLE}
 *       (the §5 default: most I/O errors are transient and the retry budget is
 *       the cheap, correct first response).
 *   <li>Anything else &rarr; {@link Severity#FATAL_HALT}. This is the "panic"
 *       fall-through: a {@link RuntimeException} the engine did not anticipate
 *       (an invariant violation, an unchecked NPE on a recovery path) is by
 *       hypothesis a bug, and the safe action is to halt rather than scribble
 *       on the on-disk state under a broken assumption.
 * </ol>
 *
 * <p>The classifier is stateless and reentrant. A single instance is safely
 * shared across all threads in the engine.
 */
public final class SeverityClassifier {

    /** Insertion-ordered overrides; {@code null} when the no-arg constructor is used. */
    private final Map<String, Severity> overrides;

    /**
     * Default classifier with the §5 rules above and no overrides. Suitable
     * for production engine wiring.
     */
    public SeverityClassifier() {
        this.overrides = null;
    }

    /**
     * Classifier with a caller-supplied override map. The map's keys are
     * substrings to match against the thrown exception's class name (and its
     * superclasses); the first match wins. The map is defensively copied and
     * iteration order is preserved (so callers can express preference rules).
     *
     * <p>Intended for tests and for engine modules that want to add a
     * classification for their own exception types without taking a build-time
     * dependency on this module's enum.
     */
    public SeverityClassifier(Map<String, Severity> overrides) {
        Objects.requireNonNull(overrides, "overrides");
        // LinkedHashMap to preserve caller-specified precedence.
        this.overrides = Collections.unmodifiableMap(new LinkedHashMap<>(overrides));
    }

    /**
     * Classify the given throwable. Pure function; safe to call from any
     * thread.
     *
     * @param t the exception to classify; must not be {@code null}.
     * @return the {@link Severity} bucket for the engine's response.
     * @throws NullPointerException if {@code t} is {@code null}.
     */
    public Severity classify(Throwable t) {
        Objects.requireNonNull(t, "throwable");

        if (overrides != null) {
            Severity match = matchOverride(t);
            if (match != null) {
                return match;
            }
        }

        String message = t.getMessage();

        if (t instanceof IOException && isNoSpaceMessage(message)) {
            return Severity.TRANSIENT_RETRYABLE;
        }

        if (classHierarchyContains(t, "BlockChecksumMismatchException")) {
            return Severity.HARD_ERROR_READ_ONLY;
        }

        if (classHierarchyContains(t, "SsTableFormatException")
                && message != null
                && message.toLowerCase().contains("footer")) {
            return Severity.FATAL_HALT;
        }

        if (t instanceof IOException) {
            return Severity.TRANSIENT_RETRYABLE;
        }

        return Severity.FATAL_HALT;
    }

    // ---------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------

    private Severity matchOverride(Throwable t) {
        for (Map.Entry<String, Severity> entry : overrides.entrySet()) {
            if (classHierarchyContains(t, entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Walk the class hierarchy of {@code t} and return {@code true} if any
     * class name (simple or fully qualified) contains {@code needle}.
     *
     * <p>Used instead of {@code instanceof} to keep this module free of
     * dependencies on the sibling modules whose exception classes we want to
     * recognise (e.g. {@code rocksdb-sstable-blockbased},
     * {@code rocksdb-manifest}). Per the §5.7 plan note, the classifier
     * intentionally avoids those build-time edges.
     */
    private static boolean classHierarchyContains(Throwable t, String needle) {
        Class<?> c = t.getClass();
        while (c != null && c != Object.class) {
            // Check both simple and fully-qualified name so callers can target
            // either; the simple name is the common case.
            if (c.getName().contains(needle) || c.getSimpleName().contains(needle)) {
                return true;
            }
            c = c.getSuperclass();
        }
        return false;
    }

    /**
     * Heuristics for "this looks like an ENOSPC", drawn from the messages
     * Linux NIO raises and the wording POSIX shells echo. Kept as a single
     * helper so the test surface is small.
     */
    private static boolean isNoSpaceMessage(String message) {
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        return lower.contains("no space left")
                || lower.contains("enospc")
                || lower.contains("disk full")
                || lower.contains("disk quota exceeded");
    }
}
