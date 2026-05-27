package com.hkg.rocksdb.errorhandling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SeverityClassifier}.
 *
 * <p>The classifier is the single point that maps exceptions to severities, so
 * every classification rule is exercised explicitly. The custom-exception
 * subclasses defined here mirror the names the engine's sibling modules expose
 * — they let us test class-name-substring matching without taking a build-time
 * dependency on those modules.
 */
final class SeverityClassifierTest {

    private final SeverityClassifier classifier = new SeverityClassifier();

    @Test
    void enospcMessageOnIOExceptionIsTransient() {
        IOException e = new IOException("No space left on device");
        assertThat(classifier.classify(e)).isEqualTo(Severity.TRANSIENT_RETRYABLE);
    }

    @Test
    void enospcSynonymsAlsoTransient() {
        assertThat(classifier.classify(new IOException("ENOSPC: write failed")))
                .isEqualTo(Severity.TRANSIENT_RETRYABLE);
        assertThat(classifier.classify(new IOException("disk full")))
                .isEqualTo(Severity.TRANSIENT_RETRYABLE);
        assertThat(classifier.classify(new IOException("Disk quota exceeded")))
                .isEqualTo(Severity.TRANSIENT_RETRYABLE);
    }

    @Test
    void blockChecksumMismatchByClassNameIsHard() {
        BlockChecksumMismatchException e = new BlockChecksumMismatchException("CRC mismatch in block 42");
        assertThat(classifier.classify(e)).isEqualTo(Severity.HARD_ERROR_READ_ONLY);
    }

    @Test
    void ssTableFormatExceptionWithFooterIsFatal() {
        SsTableFormatException e = new SsTableFormatException("footer magic mismatch");
        assertThat(classifier.classify(e)).isEqualTo(Severity.FATAL_HALT);
    }

    @Test
    void ssTableFormatExceptionWithoutFooterFallsThroughToIoDefault() {
        // The narrow "footer" rule is FATAL; other SSTable parse errors are
        // generic IOException and so are TRANSIENT under §5's default.
        SsTableFormatException e = new SsTableFormatException("unexpected block trailer length");
        assertThat(classifier.classify(e)).isEqualTo(Severity.TRANSIENT_RETRYABLE);
    }

    @Test
    void genericIoExceptionDefaultsToTransient() {
        IOException e = new IOException("brief network hiccup");
        assertThat(classifier.classify(e)).isEqualTo(Severity.TRANSIENT_RETRYABLE);
    }

    @Test
    void genericRuntimeExceptionIsFatal() {
        RuntimeException e = new RuntimeException("unexpected invariant violation");
        assertThat(classifier.classify(e)).isEqualTo(Severity.FATAL_HALT);
    }

    @Test
    void unanticipatedThrowableIsFatal() {
        // Plain Throwable that is neither IOException nor RuntimeException —
        // still falls through to the FATAL panic bucket.
        Throwable t = new Throwable("never seen this before");
        assertThat(classifier.classify(t)).isEqualTo(Severity.FATAL_HALT);
    }

    @Test
    void nullThrowableThrowsNullPointerException() {
        assertThatThrownBy(() -> classifier.classify(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("throwable");
    }

    @Test
    void overrideMapHonoured() {
        Map<String, Severity> overrides = new LinkedHashMap<>();
        overrides.put("CustomTransient", Severity.TRANSIENT_RETRYABLE);
        overrides.put("CustomFatal", Severity.FATAL_HALT);
        SeverityClassifier custom = new SeverityClassifier(overrides);

        assertThat(custom.classify(new CustomTransient("nope"))).isEqualTo(Severity.TRANSIENT_RETRYABLE);
        assertThat(custom.classify(new CustomFatal("nope"))).isEqualTo(Severity.FATAL_HALT);
    }

    @Test
    void overrideMapHasPrecedenceOverDefaultRules() {
        // A generic IOException would default to TRANSIENT; an override that
        // matches the class name should win and re-classify it as HARD.
        Map<String, Severity> overrides = Map.of("IOException", Severity.HARD_ERROR_READ_ONLY);
        SeverityClassifier custom = new SeverityClassifier(overrides);
        assertThat(custom.classify(new IOException("anything"))).isEqualTo(Severity.HARD_ERROR_READ_ONLY);
    }

    @Test
    void overrideMatchesViaSuperclassName() {
        // Subclasses of an overridden type are matched via the hierarchy walk.
        Map<String, Severity> overrides = Map.of("CustomTransient", Severity.TRANSIENT_RETRYABLE);
        SeverityClassifier custom = new SeverityClassifier(overrides);
        assertThat(custom.classify(new SubclassOfCustomTransient("x")))
                .isEqualTo(Severity.TRANSIENT_RETRYABLE);
    }

    // ------------------------------------------------------------------
    // Test-local exception classes — chosen for their *names* (the
    // classifier matches on class-name substring rather than instanceof to
    // stay dep-free of rocksdb-sstable-blockbased and rocksdb-manifest).
    // ------------------------------------------------------------------

    static final class BlockChecksumMismatchException extends IOException {
        private static final long serialVersionUID = 1L;
        BlockChecksumMismatchException(String message) { super(message); }
    }

    static final class SsTableFormatException extends IOException {
        private static final long serialVersionUID = 1L;
        SsTableFormatException(String message) { super(message); }
    }

    static class CustomTransient extends RuntimeException {
        private static final long serialVersionUID = 1L;
        CustomTransient(String message) { super(message); }
    }

    static final class SubclassOfCustomTransient extends CustomTransient {
        private static final long serialVersionUID = 1L;
        SubclassOfCustomTransient(String message) { super(message); }
    }

    static final class CustomFatal extends RuntimeException {
        private static final long serialVersionUID = 1L;
        CustomFatal(String message) { super(message); }
    }
}
