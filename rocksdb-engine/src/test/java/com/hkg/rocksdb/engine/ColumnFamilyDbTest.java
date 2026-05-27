package com.hkg.rocksdb.engine;

import com.hkg.rocksdb.columnfamily.ColumnFamilyHandle;
import com.hkg.rocksdb.columnfamily.ColumnFamilyOptions;
import com.hkg.rocksdb.columnfamily.CompactionStyle;
import com.hkg.rocksdb.common.Key;
import com.hkg.rocksdb.common.Slice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CP 14 engine tests — verify the application-facing CF semantics:
 * isolation, drop, reopen, per-CF options preserved. Deliberately tests
 * the CP-14b scope (layered, per-CF subdirectory); the shared-WAL /
 * shared-MANIFEST refactor lands later.
 */
class ColumnFamilyDbTest {

    @Test
    void freshDbOpensWithOnlyTheDefaultCf(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            assertThat(db.columnFamilies()).hasSize(1);
            assertThat(db.defaultColumnFamily().name()).isEqualTo("default");
        }
    }

    @Test
    void createColumnFamilyMintsAndPersists(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            ColumnFamilyHandle metrics = db.createColumnFamily("metrics",
                ColumnFamilyOptions.tiered());
            assertThat(metrics.options().compactionStyle()).isEqualTo(CompactionStyle.TIERED);
            assertThat(db.columnFamilies()).hasSize(2);
            // Subdir exists.
            assertThat(Files.exists(dir.resolve(String.format("cf-%06d", metrics.id().value()))))
                .isTrue();
        }
    }

    @Test
    void twoCfsIsolateTheirWrites(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            ColumnFamilyHandle def = db.defaultColumnFamily();
            ColumnFamilyHandle metrics = db.createColumnFamily("metrics", ColumnFamilyOptions.leveled());

            db.put(def, Key.of("k"), Slice.of("from-default"));
            db.put(metrics, Key.of("k"), Slice.of("from-metrics"));

            assertThat(new String(db.get(def, Key.of("k")).get().toBytes()))
                .isEqualTo("from-default");
            assertThat(new String(db.get(metrics, Key.of("k")).get().toBytes()))
                .isEqualTo("from-metrics");

            // Delete from one doesn't affect the other.
            db.delete(def, Key.of("k"));
            assertThat(db.get(def, Key.of("k"))).isEmpty();
            assertThat(db.get(metrics, Key.of("k"))).isPresent();
        }
    }

    @Test
    void cfStatePersistsAcrossReopen(@TempDir Path dir) throws IOException {
        ColumnFamilyHandle metricsHandle;
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            metricsHandle = db.createColumnFamily("metrics", ColumnFamilyOptions.tiered());
            db.put(metricsHandle, Key.of("cpu"), Slice.of("99%"));
            db.flush(metricsHandle);
        }
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            assertThat(db.columnFamilies()).hasSize(2);
            ColumnFamilyHandle recovered = db.getColumnFamily("metrics").orElseThrow();
            assertThat(recovered.options().compactionStyle()).isEqualTo(CompactionStyle.TIERED);
            assertThat(recovered.id()).isEqualTo(metricsHandle.id());
            // Data survived too.
            assertThat(new String(db.get(recovered, Key.of("cpu")).get().toBytes()))
                .isEqualTo("99%");
        }
    }

    @Test
    void dropColumnFamilyRemovesSstables(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            ColumnFamilyHandle tmp = db.createColumnFamily("scratch", ColumnFamilyOptions.leveled());
            for (int i = 0; i < 200; i++) {
                db.put(tmp, Key.of("k-" + i), Slice.of("v-" + i));
            }
            db.flush(tmp);

            Path cfDir = dir.resolve(String.format("cf-%06d", tmp.id().value()));
            assertThat(Files.list(cfDir).filter(p -> p.getFileName().toString().endsWith(".sst"))
                .count()).isGreaterThanOrEqualTo(1L);

            db.dropColumnFamily(tmp.id());
            assertThat(Files.exists(cfDir)).isFalse();
            assertThat(db.getColumnFamily("scratch")).isEmpty();
        }
    }

    @Test
    void droppedCfIsForgottenAcrossReopen(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            ColumnFamilyHandle ephemeral = db.createColumnFamily("ephemeral",
                ColumnFamilyOptions.defaults());
            db.put(ephemeral, Key.of("k"), Slice.of("v"));
            db.flush(ephemeral);
            db.dropColumnFamily(ephemeral.id());
        }
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            assertThat(db.columnFamilies()).hasSize(1);
            assertThat(db.getColumnFamily("ephemeral")).isEmpty();
        }
    }

    @Test
    void cannotDropDefault(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            assertThatThrownBy(() -> db.dropColumnFamily(db.defaultColumnFamily().id()))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void duplicateCreateRejected(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            db.createColumnFamily("a", ColumnFamilyOptions.defaults());
            assertThatThrownBy(() -> db.createColumnFamily("a", ColumnFamilyOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void operatingOnDroppedHandleFails(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            ColumnFamilyHandle h = db.createColumnFamily("doomed", ColumnFamilyOptions.defaults());
            db.dropColumnFamily(h.id());
            assertThatThrownBy(() -> db.get(h, Key.of("k")))
                .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void perCfOptionsOfDifferentStylesPreserved(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            db.createColumnFamily("leveled-cf", ColumnFamilyOptions.leveled());
            db.createColumnFamily("tiered-cf", ColumnFamilyOptions.tiered());
            db.createColumnFamily("fifo-cf", ColumnFamilyOptions.fifo(1L << 20));
        }
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            assertThat(db.getColumnFamily("leveled-cf").orElseThrow().options().compactionStyle())
                .isEqualTo(CompactionStyle.LEVELED);
            assertThat(db.getColumnFamily("tiered-cf").orElseThrow().options().compactionStyle())
                .isEqualTo(CompactionStyle.TIERED);
            ColumnFamilyHandle fifo = db.getColumnFamily("fifo-cf").orElseThrow();
            assertThat(fifo.options().compactionStyle()).isEqualTo(CompactionStyle.FIFO);
            assertThat(fifo.options().fifoMaxBytes()).isEqualTo(1L << 20);
        }
    }

    @Test
    void compactRunsOnSpecificCf(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            ColumnFamilyHandle def = db.defaultColumnFamily();
            ColumnFamilyHandle metrics = db.createColumnFamily("metrics", ColumnFamilyOptions.leveled());

            // Drop 5 L0 files into each CF.
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 20; i++) {
                    db.put(def, Key.of("d-" + batch + "-" + i), Slice.of("dv-" + i));
                    db.put(metrics, Key.of("m-" + batch + "-" + i), Slice.of("mv-" + i));
                }
                db.flush(def);
                db.flush(metrics);
            }

            int defJobs = db.compact(def, 1);
            assertThat(defJobs).isEqualTo(1);
            // metrics still has its L0 backlog — its compaction is independent.
            assertThat(db.engineFor(metrics).currentVersion().level(0).size())
                .isGreaterThanOrEqualTo(5);

            int metricsJobs = db.compact(metrics, 1);
            assertThat(metricsJobs).isEqualTo(1);
        }
    }

    @Test
    void getMissingCfReturnsEmptyOptional(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            assertThat(db.getColumnFamily("never-created")).isEmpty();
        }
    }

    @Test
    void writesInDefaultCfWorkWithoutCreatingAnyOther(@TempDir Path dir) throws IOException {
        try (ColumnFamilyDb db = ColumnFamilyDb.open(dir)) {
            ColumnFamilyHandle def = db.defaultColumnFamily();
            db.put(def, Key.of("k"), Slice.of("v"));
            Optional<Slice> got = db.get(def, Key.of("k"));
            assertThat(got).isPresent();
            assertThat(new String(got.get().toBytes())).isEqualTo("v");
        }
    }
}
