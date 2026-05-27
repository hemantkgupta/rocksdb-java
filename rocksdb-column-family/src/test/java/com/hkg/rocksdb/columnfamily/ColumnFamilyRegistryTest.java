package com.hkg.rocksdb.columnfamily;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnFamilyRegistryTest {

    @Test
    void freshRegistryHasOnlyTheDefaultCf() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        assertThat(r.size()).isEqualTo(1);
        ColumnFamilyHandle def = r.defaultColumnFamily();
        assertThat(def.id()).isEqualTo(ColumnFamilyId.DEFAULT);
        assertThat(def.name()).isEqualTo(ColumnFamilyHandle.DEFAULT_NAME);
        assertThat(def.options().compactionStyle()).isEqualTo(CompactionStyle.LEVELED);
    }

    @Test
    void createMintsMonotonicIds() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        ColumnFamilyHandle a = r.create("a", ColumnFamilyOptions.tiered());
        ColumnFamilyHandle b = r.create("b", ColumnFamilyOptions.leveled());
        assertThat(a.id().value()).isEqualTo(1);
        assertThat(b.id().value()).isEqualTo(2);
        assertThat(a.options().compactionStyle()).isEqualTo(CompactionStyle.TIERED);
        assertThat(b.options().compactionStyle()).isEqualTo(CompactionStyle.LEVELED);
    }

    @Test
    void duplicateNameRejected() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        r.create("dup", ColumnFamilyOptions.defaults());
        assertThatThrownBy(() -> r.create("dup", ColumnFamilyOptions.defaults()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cannotCreateAnotherDefault() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        assertThatThrownBy(() -> r.create(ColumnFamilyHandle.DEFAULT_NAME,
            ColumnFamilyOptions.defaults()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lookupByNameAndById() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        ColumnFamilyHandle h = r.create("metrics", ColumnFamilyOptions.tiered());
        assertThat(r.get("metrics")).hasValue(h);
        assertThat(r.get(h.id())).hasValue(h);
        assertThat(r.get("missing")).isEmpty();
        assertThat(r.get(new ColumnFamilyId(99))).isEmpty();
    }

    @Test
    void dropRemovesCf() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        ColumnFamilyHandle h = r.create("a", ColumnFamilyOptions.defaults());
        r.drop(h.id());
        assertThat(r.get("a")).isEmpty();
        assertThat(r.get(h.id())).isEmpty();
        assertThat(r.size()).isEqualTo(1); // only default remains
    }

    @Test
    void cannotDropDefault() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        assertThatThrownBy(() -> r.drop(ColumnFamilyId.DEFAULT))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cannotDropMissingId() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        assertThatThrownBy(() -> r.drop(new ColumnFamilyId(99)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registerExistingReplaysToWatermark() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        r.registerExisting(new ColumnFamilyId(5), "five", ColumnFamilyOptions.fifo(1L << 20));
        // Next create() must mint id > 5.
        ColumnFamilyHandle h = r.create("six", ColumnFamilyOptions.defaults());
        assertThat(h.id().value()).isEqualTo(6);
    }

    @Test
    void allReturnsSnapshot() {
        ColumnFamilyRegistry r = new ColumnFamilyRegistry();
        r.create("a", ColumnFamilyOptions.defaults());
        r.create("b", ColumnFamilyOptions.defaults());
        assertThat(r.all()).hasSize(3); // default + a + b
        // Mutating the registry after snapshot doesn't affect the snapshot.
        var snap = r.all();
        r.create("c", ColumnFamilyOptions.defaults());
        assertThat(snap).hasSize(3);
        assertThat(r.all()).hasSize(4);
    }

    @Test
    void defaultIdRejectsNegativeIds() {
        assertThatThrownBy(() -> new ColumnFamilyId(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void optionsRejectsNegativeFifoCap() {
        assertThatThrownBy(() -> new ColumnFamilyOptions(CompactionStyle.FIFO, -1L))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void handleRejectsEmptyName() {
        assertThatThrownBy(() -> new ColumnFamilyHandle(new ColumnFamilyId(1), "",
            ColumnFamilyOptions.defaults()))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
