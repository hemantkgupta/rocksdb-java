package com.hkg.rocksdb.columnfamily;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory map of {@link ColumnFamilyHandle}s, keyed by both name and id.
 * The default CF is registered at construction time with {@link
 * ColumnFamilyId#DEFAULT}; subsequent {@link #create} calls mint
 * monotonically increasing positive ids.
 *
 * <p>The registry is the engine's single source of truth at runtime; on
 * startup the engine rebuilds it from {@code AddColumnFamily} /
 * {@code DropColumnFamily} edits replayed out of the MANIFEST. This class
 * itself has no persistence — the engine layers that on.
 *
 * <p>Concurrency: all mutating operations synchronise on the instance; reads
 * iterate over thread-safe snapshots.
 */
public final class ColumnFamilyRegistry {

    private final Map<String, ColumnFamilyHandle> byName = new LinkedHashMap<>();
    private final Map<ColumnFamilyId, ColumnFamilyHandle> byId = new LinkedHashMap<>();
    private final AtomicInteger nextId;

    public ColumnFamilyRegistry() {
        this.nextId = new AtomicInteger(1);
        // Seed with the implicit default CF. Every engine has one.
        ColumnFamilyHandle def = ColumnFamilyHandle.defaultHandle();
        byName.put(def.name(), def);
        byId.put(def.id(), def);
    }

    /** Create a new CF. Throws if {@code name} is already taken. */
    public synchronized ColumnFamilyHandle create(String name, ColumnFamilyOptions options) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(options, "options");
        if (byName.containsKey(name)) {
            throw new IllegalArgumentException("ColumnFamily already exists: " + name);
        }
        ColumnFamilyId id = new ColumnFamilyId(nextId.getAndIncrement());
        ColumnFamilyHandle h = new ColumnFamilyHandle(id, name, options);
        byName.put(name, h);
        byId.put(id, h);
        return h;
    }

    /**
     * Replay-time helper: register a CF at a specific id (recovered from a
     * MANIFEST {@code AddColumnFamily} edit). Bumps the next-id watermark so
     * subsequent {@link #create} calls don't collide. Throws if the id is
     * already taken.
     */
    public synchronized ColumnFamilyHandle registerExisting(ColumnFamilyId id, String name,
                                                              ColumnFamilyOptions options) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(options, "options");
        if (byId.containsKey(id)) {
            throw new IllegalArgumentException("ColumnFamilyId already registered: " + id.value());
        }
        if (byName.containsKey(name)) {
            throw new IllegalArgumentException("ColumnFamily name already registered: " + name);
        }
        ColumnFamilyHandle h = new ColumnFamilyHandle(id, name, options);
        byName.put(name, h);
        byId.put(id, h);
        if (id.value() >= nextId.get()) {
            nextId.set(id.value() + 1);
        }
        return h;
    }

    /** Drop a CF by id. Returns the removed handle; the default CF cannot be dropped. */
    public synchronized ColumnFamilyHandle drop(ColumnFamilyId id) {
        Objects.requireNonNull(id, "id");
        if (id.equals(ColumnFamilyId.DEFAULT)) {
            throw new IllegalArgumentException("cannot drop the default column family");
        }
        ColumnFamilyHandle h = byId.remove(id);
        if (h == null) {
            throw new IllegalArgumentException("ColumnFamilyId not registered: " + id.value());
        }
        byName.remove(h.name());
        return h;
    }

    public synchronized Optional<ColumnFamilyHandle> get(String name) {
        return Optional.ofNullable(byName.get(name));
    }

    public synchronized Optional<ColumnFamilyHandle> get(ColumnFamilyId id) {
        return Optional.ofNullable(byId.get(id));
    }

    /** The default CF — always present. */
    public ColumnFamilyHandle defaultColumnFamily() {
        return get(ColumnFamilyId.DEFAULT).orElseThrow();
    }

    /** Defensive snapshot of every registered CF, in insertion order. */
    public synchronized Collection<ColumnFamilyHandle> all() {
        return Collections.unmodifiableCollection(new LinkedHashMap<>(byId).values());
    }

    public synchronized int size() {
        return byId.size();
    }
}
