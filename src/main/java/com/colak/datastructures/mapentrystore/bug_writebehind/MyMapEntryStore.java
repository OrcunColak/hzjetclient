package com.colak.datastructures.mapentrystore.bug_writebehind;

import com.hazelcast.map.EntryStore;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MyMapEntryStore implements EntryStore<Integer, MyRecord> {

    private final ConcurrentHashMap<Integer, MetadataAwareValue<MyRecord>> store = new ConcurrentHashMap<>();

    public int size () {
        return store.size();
    }

    @Override
    public void store(Integer key, MetadataAwareValue<MyRecord> value) {
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<Integer, MetadataAwareValue<MyRecord>> map) {
    }

    @Override
    public void delete(Integer key) {
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<Integer> keys) {
    }

    @Override
    public MetadataAwareValue<MyRecord> load(Integer key) {
        return store.get(key);
    }

    @Override
    public Map<Integer, MetadataAwareValue<MyRecord>> loadAll(Collection<Integer> keys) {
        return null;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        return null;
    }
}
