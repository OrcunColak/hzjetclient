package com.colak.datastructures.mapentrystore.writebehind;

import com.hazelcast.map.EntryStore;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
class MyMapEntryStore implements EntryStore<Integer, MyRecord> {

    private final ConcurrentHashMap<Integer, MetadataAwareValue<MyRecord>> store = new ConcurrentHashMap<>(
            Map.of(0, new MetadataAwareValue<>(new MyRecord("10")))
    );

    public int size() {
        return store.size();
    }

    @Override
    public void store(Integer key, MetadataAwareValue<MyRecord> value) {
        log.info("Storing Key : {} , Value : {}", key, value);
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<Integer, MetadataAwareValue<MyRecord>> map) {
        log.info("storeAll map size : {}", map.size());
    }

    @Override
    public void delete(Integer key) {
        log.info("Deleting Key : {}", key);
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<Integer> keys) {
        log.info("deleteAll map size : {}", keys.size());
    }

    @Override
    public MetadataAwareValue<MyRecord> load(Integer key) {
        log.info("Loading Key : {} ", key);
        // Do not load anything
        return store.get(key);
    }

    @Override
    public Map<Integer, MetadataAwareValue<MyRecord>> loadAll(Collection<Integer> keys) {
        // Do not load anything
        return null;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        // Do not load anything
        return null;
    }
}
