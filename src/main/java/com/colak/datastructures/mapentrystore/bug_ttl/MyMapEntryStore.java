package com.colak.datastructures.mapentrystore.bug_ttl;

import com.hazelcast.map.EntryStore;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
class MyMapEntryStore implements EntryStore<String, MyRecord> {

    private final ConcurrentHashMap<String, MetadataAwareValue<MyRecord>> store = new ConcurrentHashMap<>();

    public int size () {
        return store.size();
    }

    @Override
    public void store(String key, MetadataAwareValue<MyRecord> value) {
        log.info("store is called with expiration time {}", value.getExpirationTime());
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<String, MetadataAwareValue<MyRecord>> map) {
        log.info("storeAll is called");
    }

    @Override
    public void delete(String key) {
        log.info("delete is called");
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        log.info("deleteAll is called");
    }

    @Override
    public MetadataAwareValue<MyRecord> load(String key) {
        log.info("load is called");
        return store.get(key);
    }

    @Override
    public Map<String, MetadataAwareValue<MyRecord>> loadAll(Collection<String> keys) {
        log.info("loadAll is called");
        return null;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        log.info("loadAllKeys is called");
        return null;
    }
}
