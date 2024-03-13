package com.colak.datastructures.mapentrystore;

import com.hazelcast.map.EntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MyMapEntryStore implements EntryStore<String, MyRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyMapEntryStore.class);
    private final ConcurrentHashMap<String, MetadataAwareValue<MyRecord>> store = new ConcurrentHashMap<>();

    @Override
    public void store(String key, MetadataAwareValue<MyRecord> value) {
        LOGGER.info("store is called with expiration time {}", value.getExpirationTime());
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<String, MetadataAwareValue<MyRecord>> map) {
        LOGGER.info("storeAll is called");
    }

    @Override
    public void delete(String key) {
        LOGGER.info("delete is called");
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        LOGGER.info("deleteAll is called");
    }

    @Override
    public MetadataAwareValue<MyRecord> load(String key) {
        LOGGER.info("load is called");
        return store.get(key);
    }

    @Override
    public Map<String, MetadataAwareValue<MyRecord>> loadAll(Collection<String> keys) {
        LOGGER.info("loadAll is called");
        return null;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        LOGGER.info("loadAllKeys is called");
        return null;
    }
}
