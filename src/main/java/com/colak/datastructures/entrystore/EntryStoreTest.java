package com.colak.datastructures.entrystore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class EntryStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntryStoreTest.class);

    private static final String MAP_NAME = "mymap";

    private static final String KEY = "1";

    public static void main(String[] args) {
        LOGGER.info("Starting HZ Client");

        // Start server
        getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastInstanceClient = getHazelcastClientInstanceByConfig();

        // Do test
        testEntryStoreTtl(hazelcastInstanceClient);
    }

    public static void getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);

        MyEntryStore myEntryStore = new MyEntryStore();
        mapStoreConfig.setImplementation(myEntryStore);

        Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    // Test that map entry expires. Expiration value is passed to EntryStore too
    // But EntryStore#delete is not triggered when entry expires
    private static void testEntryStoreTtl(HazelcastInstance hazelcastInstanceClient) {
        IMap<String, MyRecord> map = hazelcastInstanceClient.getMap(MAP_NAME);

        EntryAdapter<String, MyRecord> stringMyRecordEntryAdapter = new EntryAdapter<>() {
            @Override
            public void entryExpired(EntryEvent<String, MyRecord> event) {
                LOGGER.info("entryExpired {} {}", event.getOldValue(), event.getValue());
            }
        };

        map.addEntryListener(stringMyRecordEntryAdapter, true);

        MyRecord value = new MyRecord("value1");

        map.put(KEY, value, 5, TimeUnit.SECONDS);
        LOGGER.info("map Size {}", map.size());
        map.replace(KEY, value, new MyRecord("value2"));
        map.setTtl(KEY, 5, TimeUnit.SECONDS);

        LOGGER.info("testEntryStore finished");
    }
}
