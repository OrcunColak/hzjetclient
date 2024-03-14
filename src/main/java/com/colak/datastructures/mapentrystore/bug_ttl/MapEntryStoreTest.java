package com.colak.datastructures.mapentrystore.bug_ttl;

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
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This example shows that expired entry still remains in EntryStore
 */
@Slf4j
class MapEntryStoreTest {

    private static final String MAP_NAME = "mymap";

    private static final String KEY = "1";

    private static final MyMapEntryStore myMapEntryStore = new MyMapEntryStore();

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        CountDownLatch countDownLatch = new CountDownLatch(1);

        addEntryListener(hazelcastClient, countDownLatch);
        testEntryStoreTtl(hazelcastClient);

        countDownLatch.await();
        hazelcastClient.shutdown();
        hazelcastServer.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);

        mapStoreConfig.setImplementation(myMapEntryStore);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void addEntryListener(HazelcastInstance hazelcastClient, CountDownLatch countDownLatch) {
        IMap<String, MyRecord> map = hazelcastClient.getMap(MAP_NAME);

        // Add an entry listener to IMap
        EntryAdapter<String, MyRecord> entryListener = new EntryAdapter<>() {
            @Override
            public void entryExpired(EntryEvent<String, MyRecord> event) {
                log.info("entryExpired {} {}", event.getOldValue(), event.getValue());
                log.info("BUG : MyMapEntryStore size is still : {} ", myMapEntryStore.size());

                countDownLatch.countDown();
            }
        };
        map.addEntryListener(entryListener, true);
    }

    // Test that map entry expires. Expiration value is passed to EntryStore too
    // But EntryStore#delete is not triggered when entry expires
    private static void testEntryStoreTtl(HazelcastInstance hazelcastClient) {
        IMap<String, MyRecord> map = hazelcastClient.getMap(MAP_NAME);

        // Add first entry to IMap
        MyRecord value = new MyRecord("value1");
        map.put(KEY, value, 5, TimeUnit.SECONDS);
        log.info("map Size {}", map.size());

        // Replace entry and set TTL
        map.replace(KEY, value, new MyRecord("value2"));
        map.setTtl(KEY, 5, TimeUnit.SECONDS);

        log.info("testEntryStore finished");
    }
}
