package com.colak.datastructures.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapEvent;
import com.hazelcast.multimap.MultiMap;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

/**
 * Test to show that EvenListener can listen to MultiMap
 */
@Slf4j
class MultiMapEventListenerTest {

    private static final String MULTI_MAP_NAME = "mymultimap";

    private static final CountDownLatch entryUpdatedLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        testPut(hazelcastClient);

        entryUpdatedLatch.await();

        // Shut down HZ client and server
        hazelcastClient.shutdown();
        hazelcastServer.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void testPut(HazelcastInstance hazelcastClientInstance) throws InterruptedException {
        MultiMap<Integer, Integer> multiMap = hazelcastClientInstance.getMultiMap(MULTI_MAP_NAME);
        multiMap.addEntryListener(new EntryListener<>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                log.info("entryAdded key : {}, value : {}", event.getKey(), event.getValue());
                if (event.getValue() == 2) {
                    entryUpdatedLatch.countDown();
                }
            }

            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                log.info("entryEvicted key : {}, value : {}", event.getKey(), event.getValue());
            }

            @Override
            public void entryExpired(EntryEvent<Integer, Integer> event) {
                log.info("entryExpired key : {}, value : {}", event.getKey(), event.getValue());
            }

            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                log.info("entryRemoved key : {}, value : {}", event.getKey(), event.getValue());
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                log.info("entryUpdated key : {}, value : {}", event.getKey(), event.getValue());

            }

            @Override
            public void mapCleared(MapEvent event) {
                log.info("mapCleared");
            }

            @Override
            public void mapEvicted(MapEvent event) {
                log.info("mapEvicted");
            }
        }, true);

        int key = 1;
        multiMap.put(key, 1);
        multiMap.put(key, 2);


    }

}
