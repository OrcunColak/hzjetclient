package com.colak.datastructures.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.Predicate;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;


// Test to show that QueryCache works
@Slf4j
class MapQueryCacheTest {

    private static final String MAP_NAME = "my-map";

    private static final CountDownLatch entryUpdatedLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        populateMap(hazelcastServer);
        testQueryCache(hazelcastServer);

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

    private static void populateMap(HazelcastInstance hazelcastServer) {
        IMap<Integer, Integer> myMap = hazelcastServer.getMap(MAP_NAME);

        Map<Integer, Integer> map = Map.of(1, 1, 2, 2);
        myMap.putAll(map);
    }

    private static void testQueryCache(HazelcastInstance hazelcastClient) {
        IMap<Integer, Integer> myMap = hazelcastClient.getMap(MAP_NAME);
        QueryCache<Integer, Integer> queryCache = myMap.getQueryCache("queryCache", (Predicate<Integer, Integer>) mapEntry -> isOdd(mapEntry.getKey()), true);
        while (true) {
            Set<Map.Entry<Integer, Integer>> entries = queryCache.entrySet();
            for (Map.Entry<Integer, Integer> entry : entries) {
                log.info("Key : {} Value : {}", entry.getKey(), entry.getValue());
            }

            int size = queryCache.size();
            if (size == 1) {
                entryUpdatedLatch.countDown();
                break;
            }
        }
    }

    private static boolean isOdd(int number) {
        return number % 2 != 0;
    }
}
