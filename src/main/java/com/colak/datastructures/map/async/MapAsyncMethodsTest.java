package com.colak.datastructures.map.async;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Test that we can use async methods of IMap
 */
@Slf4j
class MapAsyncMethodsTest {
    private static final String MAP_NAME = "my-map";
    private static final int MAP_SIZE = 100;

    public static void main(String[] args) {
        log.info("Starting MapAsyncMethodsTest");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        testAsyncAdd(hazelcastServer);

        // Shutdown server
        hazelcastServer.shutdown();
        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }


    private static void testAsyncAdd(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Integer> map = hazelcastInstance.getMap(MAP_NAME);

        for (int index = 0; index < MAP_SIZE; index++) {
            map.putAsync(index, index)
                    .whenComplete((ignored, throwable) -> {
                        if (throwable != null) {
                            log.error("Exception occurred", throwable);
                        }
                    });
        }
        printMapSize(hazelcastInstance);
    }

    private static void printMapSize(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Integer> map = hazelcastInstance.getMap(MAP_NAME);
        log.info("Map size is {}", map.size());
    }
}
