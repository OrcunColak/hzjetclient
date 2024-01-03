package com.colak.datastructures.map.async;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that we can use async methods of IMap
 */
@UtilityClass
@Slf4j
class MapAsyncMethodsTest {
    private final Logger LOGGER = LoggerFactory.getLogger(MapAsyncMethodsTest.class);
    private final String MAP_NAME = "my-map";
    private final int MAP_SIZE = 100;

    public static void main(String[] args) {
        LOGGER.info("Starting MapAsyncMethodsTest");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        testAsyncAdd(hazelcastServer);

        // Shutdown server
        hazelcastServer.shutdown();
        LOGGER.info("Test completed");
    }

    private HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }


    private void testAsyncAdd(HazelcastInstance hazelcastInstance) {
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

    private void printMapSize(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Integer> map = hazelcastInstance.getMap(MAP_NAME);
        log.info("Map size is {}", map.size());
    }
}
