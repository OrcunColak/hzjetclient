package com.colak.datastructures.nearcache.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.nearcache.NearCacheStats;
import lombok.extern.slf4j.Slf4j;

/**
 * See <a href="https://github.com/hazelcast/hazelcast/issues/24982">...</a>
 */
@Slf4j
class NearCacheContainsKeyTest {

    private static final String MAP_NAME = "mostlyReadMap";

    private static final int MAP_SIZE = 1;

    public static void main(String[] args) {
        log.info("Starting HZ Server");

        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        log.info("Starting HZ Client");
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        populateMap(hazelcastServer);

        IMap<Integer, Integer> map = hazelcastClient.getMap(MAP_NAME);
        // This will populate the near cache
        getFromNearCache(map);

        // This will get results from near cache. We can see the hit count increasing for NearCacheStats
        getFromNearCache(map);

        // This will get results from near cache. We can see the hit count increasing for NearCacheStats
        nearCacheContainsKey(map);

        hazelcastClient.shutdown();
        hazelcastServer.shutdown();
        log.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        NearCacheConfig nearCacheConfig = new NearCacheConfig(MAP_NAME);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }


    public static void populateMap(HazelcastInstance hazelcastServer) {
        IMap<Integer, Integer> map = hazelcastServer.getMap(MAP_NAME);
        for (int index = 0; index < MAP_SIZE; index++) {
            map.put(index, index);
        }
    }

    private static void getFromNearCache(IMap<Integer, Integer> map) {
        for (int index = 0; index < MAP_SIZE; index++) {
            log.info("getFromNearCache index : {} , value : {}", index, map.get(index));
        }
        LocalMapStats localMapStats = map.getLocalMapStats();
        log.info("getFromNearCache LocalMapStats {}", localMapStats);

        NearCacheStats nearCacheStats = localMapStats.getNearCacheStats();
        log.info("getFromNearCache NearCacheStats {}", nearCacheStats);
    }

    private static void nearCacheContainsKey(IMap<Integer, Integer> map) {
        for (int index = 0; index < MAP_SIZE; index++) {
            log.info("nearCacheContainsKey index : {} , value : {}", index, map.containsKey(index));
        }
        LocalMapStats localMapStats = map.getLocalMapStats();
        log.info("nearCacheContainsKey LocalMapStats {}", localMapStats);

        NearCacheStats nearCacheStats = localMapStats.getNearCacheStats();
        log.info("nearCacheContainsKey NearCacheStats {}", nearCacheStats);
    }

}
