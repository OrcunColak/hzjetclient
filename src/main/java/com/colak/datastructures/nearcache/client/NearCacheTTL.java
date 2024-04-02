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

import java.util.Timer;
import java.util.TimerTask;

@Slf4j
class NearCacheTTL {

    private static final String MOSTLY_READ_MAP = "mostlyReadMap";

    private static final int MAP_SIZE = 1;

    public static void main(String[] args) {
        log.info("Starting HZ Server");
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        populateMap(hazelcastServer);

        log.info("Starting HZ Client");
        HazelcastInstance hazelcastClient = getHazelcastInstanceByConfig();
        testTTL(hazelcastClient);

        log.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    public static HazelcastInstance getHazelcastInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        NearCacheConfig nearCacheConfig = new NearCacheConfig(MOSTLY_READ_MAP);
        nearCacheConfig.setTimeToLiveSeconds(10);
        nearCacheConfig.setMaxIdleSeconds(10);

        clientConfig.addNearCacheConfig(nearCacheConfig);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public static void populateMap(HazelcastInstance hazelcastServer) {
        IMap<Integer, Integer> map = hazelcastServer.getMap(MOSTLY_READ_MAP);
        for (int index = 0; index < MAP_SIZE; index++) {
            map.put(index, index);
        }
    }

    // Test NearCache entry expires
    public static void testTTL(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Integer> map = hazelcastInstance.getMap(MOSTLY_READ_MAP);

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                map.get(0);

                // As the near cache expires we should see misses field of NearCacheStats increase gradually
                LocalMapStats localMapStats = map.getLocalMapStats();
                log.info("getFromNearCache LocalMapStats {}", localMapStats);

                NearCacheStats nearCacheStats = localMapStats.getNearCacheStats();
                log.info("getFromNearCache NearCacheStats {}", nearCacheStats);
            }
        };
        // Schedule the timer task to run after 1 second
        timer.scheduleAtFixedRate(task, 0, 1000);
    }
}
