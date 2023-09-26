package com.colak.datastructures.nearcache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

@UtilityClass
@Slf4j
class NearCacheTTL {

    private static final Logger LOGGER = LoggerFactory.getLogger(NearCacheTTL.class);

    static final String MOSTLY_READ_MAP = "mostlyReadMap";
    public static void main(String[] args) {
        LOGGER.info("Starting HZ Client");

        HazelcastInstance hazelcastInstanceClient = getHazelcastInstanceByConfig();
        testTTL(hazelcastInstanceClient);
    }

    public static HazelcastInstance getHazelcastInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(MOSTLY_READ_MAP);
        nearCacheConfig.setTimeToLiveSeconds(10);
        nearCacheConfig.setMaxIdleSeconds(10);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    // Test NearCache entry expires
    public static void testTTL(HazelcastInstance hazelcastInstance) {
        IMap<String, Integer> nearCache = hazelcastInstance.getMap(MOSTLY_READ_MAP);

        Timer timer = new Timer();

        String key = "1";
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                int i = nearCache.get(key);
                log.info("Timer expired! " + i);
            }
        };
        // Schedule the timer task to run after 1 second
        timer.scheduleAtFixedRate(task, 0,1000);
    }
}
