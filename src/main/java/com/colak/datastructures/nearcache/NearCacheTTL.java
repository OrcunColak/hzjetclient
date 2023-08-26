package com.colak.datastructures.nearcache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Timer;
import java.util.TimerTask;

@UtilityClass
@Slf4j
public class NearCacheTTL {

    public static void testTTL(HazelcastInstance hazelcastInstance) {
        IMap<String, Integer> nearCache = hazelcastInstance.getMap("mostlyReadMap");

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
