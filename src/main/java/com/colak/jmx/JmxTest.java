package com.colak.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class JmxTest {

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        config.setProperty("hazelcast.jmx", "true");

        return Hazelcast.newHazelcastInstance(config);
    }

}
