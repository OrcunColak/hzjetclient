package com.colak.clusterproperty.jvmproperty;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Pass system properties using JVM’s System class
 * See <a href="https://github.com/hazelcast/hazelcast/issues/26310">...</a>
 */
@Slf4j
class JVMPropertyTest {

    public static void main(String[] args) throws Exception {
        Properties properties = java.lang.System.getProperties();
        properties.put(ClusterProperty.SHUTDOWNHOOK_ENABLED.getName(), "true");
        properties.put(ClusterProperty.SHUTDOWNHOOK_POLICY.getName(), "GRACEFUL");

        log.info("Starting HZ Server");

        // Start client
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();

        TimeUnit.SECONDS.sleep(10);

        log.info("Test completed");

        hazelcastServerInstance.shutdown();
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

}
