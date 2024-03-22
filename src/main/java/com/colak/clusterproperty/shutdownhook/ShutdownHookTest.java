package com.colak.clusterproperty.shutdownhook;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * See
 * <a href="https://stackoverflow.com/questions/69255913/hazelcast-not-shutting-down-gracefully-in-spring-boot?answertab=modifieddesc#tab-top">...</a>
 * and
 * <a href="https://hazelcast.com/blog/rolling-upgrade-hazelcast-imdg-on-kubernetes/">...</a>
 */
class ShutdownHookTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownHookTest.class);

    public static void main(String[] args) throws Exception {

        LOGGER.info("Starting HZ Server");

        // Start client
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();

        TimeUnit.SECONDS.sleep(10);

        LOGGER.info("Test completed");

        System.exit(0);
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        config.setProperty(ClusterProperty.LOGGING_TYPE.getName(), "slf4j");
        config.setProperty(ClusterProperty.SHUTDOWNHOOK_ENABLED.getName(), "true");
        config.setProperty(ClusterProperty.SHUTDOWNHOOK_POLICY.getName(), "GRACEFUL");

        return Hazelcast.newHazelcastInstance(config);
    }

}
