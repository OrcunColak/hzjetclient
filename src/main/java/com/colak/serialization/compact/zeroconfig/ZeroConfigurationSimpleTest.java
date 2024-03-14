package com.colak.serialization.compact.zeroconfig;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to show that zero configuration works
 */
class ZeroConfigurationSimpleTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigurationSimpleTest.class);

    private static final String MAP_NAME = "uuid_map";

    public static void main(String[] args) {
        LOGGER.info("Starting Test");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        populateMap(hazelcastClient);

        // Shutdown server
        hazelcastServer.shutdown();

        // Shut down HZ client
        hazelcastClient.shutdown();

        LOGGER.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void populateMap(HazelcastInstance hazelcastClient) {
        IMap<Long, Employee> myMap = hazelcastClient.getMap(MAP_NAME);
        long id = 1L;
        Employee employee = new Employee(id, "John Doe");
        myMap.set(id, employee);
        Employee employeeFromMap = myMap.get(id);
        LOGGER.info("employeeFromMap : {}", employeeFromMap);
    }

    @Getter
    @AllArgsConstructor
    @ToString
    private static class Employee {
        private long id;
        private String name;
    }

}
