package com.colak.serialization.compact.ucd.zeroconfiguration;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Test to show that UCD and zero configuration compact serializer works
 */
@Slf4j
class ZeroConfigurationUserCodeDeploymentTest {

    private static final String MAP_NAME = "worker_map";

    private static final Integer KEY = 1;

    public static void main(String[] args) throws Exception {

        log.info("Starting HZ Server");

        // Start client
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();

        log.info("Starting HZ Client");

        // Start client
        HazelcastInstance hazelcastClientInstance = getHazelcastClientInstanceByConfig();

        // Do test
        populateMap(hazelcastClientInstance);
        updateWorker(hazelcastClientInstance);
        printWorker(hazelcastClientInstance);

        // Shut down HZ client
        hazelcastClientInstance.shutdown();

        // Shut down HZ server
        hazelcastServerInstance.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        config.setProperty(ClusterProperty.LOGGING_TYPE.getName(), "slf4j");
        UserCodeDeploymentConfig userCodeDeploymentConfig = config.getUserCodeDeploymentConfig();
        userCodeDeploymentConfig.setEnabled(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        // UCD
        ClientUserCodeDeploymentConfig userCodeDeploymentConfig = clientConfig.getUserCodeDeploymentConfig();
        userCodeDeploymentConfig.setEnabled(true);
        userCodeDeploymentConfig.addClass(MyWorker.class);
        userCodeDeploymentConfig.addClass(MyWorkerEntryProcessor.class);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void populateMap(HazelcastInstance hazelcastClientInstance) {
        IMap<Integer, MyWorker> myWorkerMap = hazelcastClientInstance.getMap(MAP_NAME);
        myWorkerMap.put(KEY, new MyWorker("worker1"));
    }

    private static void updateWorker(HazelcastInstance hazelcastClientInstance) {
        IMap<Integer, MyWorker> myWorkerMap = hazelcastClientInstance.getMap(MAP_NAME);

        MyWorkerEntryProcessor entryProcessor = new MyWorkerEntryProcessor();
        Map<Integer, MyWorker> updatedMap = myWorkerMap.executeOnEntries(entryProcessor);
        log.info("Updated map : {}", updatedMap);
    }

    private static void printWorker(HazelcastInstance hazelcastClientInstance) {
        IMap<Integer, MyWorker> myWorkerMap = hazelcastClientInstance.getMap(MAP_NAME);

        MyWorker worker = myWorkerMap.get(KEY);
        log.info("New Worker : {}", worker);
    }
}
