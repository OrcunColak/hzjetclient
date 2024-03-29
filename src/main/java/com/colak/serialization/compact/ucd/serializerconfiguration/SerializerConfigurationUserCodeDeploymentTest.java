package com.colak.serialization.compact.ucd.serializerconfiguration;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Test to show that UCD and compact serializer  configuration works
 */
@Slf4j
class SerializerConfigurationUserCodeDeploymentTest {

    private static final String MAP_NAME = "worker_map";

    private static final Integer KEY = 1;

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        // Start client
        HazelcastInstance hazelcastClientInstance = getHazelcastClientInstanceByConfig();

        // Do test
        populateMap(hazelcastClientInstance);
        updateWorker(hazelcastClientInstance);
        printWorker(hazelcastClientInstance);

        // Shut down HZ client and server
        hazelcastClientInstance.shutdown();

        log.info("Test completed");
    }

//    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
//        Config config = new Config();
//        UserCodeDeploymentConfig userCodeDeploymentConfig = config.getUserCodeDeploymentConfig();
//        userCodeDeploymentConfig.setEnabled(true);
//
//        return Hazelcast.newHazelcastInstance(config);
//    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        // UCD
        ClientUserCodeDeploymentConfig userCodeDeploymentConfig = clientConfig.getUserCodeDeploymentConfig();
        userCodeDeploymentConfig.setEnabled(true);
        userCodeDeploymentConfig.addClass(MyWorker.class);
        userCodeDeploymentConfig.addClass(MyWorkerEntryProcessor.class);

        clientConfig.getSerializationConfig()
                .getCompactSerializationConfig()
                .addSerializer(new MyWorkerSerializer());

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
