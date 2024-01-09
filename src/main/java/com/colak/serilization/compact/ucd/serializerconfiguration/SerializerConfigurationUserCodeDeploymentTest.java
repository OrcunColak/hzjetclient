package com.colak.serilization.compact.ucd.serializerconfiguration;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Test to show that UCD and compact serializer  configuration works
 */
class SerializerConfigurationUserCodeDeploymentTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializerConfigurationUserCodeDeploymentTest.class);

    private static final String MAP_NAME = "worker_map";

    private static final Integer KEY = 1;

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting HZ Client");

        // Start client
        HazelcastInstance hazelcastClientInstance = getHazelcastClientInstanceByConfig();

        // Do test
        populateMap(hazelcastClientInstance);
        updateWorker(hazelcastClientInstance);
        printWorker(hazelcastClientInstance);

        // Shut down HZ client and server
        hazelcastClientInstance.shutdown();

        LOGGER.info("Test completed");
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
        LOGGER.info("Updated map : {}", updatedMap);
    }

    private static void printWorker(HazelcastInstance hazelcastClientInstance) {
        IMap<Integer, MyWorker> myWorkerMap = hazelcastClientInstance.getMap(MAP_NAME);

        MyWorker worker = myWorkerMap.get(KEY);
        LOGGER.info("New Worker : {}", worker);
    }
}
