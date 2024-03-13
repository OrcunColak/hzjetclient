package com.colak.datastructures.mapentrprocessor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Example for custom MapEntryStore
 */
class MapEntryProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapEntryProcessorTest.class);

    private static final String MAP_NAME = "mymap";

    public static void main(String[] args) {
        LOGGER.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        testEntryProcessor(hazelcastClient);

        hazelcastServer.shutdown();
        hazelcastClient.shutdown();
        LOGGER.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void testEntryProcessor(HazelcastInstance hazelcastClient) {
        IMap<Integer, Integer> map = hazelcastClient.getMap(MAP_NAME);
        for (int index = 0; index < 100; index++) {
            map.put(index, index);
        }
        Map<Integer, Integer> resultMap = map.executeOnEntries(new IncrementingEntryProcessor());
        LOGGER.info("resultMap : {}", resultMap);
    }

    static class IncrementingEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {
        @Override
        public Integer process(Map.Entry<Integer, Integer> entry) {
            Integer value = entry.getValue();
            entry.setValue(value + 1);
            return value + 1;
        }
    }
}
