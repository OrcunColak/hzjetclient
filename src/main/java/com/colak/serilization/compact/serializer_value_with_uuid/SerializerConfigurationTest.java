package com.colak.serilization.compact.serializer_value_with_uuid;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to show that compact serializer works for value object that has a UUI field
 */
class SerializerConfigurationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializerConfigurationTest.class);

    private static final String MAP_NAME = "uuid_map";

    private static final Integer KEY = 1;

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting Test");

        // Start server
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClientInstance = getHazelcastClientInstanceByConfig();

        // Do test
        populateMap(hazelcastClientInstance);

        // Shutdown server
        hazelcastServerInstance.shutdown();

        // Shut down HZ client
        hazelcastClientInstance.shutdown();

        LOGGER.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        clientConfig.getSerializationConfig()
                .getCompactSerializationConfig()
                .addSerializer(new UUIDValueObjectSerializer());

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void populateMap(HazelcastInstance hazelcastClientInstance) {
        IMap<Integer, UUIDValueObject> myMap = hazelcastClientInstance.getMap(MAP_NAME);
        myMap.put(KEY, UUIDValueObject.createNew());
    }

}
