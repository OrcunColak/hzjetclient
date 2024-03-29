package com.colak.serialization.compact.serializer_value_with_uuid;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Test to show that compact serializer works for value object that has a UUI field
 */
@Slf4j
class SerializerConfigurationTest {

    private static final String MAP_NAME = "uuid_map";

    private static final Integer KEY = 1;

    public static void main(String[] args) throws Exception {
        log.info("Starting Test");

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

        log.info("Test completed");
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

    private static void populateMap(HazelcastInstance hazelcastClient) {
        IMap<Integer, UUIDValueObject> myMap = hazelcastClient.getMap(MAP_NAME);
        myMap.put(KEY, UUIDValueObject.createNew());
    }

}
