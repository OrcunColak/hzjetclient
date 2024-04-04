package com.colak.serialization.portable;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;


/**
 * Test to show portable serialization
 */
@Slf4j
class PortableSerializationTest {

    private static final String MAP_NAME = "my-map";

    private static final Integer KEY = 1;

    public static void main(String[] args) throws Exception {
        log.info("Starting Test");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        populateMap(hazelcastClient);
        printMap(hazelcastServer);

        // Shutdown server
        hazelcastServer.shutdown();

        // Shut down HZ client
        hazelcastClient.shutdown();

        log.info("Test completed");
    }

    private static void printMap(HazelcastInstance hazelcastServer) {
        IMap<Integer, Foo> myMap = hazelcastServer.getMap(MAP_NAME);
        Foo foo = myMap.get(KEY);
        log.info("Foo is : {}", foo);
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.addPortableFactory(1, new MyPortableFactory());
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();

        SerializationConfig serializationConfig = clientConfig.getSerializationConfig();
        serializationConfig.addPortableFactory(1, new MyPortableFactory());

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void populateMap(HazelcastInstance hazelcastClient) {
        IMap<Integer, Foo> myMap = hazelcastClient.getMap(MAP_NAME);
        Foo foo = new Foo();
        foo.setFoo("foo1");
        myMap.put(KEY, foo);
    }
}
