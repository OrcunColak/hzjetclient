package com.colak.datastructures.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

/**
 * Test to show that collection mutation of MultiMap is undefined
 */
class MultiMapLockTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiMapLockTest.class);

    private static final String MULTI_MAP_NAME = "mymultimap";

    private static final int KEY = 1;

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClientInstance = getHazelcastClientInstanceByConfig();

        // Do test
        testMultiThreadedPut(hazelcastClientInstance);
        printMultiMapSize(hazelcastClientInstance);

        testMultiThreadedPutAllAsync(hazelcastClientInstance);
        printMultiMapSize(hazelcastClientInstance);

        // Shut down HZ client and server
        hazelcastClientInstance.shutdown();
        hazelcastServerInstance.shutdown();

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

    private static void testMultiThreadedPut(HazelcastInstance hazelcastClientInstance) throws InterruptedException {
        MultiMap<Integer, Integer> multiMap = hazelcastClientInstance.getMultiMap(MULTI_MAP_NAME);

        final int numThreads = 1000;
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int index = 0; index < numThreads; index++) {
            final int value = index;
            new Thread(() -> {
                multiMap.put(KEY, value);
                latch.countDown();
            }).start();
        }
        latch.await();
    }

    private static void testMultiThreadedPutAllAsync(HazelcastInstance hazelcastClientInstance) throws InterruptedException {
        MultiMap<Integer, Integer> multiMap = hazelcastClientInstance.getMultiMap(MULTI_MAP_NAME);
        multiMap.clear();
        multiMap.put(KEY, 0);

        final int numThreads = 1000;
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int index = 0; index < numThreads; index++) {
            new Thread(() -> {
                TreeSet<Integer> integerSet = new TreeSet<>(multiMap.get(KEY));
                Integer first = integerSet.first();
                LOGGER.info("first is {}", first);
                Set<Integer> collection = Set.of(first + 1);
                try {
                    multiMap.putAllAsync(KEY, collection).toCompletableFuture().get();
                } catch (Exception ignored) {
                }
                latch.countDown();
            }).start();
        }
        latch.await();
    }

    private static void printMultiMapSize(HazelcastInstance hazelcastClientInstance) {
        MultiMap<Integer, Integer> multiMap = hazelcastClientInstance.getMultiMap(MULTI_MAP_NAME);

        Collection<Integer> integers = multiMap.get(KEY);
        LOGGER.info("Multimap size : {}", integers.size());
        LOGGER.info("Multimap value : {}", integers);
    }
}
