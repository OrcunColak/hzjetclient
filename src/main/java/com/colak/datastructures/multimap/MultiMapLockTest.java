package com.colak.datastructures.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

/**
 * Test to show that collection mutation of MultiMap is undefined
 */
@Slf4j
class MultiMapLockTest {

    private static final String MULTI_MAP_NAME = "mymultimap";

    private static final int KEY = 1;

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        testMultiThreadedPut(hazelcastClient);
        printMultiMapSize(hazelcastClient);

        testMultiThreadedPutAllAsync(hazelcastClient);
        printMultiMapSize(hazelcastClient);

        // Shut down HZ client and server
        hazelcastClient.shutdown();
        hazelcastServer.shutdown();

        log.info("Test completed");
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
                log.info("first is {}", first);
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
        log.info("Multimap size : {}", integers.size());
        log.info("Multimap value : {}", integers);
    }
}
