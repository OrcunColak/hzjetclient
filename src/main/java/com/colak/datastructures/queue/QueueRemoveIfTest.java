package com.colak.datastructures.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// See https://stackoverflow.com/questions/78408548/hazelcast-iqueue-objects-stucks-on-more-requests
// Producer is populating IQueue
// Consumer is removing from IQueue at a slower rate
// IQueue does not support Iterator.remove(), therefore removeIf() method does not work
@Slf4j
class QueueRemoveIfTest {

    private static final String QUEUE_NAME = "my-queue";

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        startProducer(hazelcastServer);
        startConsumer(hazelcastClient);

        TimeUnit.SECONDS.sleep(10);

        scheduledExecutorService.shutdown();

        printQueue(hazelcastServer);

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

    private static void startProducer(HazelcastInstance hazelcastServerInstance) {
        IQueue<Integer> queue = hazelcastServerInstance.getQueue(QUEUE_NAME);
        AtomicInteger atomicInteger = new AtomicInteger();
        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    int andIncrement = atomicInteger.getAndIncrement();
                    queue.add(andIncrement);
                    log.info("Added : {}", andIncrement);
                },
                0,
                1, TimeUnit.SECONDS);
    }

    private static void startConsumer(HazelcastInstance hazelcastInstanceClient) {
        IQueue<Integer> queue = hazelcastInstanceClient.getQueue(QUEUE_NAME);
        AtomicInteger atomicInteger = new AtomicInteger();
        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    int andIncrement = atomicInteger.getAndIncrement();
                    boolean removed = queue.remove(andIncrement);
                    log.info("Head is : {} Removed is : {}", andIncrement, removed);
                },
                0,
                2, TimeUnit.SECONDS);
    }

    private static void printQueue(HazelcastInstance hazelcastServerInstance) {
        IQueue<Integer> queue = hazelcastServerInstance.getQueue(QUEUE_NAME);
        log.info("Queue size : {}", queue.size());
        for (Integer integer : queue) {
            log.info("Next : {}", integer);
        }
    }

}
