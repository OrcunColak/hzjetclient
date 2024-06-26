package com.colak.datastructures.ringbuffer;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumer offloads processing to a thread pool
 */
@Slf4j
class RingBufferOffloadTest {

    private static final String RING_BUFFER_NAME = "myringbuffer";
    private static final int RING_BUFFER_SIZE = 5000;

    private static final int NUMBER_OF_PRODUCERS = 3;
    private static final ExecutorService producers = Executors.newFixedThreadPool(NUMBER_OF_PRODUCERS);

    private static final ExecutorService consumers = Executors.newFixedThreadPool(1);
    private static final ExecutorService sequenceCheck = Executors.newFixedThreadPool(1);

    private static final BlockingDeque<Integer> blockingQueue = new LinkedBlockingDeque<>();

    private static final AtomicBoolean runThreadsFlag = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        startProducer(hazelcastClient);

        // Do test
        startConsumer(hazelcastClient);

        startSequenceCheck();

        // Keep the main thread alive so that the ExecutorService can continue running.
        log.info("Press a key to exit");
        int ignored = System.in.read();

        runThreadsFlag.set(false);

        // Shut down the ExecutorService and all of its running tasks.
        shutdownExecutorService(producers);
        shutdownExecutorService(consumers);
        shutdownExecutorService(sequenceCheck);

        // Shut down HZ client and server
        hazelcastClient.shutdown();
        hazelcastServer.shutdown();

        log.info("Test completed");
    }

    private static void shutdownExecutorService(ExecutorService executorService) throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        RingbufferConfig rbConfig = new RingbufferConfig(RING_BUFFER_NAME)
                .setCapacity(RING_BUFFER_SIZE);
        config.addRingBufferConfig(rbConfig);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void startProducer(HazelcastInstance hazelcastInstanceClient) {
        Ringbuffer<Integer> ringbuffer = hazelcastInstanceClient.getRingbuffer(RING_BUFFER_NAME);

        AtomicInteger counter = new AtomicInteger();
        Runnable infiniteJob = () -> {
            while (runThreadsFlag.get()) {
                ringbuffer.add(counter.getAndIncrement());
            }
        };
        for (int index = 0; index < NUMBER_OF_PRODUCERS; index++) {
            producers.submit(infiniteJob);
        }
    }

    private static void startConsumer(HazelcastInstance hazelcastInstanceClient) {
        Runnable infiniteJob = () -> {
            Ringbuffer<Integer> ringbuffer = hazelcastInstanceClient.getRingbuffer(RING_BUFFER_NAME);

            long headSequence = ringbuffer.headSequence();

            while ((runThreadsFlag.get())) {
                try {
                    Integer value = ringbuffer.readOne(headSequence);
                    blockingQueue.put(value);
                    headSequence++;

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };
        consumers.submit(infiniteJob);
    }

    private static void startSequenceCheck() {
        Runnable infiniteJob = () -> {


            while ((runThreadsFlag.get())) {
                try {
                    blockingQueue.takeFirst();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };
        sequenceCheck.submit(infiniteJob);
    }
}
