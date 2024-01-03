package com.colak.datastructures.reliabletopic;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Early joiner gets all published messages
 * Late joiner gets the latest message
 */
class ReliableTopicLateJoinerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReliableTopicLateJoinerTest.class);

    private static final String TOPIC_NAME = "myringbuffer";

    private static final CountDownLatch earlyJoinerCountDownLatch = new CountDownLatch(RingbufferConfig.DEFAULT_CAPACITY + 1);
    private static final CountDownLatch lateJoinerCountDownLatch = new CountDownLatch(1);


    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();

        startEarlyJoiningListener(hazelcastServerInstance);

        startProducer(hazelcastServerInstance);

        // Do test
        startLateJoiningListener(hazelcastServerInstance);

        earlyJoinerCountDownLatch.await();
        lateJoinerCountDownLatch.await();

        // Shutdown server
        hazelcastServerInstance.shutdown();

        LOGGER.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void startProducer(HazelcastInstance hazelcastServerInstance) {
        ITopic<Integer> reliableTopic = hazelcastServerInstance.getReliableTopic(TOPIC_NAME);
        // Overflow the ring buffer
        for (int index = 0; index <= RingbufferConfig.DEFAULT_CAPACITY + 1; index++) {
            reliableTopic.publish(index);
        }
    }

    private static void startEarlyJoiningListener(HazelcastInstance hazelcastServerInstance) {
        ITopic<Integer> reliableTopic = hazelcastServerInstance.getReliableTopic(TOPIC_NAME);
        reliableTopic.addMessageListener(new ReliableMessageListenerAdapter<>(message -> {
            Integer integer = message.getMessageObject();
            LOGGER.info("Early joiner received : {}", integer);
            earlyJoinerCountDownLatch.countDown();
        }) {
            @Override
            public long retrieveInitialSequence() {
                Ringbuffer<Object> ringbuffer = hazelcastServerInstance.getRingbuffer(RingbufferService.TOPIC_RB_PREFIX + TOPIC_NAME);
                // if tailSequence is -1 get next message
                // if tailSequence is different from -1 get the oldest message
                return ringbuffer.tailSequence();
            }
        });
    }

    private static void startLateJoiningListener(HazelcastInstance hazelcastServerInstance) {
        ITopic<Integer> reliableTopic = hazelcastServerInstance.getReliableTopic(TOPIC_NAME);
        reliableTopic.addMessageListener(new ReliableMessageListenerAdapter<>(message -> {
            Integer integer = message.getMessageObject();
            LOGGER.info("Late joiner received : {}", integer);
            lateJoinerCountDownLatch.countDown();
        }) {

            @Override
            public long retrieveInitialSequence() {
                Ringbuffer<Object> ringbuffer = hazelcastServerInstance.getRingbuffer(RingbufferService.TOPIC_RB_PREFIX + TOPIC_NAME);
                // if tailSequence is -1 get next message
                // if tailSequence is different from -1 get the oldest message
                return ringbuffer.tailSequence();
            }
        });
    }
}
