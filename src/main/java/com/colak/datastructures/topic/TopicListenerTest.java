package com.colak.datastructures.topic;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

// Client receives message published by member
@Slf4j
class TopicListenerTest {

    private static final String TOPIC_NAME = "my-topic";

    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        log.info("Starting Test");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        addListener(hazelcastClient);

        startProducer(hazelcastServer);

        countDownLatch.await();

        // Shutdown client
        hazelcastClient.shutdown();

        // Shutdown server
        hazelcastServer.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        config.getTopicConfig(TOPIC_NAME);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void addListener(HazelcastInstance hazelcastClientInstance) {
        ITopic<Integer> topic = hazelcastClientInstance.getTopic(TOPIC_NAME);
        topic.addMessageListener(message -> {
            log.info("Received message : {}", message.getMessageObject());
            countDownLatch.countDown();
        });
    }

    private static void startProducer(HazelcastInstance hazelcastServerInstance) {
        ITopic<Integer> topic = hazelcastServerInstance.getTopic(TOPIC_NAME);
        topic.publish(1);

    }

}
