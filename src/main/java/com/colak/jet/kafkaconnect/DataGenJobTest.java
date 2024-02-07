package com.colak.jet.kafkaconnect;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * When Jet Job throws exception, we receive java.util.concurrent.CompletionException for job.join() call
 */
class DataGenJobTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenJobTest.class);

    private static final int ITEM_COUNT = 10;

    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting HZ Server");

        // Start server
//        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        LOGGER.info("Starting HZ Client");
        // Start client
        HazelcastInstance hazelcastInstanceClient = getHazelcastClientInstanceByConfig();

        // Do test
        testJetJobException(hazelcastInstanceClient);

        hazelcastInstanceClient.shutdown();
//        hazelcastInstanceServer.shutdown();

        LOGGER.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }


    static class MyProjection implements FunctionEx<SourceRecord, String> {
        private int counter = 0;

        @Override
        public String applyEx(SourceRecord sourceRecord) {
            return String.valueOf(counter++);
        }
    }

    static class MyConsumer implements ConsumerEx<List<String>> {

        private final int itemCount;

        public MyConsumer(int itemCount) {
            this.itemCount = itemCount;
        }

        @Override
        public void acceptEx(List<String> list) {
            if (list.size() == itemCount) {
                throw new AssertionCompletedException();
            }
        }
    }

    private static void testJetJobException(HazelcastInstance hazelcastInstanceClient) throws MalformedURLException {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "datagen-connector");
        randomProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        randomProperties.setProperty("max.interval", "1");
        randomProperties.setProperty("kafka.topic", "orders");
        randomProperties.setProperty("quickstart", "orders");
        randomProperties.setProperty("tasks.max", "1");


        Pipeline pipeline = Pipeline.create();
        StreamSource<String> source = KafkaConnectSources.connect(randomProperties, new MyProjection());
        StreamStage<String> streamStage = pipeline.readFrom(source)
                .withoutTimestamps()
                .setLocalParallelism(1);

        streamStage.writeTo(AssertionSinks.assertCollectedEventually(60,new MyConsumer(ITEM_COUNT)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());
        jobConfig.addClass(MyProjection.class, MyConsumer.class);

        JetService jetService = hazelcastInstanceClient.getJet();
        LOGGER.info("Starting job");
        Job job = jetService.newJob(pipeline, jobConfig);
        try {
            job.join();
        } catch (Exception exception) {
            LOGGER.error("Exception", exception);
        }
    }

    private static URL getDataGenConnectorURL() throws MalformedURLException {
        return new URL("https://repository.hazelcast.com/download/tests/confluentinc-kafka-connect-datagen-0.6.0.zip");
    }
}
