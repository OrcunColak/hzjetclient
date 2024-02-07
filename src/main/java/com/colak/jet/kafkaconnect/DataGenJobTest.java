package com.colak.jet.kafkaconnect;


import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * When Jet Job throws exception, we receive java.util.concurrent.CompletionException for job.join() call
 */
@Slf4j
class DataGenJobTest {

    private static final int ITEM_COUNT = 10;

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        // Do test
        testJetJobException(hazelcastInstanceServer);

        hazelcastInstanceServer.shutdown();

        log.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);
        return Hazelcast.newHazelcastInstance(config);
    }

    static class CountingProjection implements FunctionEx<SourceRecord, String> {
        private int counter = 0;

        @Override
        public String applyEx(SourceRecord sourceRecord) {
            return String.valueOf(counter++);
        }
    }

    private static void testJetJobException(HazelcastInstance hazelcastInstance) throws MalformedURLException {
        Properties connectorProperties = getConnectorProperties();


        Pipeline pipeline = Pipeline.create();
        StreamSource<String> source = KafkaConnectSources.connect(connectorProperties, new CountingProjection());
        StreamStage<String> streamStage = pipeline.readFrom(source)
                .withoutTimestamps()
                .setLocalParallelism(1);

        String listName = "test_kafka_connect_list";
        streamStage.writeTo(Sinks.list(listName));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getDataGenConnectorURL());
        jobConfig.addClass(CountingProjection.class);

        JetService jetService = hazelcastInstance.getJet();
        log.info("Starting job");
        Job job = jetService.newJob(pipeline, jobConfig);
        try {
            job.join();
        } catch (Exception exception) {
            log.error("Exception", exception);
        }
        IList<Object> list = hazelcastInstance.getList(listName);
        log.info("List size : {}", list.size());
    }

    private static Properties getConnectorProperties() {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "datagen-connector");
        connectorProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        connectorProperties.setProperty("max.interval", "1");
        connectorProperties.setProperty("iterations", String.valueOf(ITEM_COUNT));
        connectorProperties.setProperty("kafka.topic", "orders");
        connectorProperties.setProperty("quickstart", "orders");
        connectorProperties.setProperty("tasks.max", "1");
        return connectorProperties;
    }

    private static URL getDataGenConnectorURL() throws MalformedURLException {
        return new URL("https://repository.hazelcast.com/download/tests/confluentinc-kafka-connect-datagen-0.6.0.zip");
    }
}
