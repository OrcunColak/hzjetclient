package com.colak.jet.kafkaconnect.coucbase;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
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
class CouchbaseJobTest {


    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        testJetJobException(hazelcastClient);

        hazelcastClient.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static class CountingProjection implements FunctionEx<SourceRecord, String> {
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
        jobConfig.addJarsInZip(getConnectorURL());
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
        Properties properties = new Properties();
        properties.setProperty("name", "couchbase");
        properties.setProperty("connections.max.idle.ms", "2000"); // 2 seconds
        properties.setProperty("connector.class", "com.couchbase.connect.kafka.CouchbaseSourceConnector");
        properties.setProperty("couchbase.bucket", "travel-sample");
        properties.setProperty("couchbase.seed.nodes", "couchbase://couchbase");
        properties.setProperty("couchbase.password", "password");
        properties.setProperty("couchbase.username", "admin");
        properties.setProperty("couchbase.collections", "_default._default");
        properties.setProperty("couchbase.source.handler",
                "com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler");
        return properties;
    }

    private static URL getConnectorURL() {
        ClassLoader classLoader = CouchbaseJobTest.class.getClassLoader();
        final String CONNECTOR_FILE_PATH = "couchbase-kafka-connect-couchbase-4.1.11.zip";

        return classLoader.getResource(CONNECTOR_FILE_PATH);
    }
}
