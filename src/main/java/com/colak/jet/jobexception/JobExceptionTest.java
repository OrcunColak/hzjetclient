package com.colak.jet.jobexception;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;

/**
 * When Jet Job throws exception, we receive java.util.concurrent.CompletionException
 */
class JobExceptionTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobExceptionTest.class);

    public static void main(String[] args) {
        LOGGER.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        LOGGER.info("Starting HZ Client");
        // Start client
        HazelcastInstance hazelcastInstanceClient = getHazelcastClientInstanceByConfig();

        // Do test
        testJetJobException(hazelcastInstanceClient);

        hazelcastInstanceClient.shutdown();
        hazelcastInstanceServer.shutdown();

        LOGGER.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);
        return Hazelcast.newHazelcastInstance(config);
    }

    public static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }


    private static void testJetJobException(HazelcastInstance hazelcastInstanceClient) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(IntStream.range(0, 10).boxed().toArray(Integer[]::new)))
                .map(item -> entry(item, item.toString()))
                .writeTo(Sinks.jdbc("INSERT INTO foo VALUES(?)",
                        () -> {
                            throw new SQLException("Error creating DataSource");
                        },
                        (stmt, item) -> {
                            // execution doesn't get here
                        }
                ));

        JetService jetService = hazelcastInstanceClient.getJet();
        Job job = jetService.newJob(p);
        try {
            job.join();
        } catch (Exception exception) {
            LOGGER.error("Exception caught: ", exception);
        }
        LOGGER.info("testEntryStore finished");
    }
}
