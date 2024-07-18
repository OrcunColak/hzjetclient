package com.colak.jet.sinks.jdbc;

import com.colak.jet.jdbc_dataconnection.CreatePostgresDataConnection;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class SinkJdbcIncorrectSqlTest {

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        CreatePostgresDataConnection.createDataConnection(hazelcastServer);

        testSink(hazelcastClient);

        hazelcastClient.shutdown();
        hazelcastServer.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        // Add JetConfig
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void testSink(HazelcastInstance hazelcastClient) throws InterruptedException {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(10, 11))
                .writeTo(Sinks.jdbc("INSERT INTO myworker (id,name,ssn) VALUES (?, ?, ?, ?)",
                        DataConnectionRef.dataConnectionRef(CreatePostgresDataConnection.CONNECTION_NAME),
                        (stmt, item) -> {
                            stmt.setInt(1, item);
                            stmt.setString(2, String.valueOf(item));
                            stmt.setString(3, String.valueOf(item));
                        }
                ));

        JetService jetService = hazelcastClient.getJet();

        Job job = null;
        try {
            job = jetService.newJob(pipeline);
            TimeUnit.SECONDS.sleep(10);

            JobStatus status = job.getStatus();
            log.info("Job Status 1: {}", status);

            job.cancel();
            log.info("Cancelling job");

            job.join();

        } catch (RuntimeException exception) {
            // Getting .CancellationByUserException
            log.error("Exception : ", exception);
        }

        assert job != null;
        JobStatus status = job.getStatus();
        // Job Status after cancellation: FAILED
        log.info("Job Status after cancellation: {}", status);
    }
}
