package com.colak.jet.submitjob;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.SubmitJobParameters;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SubmitFaultyJobTest {

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        log.info("Starting HZ Client");
        // Start client
        HazelcastInstance hazelcastInstanceClient = getHazelcastClientInstanceByConfig();
        printJobs(hazelcastInstanceClient);

        testFaultJarSubmission(hazelcastInstanceClient);

        for (int index = 0; index < 100; index++) {
            boolean exitFlag = printJobs(hazelcastInstanceClient);
            if (exitFlag) {
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }


        hazelcastInstanceClient.shutdown();
        hazelcastInstanceServer.shutdown();

        log.info("Test completed");
    }

    private static boolean printJobs(HazelcastInstance hazelcastInstanceClient) {
        JetService jetService = hazelcastInstanceClient.getJet();
        List<Job> jobs = jetService.getJobs();
        String title = """
                                
                JOBS
                -----
                """;
        log.info(title);
        boolean exitFlag = false;
        for (Job job : jobs) {
            log.info("job : {} status : {}", job.getName(), job.getStatus());
            if (job.getStatus() == JobStatus.RUNNING) {
                exitFlag = true;
            }
        }
        return exitFlag;
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        // Add JetConfig
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    public static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void testFaultJarSubmission(HazelcastInstance hazelcastInstanceClient) {

        try {
            JetClientInstanceImpl jetService = (JetClientInstanceImpl) hazelcastInstanceClient.getJet();

            Path jarPath = getPath();
            SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
                    .setJobName("my-faulty-job")
                    .setJarPath(jarPath);


            jetService.submitJobFromJar(submitJobParameters);
        } catch (Exception exception) {
            log.error("Exception caught ", exception);
        }
    }

    private static Path getPath() throws URISyntaxException {
        ClassLoader classLoader = SubmitFaultyJobTest.class.getClassLoader();
        URL resource = classLoader.getResource("joiningjob-1.0.0.jar");

        assert resource != null;
        return Paths.get(resource.toURI());
    }
}
