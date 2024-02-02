package com.colak.jet.submitjob;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.SubmitJobParameters;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class SubmitFaultyJobTest {

    public static void main(String[] args) {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        log.info("Starting HZ Client");
        // Start client
        HazelcastInstance hazelcastInstanceClient = getHazelcastClientInstanceByConfig();

        testFaultJarSubmission(hazelcastInstanceClient);

        hazelcastInstanceClient.shutdown();
        hazelcastInstanceServer.shutdown();

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

    public static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void testFaultJarSubmission(HazelcastInstance hazelcastInstanceClient) {

        try {
            JetClientInstanceImpl jetService = (JetClientInstanceImpl) hazelcastInstanceClient.getJet();

            Path jarPath = getPath();
            SubmitJobParameters submitJobParameters = SubmitJobParameters.withJarOnClient()
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
