package com.colak.jet.mapstateful;


import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Add each item to accumulator and add the result to list
 */
@Slf4j
public class MapStatefulExample {

    private static final String LIST_NAME = "my-list";

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        statefulTest(hazelcastServer);

        printOutputList(hazelcastServer);

        hazelcastServer.shutdown();
        log.info("Test completed");

    }

    private static void printOutputList(HazelcastInstance hazelcastServer) {
        IList<Long> list = hazelcastServer.getList(LIST_NAME);
        // 1, 3, 6
        for (Long value : list) {
            log.info("Item : {}", value);
        }
    }

    private static void statefulTest(HazelcastInstance hazelcastServer) {
        Pipeline pipeline = Pipeline.create();

        List<Integer> input = List.of(1, 2, 3);
        pipeline.readFrom(TestSources.items(input))
                .mapStateful(LongAccumulator::new, (accumulator, value) -> {
                    accumulator.add(value);
                    return accumulator.get();
                })
                .writeTo(Sinks.list(LIST_NAME));

        Job job = hazelcastServer.getJet().newJob(pipeline);
        job.join();
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        // Add JetConfig
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);

        return Hazelcast.newHazelcastInstance(config);
    }

}
