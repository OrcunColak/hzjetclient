package com.colak.jet.fromimap;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Example job to read from IMap and write to postgres database
 */
@Slf4j
class IMapToPostgresJob {
    private static final String TABLE_NAME = "myworker_backup";

    private static final String MAP_NAME = "my-map";

    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/db?user=postgres&password=postgres";

    public static void main(String[] args) {
        log.info("Starting HZ Server");

        // Start client
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();
        populateIMap(hazelcastServerInstance);
        submitJob(hazelcastServerInstance);

        hazelcastServerInstance.shutdown();
        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void populateIMap(HazelcastInstance hazelcastInstance) {
        IMap<Integer, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (int index = 0; index < 100; index++) {
            map.put(index, String.valueOf(index));
        }
    }

    @SneakyThrows
    private static void submitJob(HazelcastInstance hazelcastInstance) {
        Pipeline pipeline = createPipeline();

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(IMapToPostgresJob.class);

        JetService jet = hazelcastInstance.getJet();
        Job job = jet.newJob(pipeline, jobConfig);
        job.join();
    }


    private static Pipeline createPipeline() {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source = Sources.map(MAP_NAME);

        String insertSql = "INSERT INTO " + TABLE_NAME + " (id,name) VALUES(?, ?) ON CONFLICT (id) DO NOTHING";
        Sink<Map.Entry<Integer, String>> sink = Sinks.jdbc(insertSql,
                DATABASE_URL,
                (stmt, item) -> {
                    stmt.setInt(1, item.getKey());
                    stmt.setString(2, item.getValue());
                });

        pipeline.readFrom(source)
                .writeTo(sink);
        return pipeline;
    }
}
