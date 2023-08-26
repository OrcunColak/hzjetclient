package com.colak.jet.fromimap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class IMapToPostgresJob {
    public final String TABLE_NAME = "my_worker_backup";

    public final String MAP_NAME = "my-map";

    @SneakyThrows
    public void submitJob(HazelcastInstance hazelcastInstance, String databaseUrl) {
        Pipeline pipeline = createPipeline(databaseUrl);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(IMapToPostgresJob.class);

        JetService jet = hazelcastInstance.getJet();
        Job job = jet.newJob(pipeline, jobConfig);
        job.join();
    }


    private Pipeline createPipeline(String databaseUrl) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source = Sources.map(MAP_NAME);

        String QUERY = "INSERT INTO " + TABLE_NAME + " (id,name) VALUES(?, ?) ON CONFLICT (id) DO NOTHING";
        Sink<Map.Entry<Integer, String>> sink = Sinks.jdbc(QUERY,
                databaseUrl,
                (stmt, item) -> {
                    stmt.setInt(1, item.getKey());
                    stmt.setString(2, item.getValue());
                });

        pipeline.readFrom(source)
                .writeTo(sink);
        return pipeline;
    }
}
