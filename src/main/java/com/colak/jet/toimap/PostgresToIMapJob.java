package com.colak.jet.toimap;

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

@UtilityClass
public class PostgresToIMapJob {

    public final String TABLE_NAME = "my_worker";

    public final String MAP_NAME = "my-map";

    @SneakyThrows
    public void submitJob(HazelcastInstance hazelcastInstance, String databaseUrl) {
        Pipeline pipeline = createPipeline(databaseUrl);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(PostgresToIMapJob.class);

        JetService jet = hazelcastInstance.getJet();
        Job job = jet.newJob(pipeline, jobConfig);
        job.join();
    }

    private Pipeline createPipeline(String databaseUrl) {
        Pipeline pipeline = Pipeline.create();

        BatchSource<Map.Entry<Integer, String>> source = Sources.jdbc(databaseUrl,
                "SELECT * FROM " + TABLE_NAME,
                PostgresToIMapJob::mapOutput);

        Sink<Map.Entry<Integer, String>> sink = Sinks.map(MAP_NAME, Map.Entry::getKey, Map.Entry::getValue);

        pipeline.readFrom(source)
                .writeTo(sink);
        return pipeline;
    }

    private Map.Entry<Integer, String> mapOutput(ResultSet resultSet) throws SQLException {
        return Map.entry(resultSet.getInt("id"), resultSet.getString("name"));
    }
}
