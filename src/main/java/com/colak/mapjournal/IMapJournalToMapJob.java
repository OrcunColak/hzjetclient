package com.colak.mapjournal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@UtilityClass
@Slf4j
public class IMapJournalToMapJob {

    public static void submit(HazelcastInstance hazelcastInstance) {
        try {
            var pipeline = Pipeline.create();
            var source = Sources.mapJournal("foo", JournalInitialPosition.START_FROM_OLDEST);
            var sink = Sinks.map("fizz");
            pipeline.readFrom(source)
                    .withIngestionTimestamps()
                    .setName("ReadFromFooMap")
                    .map(e -> {
                        var k = "time-" + System.currentTimeMillis() + "-" + e.getKey();
                        return Map.entry((Object) k, e.getValue());
                    })
                    .setName("TransformTheKey")
                    .writeTo(sink)
                    .setName("WriteToFizzMap");

            JobConfig jobConfig = new JobConfig();
            jobConfig.addClass(IMapJournalToMapJob.class);

            JetService jetService = hazelcastInstance.getJet();
            Job job = jetService.newJob(pipeline, jobConfig);
            job.join();

        } catch (Exception exception) {
            log.error("An exception occurred!", exception);
        }
    }
}
