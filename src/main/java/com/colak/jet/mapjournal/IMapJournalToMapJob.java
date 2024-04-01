package com.colak.jet.mapjournal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Predicate;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
class IMapJournalToMapJob {

    private static final String SOURCE_MAP = "foo";
    private static final String SINK_MAP = "bar";


    public static void main(String[] args) throws InterruptedException {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        populateSourceMap(hazelcastInstanceServer);
        submitJob(hazelcastInstanceServer);
        populateSourceMap(hazelcastInstanceServer);


        TimeUnit.SECONDS.sleep(10);

        printSinkMap(hazelcastInstanceServer);

        hazelcastInstanceServer.shutdown();

        log.info("Test completed");
    }


    private static void populateSourceMap(HazelcastInstance hazelcastInstance) {
        IMap<Integer, HazelcastJsonValue> map = hazelcastInstance.getMap(SOURCE_MAP);
        map.put(1, new HazelcastJsonValue("""
                {
                  "payload": {
                    "integerList": [1, 2, 3, 4, 9145]
                  }
                }
                """));
    }

    private static void printSinkMap(HazelcastInstance hazelcastInstance) {
        log.info("Printing sink map");
        IMap<Integer, HazelcastJsonValue> sinkMap = hazelcastInstance.getMap(SINK_MAP);
        for (Map.Entry<Integer, HazelcastJsonValue> mapEntry : sinkMap) {
            log.info("Key : {} , Value : {}", mapEntry.getKey(), mapEntry.getValue());
        }
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        EventJournalConfig eventJournalMapConfig = new EventJournalConfig()
                .setEnabled(true)
                .setCapacity(5000)
                .setTimeToLiveSeconds(20);

        config.getMapConfig(SOURCE_MAP).setEventJournalConfig(eventJournalMapConfig);

        // Add JetConfig
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);


        return Hazelcast.newHazelcastInstance(config);
    }

    public static void submitJob(HazelcastInstance hazelcastInstance) {
        var pipeline = Pipeline.create();
        IMap<Integer, HazelcastJsonValue> sinkMap = hazelcastInstance.getMap(SINK_MAP);

        FunctionEx<EventJournalMapEvent<Integer, HazelcastJsonValue>, Map.Entry<Integer, HazelcastJsonValue>> projection = Util.mapEventToEntry();

        StreamSource<Map.Entry<Integer, HazelcastJsonValue>> source = Sources.mapJournal(SOURCE_MAP,
                JournalInitialPosition.START_FROM_OLDEST,
                projection::apply,
                mapEntry -> {
                    HazelcastJsonValue newValue = mapEntry.getNewValue();
                    String json = newValue.getValue();
                    Predicate predicate = ctx -> {
                        int value = ctx.item(Integer.class);
                        return value > 9144;
                    };

                    DocumentContext documentContext = JsonPath.parse(json);
                    List<Integer> list  = documentContext.read("$.payload.integerList[?]", predicate);
                    return !list.isEmpty();
                });

        var sink = Sinks.map(sinkMap);
        pipeline.readFrom(source)
                .withoutTimestamps()
                .writeTo(sink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(IMapJournalToMapJob.class);

        JetService jetService = hazelcastInstance.getJet();
        jetService.newJob(pipeline, jobConfig);

        /*
        try {
            var pipeline = Pipeline.create();
            var source = Sources.mapJournal(SOURCE_MAP, JournalInitialPosition.START_FROM_OLDEST);
            var sink = Sinks.map(SINK_MAP);
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
        }*/
    }
}
