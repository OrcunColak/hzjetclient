package com.colak.jet.activemq;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import jakarta.jms.Message;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

@UtilityClass
@Slf4j
public class ArtemisToArtemisJob {

    // Job to read from one queue and write to another queue
    public static void submit(HazelcastInstance hazelcastInstance) {
        Pipeline pipeline = Pipeline.create();
        StreamSource<Message> sourceJms = Sources.jmsQueue("foo",
                () -> new ActiveMQConnectionFactory("tcp://localhost:61616"));

        Sink<Object> sinkJms = Sinks.jmsQueue("bar",
                () -> new ActiveMQConnectionFactory("tcp://localhost:61616"));

        pipeline.readFrom(sourceJms)
                .withoutTimestamps()
                .writeTo(sinkJms);

        JetService jetService = hazelcastInstance.getJet();

        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.addClass(ArtemisToArtemisJob.class);
            jobConfig.addJar("D:\\Users\\user\\.m2\\repository\\org\\apache\\activemq\\artemis-jakarta-client-all\\2.30.0\\artemis-jakarta-client-all-2.30.0.jar");

            // This is a stream job
            jetService.newJob(pipeline, jobConfig);
        } catch (Exception exception) {
            log.error("An exception occurred!", exception);
        }
    }
}
