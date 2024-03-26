package com.colak.jet.ingest;

import com.colak.jet.genericmapstore.WorkerTableEntry;
import com.colak.jet.createmapping.kafkamapping.KafkaMappingConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static com.hazelcast.jet.pipeline.JoinClause.onKeys;

@UtilityClass
public class IngestJob {

    public final String TABLE_NAME = "myworker";

    public final String MAP_NAME = "my-map";

    @SneakyThrows
    public void submitJob(HazelcastInstance hazelcastInstance, String databaseUrl) {
        Pipeline pipeline = createPipeline(databaseUrl);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(IngestJob.class);
        jobConfig.addClass(WorkerTableEntry.class,TradeEntry.class, EnrichedTradeEntry.class);

        JetService jet = hazelcastInstance.getJet();
        Job job = jet.newJob(pipeline, jobConfig);
        job.join();
    }

    public static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r087.us-west2.gcp.confluent.cloud:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.mechanism","PLAIN");
        String format = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                System.getenv("KAFKA_CONFLUENT_USERNAME"), System.getenv("KAFKA_CONFLUENT_PASSWORD"));
        properties.setProperty("sasl.jaas.config", format);
        return properties;
    }

    private Pipeline createPipeline(String databaseUrl) {
        Pipeline pipeline = Pipeline.create();

        Properties properties = properties();
        StreamSource<TradeEntry> kafkaSource = KafkaSources.kafka(properties,
                (ConsumerRecord<BigInteger,String> consumerRecord) -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(consumerRecord.value(), TradeEntry.class);

                    //JsonUtil.beanFrom(consumerRecord.value(), TradeEntry.class)}
                },
                KafkaMappingConfig.kafkaTopicName);

        BatchSource<WorkerTableEntry> jdbcSource = Sources.jdbc(databaseUrl,
                "SELECT * FROM " + TABLE_NAME, IngestJob::mapOutput);

        Sink<EnrichedTradeEntry> sink = Sinks.map(MAP_NAME, EnrichedTradeEntry::getId, FunctionEx.identity());

        StreamStage<TradeEntry> kafkaStream = pipeline.readFrom(kafkaSource).withoutTimestamps();
        BatchStage<WorkerTableEntry> jdbcBatch = pipeline.readFrom(jdbcSource);

        StreamStage<EnrichedTradeEntry> joined = kafkaStream.innerHashJoin(jdbcBatch,
                onKeys(TradeEntry::getId, WorkerTableEntry::getId),
                EnrichedTradeEntry::new);

        joined.writeTo(sink);

        return pipeline;
    }

    private WorkerTableEntry mapOutput(ResultSet resultSet) throws SQLException {
        return new WorkerTableEntry(resultSet.getInt("id"), resultSet.getString("name"), resultSet.getString("ssn"));
    }
}
