package com.colak.jet.createmapping.kafkamapping;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlService;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@UtilityClass
public class CreateKafkaMapping {

    public static void createMapping(HazelcastInstance hazelcastInstanceClient) {
        String createMappingQuery =
                format("CREATE OR REPLACE MAPPING %s (\n" +
                       "    id BIGINT,\n" +
                       "    ticker VARCHAR,\n" +
                       "    price DECIMAL,\n" +
                       "    amount BIGINT)\n" +
                       "TYPE Kafka\n" +
                       "OPTIONS (\n" +
                       "    'valueFormat' = 'json-flat',\n" +
                       "    'bootstrap.servers' = '%s',\n" +
                       "    'security.protocol' = 'SASL_SSL',\n" +
                       "    'sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";',\n" +
                       "    'sasl.mechanism' = 'PLAIN',\n" +
                       "    'client.dns.lookup' = 'use_all_dns_ips',\n" +
                       "    'session.timeout.ms' = '45000',\n" +
                       "    'acks' = 'all'\n" +
                       ");", KafkaMappingConfig.kafkaMappingName, KafkaMappingConfig.bootstrapServers, KafkaMappingConfig.confluentUsername, KafkaMappingConfig.confluentPassword);

        SqlService sqlService = hazelcastInstanceClient.getSql();
        sqlService.executeUpdate(createMappingQuery);

        populateMainMapWithTwoEntries(sqlService);

    }

    @SneakyThrows
    private static void populateMainMapWithTwoEntries(SqlService sqlService) {
        // It seems tests perform actions too quick so a bit of delay is needed
        // to wait until the cluster is fully functional
        TimeUnit.SECONDS.sleep(10);

        // This will insert JSON object as value to Kafka topic
        String insertQuery =
                format("INSERT INTO %s VALUES\n" +
                       "  (1, 'ABCD', 5.5, 10),\n" +
                       "  (2, 'EFGH', 14, 20);", KafkaMappingConfig.kafkaMappingName);

        sqlService.executeUpdate(insertQuery);
    }
}
