package com.colak;

import com.colak.mapjournal.IMapJournalToMapJob;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Starting HZ Client");
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance hazelcastInstanceClient = HazelcastClient.newHazelcastClient(clientConfig);

        IMapJournalToMapJob.submit(hazelcastInstanceClient);

//        ArtemisToArtemisJob.submit();

//
//        String databaseUrl = "jdbc:postgresql://postgresql:5432/db?user=postgres&password=postgres";
//
//        CreateJdbcDataConnection.createDataConnection(hazelcastInstanceClient, databaseUrl, "postgres", "postgres");
//        CreateJdbcMapping.createMapping(hazelcastInstanceClient);
//
//        CreateKafkaMapping.createMapping(hazelcastInstanceClient);
//
//        IngestJob.submitJob(hazelcastInstanceClient, databaseUrl);

//        CreateGenericMapStore.createGenericMapStore(hazelcastInstanceClient);


//        String connectorJar = "postgresql-42.6.0.jar";
//        JdbcToIMapJob.submit(hazelcastInstanceClient, baseJdbcURL, connectorJar);
//        IMapToJdbcJob.submit(hazelcastInstanceClient, baseJdbcURL, connectorJar);
    }

}