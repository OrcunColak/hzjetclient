package com.colak.jet.submitjob.join_imap;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Example job to read from Postgres and then join with IMap and write to another IMap
 */
@Slf4j
class PostgresJoinWithIMapJobTest {
    private static final String TABLE_NAME = "myworker";

    private static final String MAP_NAME = "my-map";
    private static final String MAP_NAME_ENRICHED = "my-map-enriched";

    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/db?user=postgres&password=postgres";

    private static final int MAX_ITEMS = 20;

    public static void main(String[] args) {

        log.info("Creating database table");
        populateDatabase();

        log.info("Starting HZ Server");

        // Start client
        HazelcastInstance hazelcastServerInstance = getHazelcastServerInstanceByConfig();
        populateIMap(hazelcastServerInstance);
        clearEnrichedIMap(hazelcastServerInstance);

        submitJob(hazelcastServerInstance);

        printEnrichedIMap(hazelcastServerInstance);

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

    @SneakyThrows
    private static void populateDatabase() {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL);
             Statement statement = connection.createStatement()) {
            String createTableQuery = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                                      + " (id INT NOT NULL, name VARCHAR(45), ssn VARCHAR(45), PRIMARY KEY (id))";

            // Create the table if it does not exist
            statement.executeUpdate(createTableQuery);

            // Truncate the table if it exists
            String truncateTableQuery = "TRUNCATE TABLE " + TABLE_NAME;
            statement.executeUpdate(truncateTableQuery);


            // Populate the table
            String insertQuery = "INSERT INTO " + TABLE_NAME + " (id,name,ssn) VALUES (?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                for (int index = 0; index < MAX_ITEMS; index++) {
                    preparedStatement.setInt(1, index);
                    preparedStatement.setString(2, getWorkerName(index));
                    preparedStatement.setString(3, getSsn(index));

                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        }
    }

    private static String getWorkerName(int index) {
        return "worker" + index;
    }

    private static String getSsn(int index) {
        return "ssn" + index;
    }

    private static void populateIMap(HazelcastInstance hazelcastInstance) {
        IMap<Integer, WorkerMapEntry> map = hazelcastInstance.getMap(MAP_NAME);
        for (int index = 0; index < MAX_ITEMS; index++) {
            map.put(index, getWorkerMapEntry(index));
        }
    }

    public static WorkerMapEntry getWorkerMapEntry(int index) {
        return new WorkerMapEntry(index, "surname" + index);
    }

    @SneakyThrows
    private static void submitJob(HazelcastInstance hazelcastInstance) {
        Pipeline pipeline = createPipeline(hazelcastInstance);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(PostgresJoinWithIMapJobTest.class);

        JetService jet = hazelcastInstance.getJet();
        Job job = jet.newJob(pipeline, jobConfig);
        job.join();
    }

    private static Pipeline createPipeline(HazelcastInstance hazelcastInstance) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<WorkerTableEntry> tableSource = Sources.jdbc(DATABASE_URL,
                "SELECT * FROM " + TABLE_NAME,
                PostgresJoinWithIMapJobTest::mapToWorkerTableEntry);


        BatchStage<WorkerTableEntry> tableBatch = pipeline.readFrom(tableSource);

        IMap<Integer, WorkerMapEntry> mapSource = hazelcastInstance.getMap(MAP_NAME);

        BatchStage<Map.Entry<Integer, String>> joined =
                tableBatch.mapUsingIMap(mapSource,
                        WorkerTableEntry::getId,
                        (workerTableEntry, workerMapEntry) -> {
                            String enrichedName = workerTableEntry.getName() + " " + workerMapEntry.getSurname();
                            return Map.entry(workerTableEntry.getId(), enrichedName);
                        }
                );

        joined.writeTo(Sinks.map(MAP_NAME_ENRICHED));
        return pipeline;
    }

    private static WorkerTableEntry mapToWorkerTableEntry(ResultSet resultSet) throws SQLException {
        return new WorkerTableEntry(
                resultSet.getInt("id"),
                resultSet.getString("name"),
                resultSet.getString("ssn"));
    }


    private static void clearEnrichedIMap(HazelcastInstance hazelcastServerInstance) {
        IMap<Integer, String> map = hazelcastServerInstance.getMap(MAP_NAME_ENRICHED);
        map.clear();
    }

    private static void printEnrichedIMap(HazelcastInstance hazelcastServerInstance) {
        IMap<Integer, String> map = hazelcastServerInstance.getMap(MAP_NAME_ENRICHED);
        for (Map.Entry<Integer, String> entry : map) {
            log.info("Key : {} Value : {}", entry.getKey(), entry.getValue());
        }
    }
}
