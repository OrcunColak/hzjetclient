package com.colak.jet.imapmapping;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;

import static java.lang.String.format;

/**
 * Create an example mapping
 */
@Slf4j
public class CreateIMapMappingTest {

    private static final String MAP_NAME = "myMap";

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        createMapping(hazelcastInstanceServer);
        log.info("Created Mapping");

        insertIntoMapping(hazelcastInstanceServer);

        selectOnMapping(hazelcastInstanceServer);

        hazelcastInstanceServer.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        // Add JetConfig
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);

        return Hazelcast.newHazelcastInstance(config);
    }


    private static void createMapping(HazelcastInstance hazelcastInstance) {
        // Mapping name is the same as table name
        String format = """
                CREATE OR REPLACE MAPPING %s (
                  __key INT, id INT, firstName VARCHAR, lastName VARCHAR
                ) TYPE IMap OPTIONS
                (
                  'keyFormat' = 'int',
                  'valueFormat' = 'compact',
                  'valueCompactTypeName' = 'person'
                )
                """;
        String createMappingQuery = format(format, MAP_NAME);

        SqlService sqlService = hazelcastInstance.getSql();
        sqlService.executeUpdate(createMappingQuery);
    }

    private static void insertIntoMapping(HazelcastInstance hazelcastInstance) {
        IMap<Integer, GenericRecord> map = hazelcastInstance.getMap(MAP_NAME);
        GenericRecord genericRecord = GenericRecordBuilder.compact("person")
                .setInt32("id", 1)
                .setString("firstName", "a")
                .setString("lastName", "b")
                .build();
        map.put(1, genericRecord);
    }

    private static void selectOnMapping(HazelcastInstance hazelcastInstance) {
        String selectSql = "SELECT * FROM " + MAP_NAME;

        SqlService sqlService = hazelcastInstance.getSql();
        int counter = 0;
        try (SqlResult sqlResult = sqlService.execute(selectSql)) {
            counter = showTable(sqlResult);
        }
        log.info("Selected {} rows", counter);
    }

    private static int showTable(SqlResult sqlResult) {
        SqlRowMetadata rowMetadata = sqlResult.getRowMetadata();
        int columnCount = rowMetadata.getColumnCount();
        Iterator<SqlRow> iterator = sqlResult.iterator();
        int counter = 0;
        while (iterator.hasNext()) {
            counter++;
            SqlRow sqlRow = iterator.next();
            for (int index = 0; index < columnCount; index++) {
                log.info("{} : {}", rowMetadata.getColumn(index).getName(), sqlRow.getObject(index));
            }
            log.info("---------");
        }
        return counter;
    }
}
