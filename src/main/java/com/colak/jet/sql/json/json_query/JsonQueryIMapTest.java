package com.colak.jet.sql.json.json_query;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import lombok.extern.slf4j.Slf4j;

import static java.lang.String.format;

@Slf4j
public class JsonQueryIMapTest {

    public static final String MAP_NAME = "product";

    public static void main(String[] args) {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();
        populateMap(hazelcastInstanceServer);

        createMapping(hazelcastInstanceServer);

        testSelect(hazelcastInstanceServer);

        hazelcastInstanceServer.shutdown();

        log.info("Test completed");

    }

    private static void populateMap(HazelcastInstance hazelcastInstanceServer) {
        IMap<String, HazelcastJsonValue> map = hazelcastInstanceServer.getMap(MAP_NAME);
        map.put("1", new HazelcastJsonValue("""
                {
                  "field1": {
                    "hasEvents": false
                  }
                }
                """));

        map.put("2", new HazelcastJsonValue("""
                {
                  "field1": {
                    "hasEvents": true
                  }
                }
                """));
    }

    private static void testSelect(HazelcastInstance hazelcastInstanceServer) {
        SqlService sqlService = hazelcastInstanceServer.getSql();
        try (SqlResult sqlResult = sqlService.execute("SELECT __key, this FROM product WHERE JSON_QUERY(this, '$.field1.hasEvents') = 'true'")) {
            if (sqlResult.isRowSet()) {
                String[] columnNames = getColumnNames(sqlResult);
                int numberOfColumns = columnNames.length;
                for (SqlRow sqlRow : sqlResult) {
                    for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
                        Object columnValue = sqlRow.getObject(columnIndex);
                        log.info("{} : {}", columnNames[columnIndex], columnValue);
                    }
                }
            }
        }
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        // Add JetConfig
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void createMapping(HazelcastInstance hazelcastInstanceClient) {
        String createMappingQuery = format("CREATE OR REPLACE MAPPING %s Type IMap OPTIONS('keyFormat'='varchar', 'valueFormat'='json')",
                MAP_NAME);

        SqlService sqlService = hazelcastInstanceClient.getSql();
        sqlService.executeUpdate(createMappingQuery);
    }

    private static String[] getColumnNames(SqlResult sqlResult) {
        SqlRowMetadata rowMetadata = sqlResult.getRowMetadata();
        return rowMetadata.getColumns().stream()
                .map(SqlColumnMetadata::getName)
                .toArray(String[]::new);
    }
}
