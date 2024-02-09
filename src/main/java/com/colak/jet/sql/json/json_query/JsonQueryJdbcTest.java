package com.colak.jet.sql.json.json_query;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

import static java.lang.String.format;

@Slf4j
public class JsonQueryJdbcTest {

    private static final String CONNECTION_NAME = "postgres_shared";

    public static final String DB_TABLE_NAME = "product";

    public static void main(String[] args) {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();
        createMapping(hazelcastInstanceServer);

        testSelect(hazelcastInstanceServer);

        hazelcastInstanceServer.shutdown();

        log.info("Test completed");

    }

    private static void testSelect(HazelcastInstance hazelcastInstanceServer) {
        SqlService sqlService = hazelcastInstanceServer.getSql();
        try (SqlResult sqlResult = sqlService.execute("SELECT * FROM product WHERE JSON_QUERY(data, '$.field1.hasEvents') = 'true'")) {
            if (sqlResult.isRowSet()) {
                String[] columnNames = getColumnNames(sqlResult);
                int numberOfColumns = columnNames.length;
                for (SqlRow sqlRow : sqlResult) {
                    for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
                        Object columnValue = sqlRow.getObject(columnIndex);
                        log.info("{} : {}", columnNames[columnIndex],columnValue);
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

        // Add DataConnectionConfig
        DataConnectionConfig dataConnectionConfig = getDataConnectionConfig();
        config.addDataConnectionConfig(dataConnectionConfig);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static DataConnectionConfig getDataConnectionConfig() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setName(CONNECTION_NAME);
        dataConnectionConfig.setShared(true);
        dataConnectionConfig.setType("JDBC");

        Properties properties = new Properties();
        properties.put("jdbcUrl", "jdbc:postgresql://localhost:5432/db");
        properties.put("user", "postgres");
        properties.put("password", "postgres");
        dataConnectionConfig.setProperties(properties);
        return dataConnectionConfig;
    }

    private static void createMapping(HazelcastInstance hazelcastInstanceClient) {
        String createMappingQuery = format("CREATE OR REPLACE MAPPING %s (id INTEGER,data VARCHAR) DATA CONNECTION %s",
                DB_TABLE_NAME, CONNECTION_NAME);

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
