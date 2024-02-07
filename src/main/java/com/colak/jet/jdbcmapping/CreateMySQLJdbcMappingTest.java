package com.colak.jet.jdbcmapping;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import static java.lang.String.format;

/**
 * Create an example mapping
 */
@Slf4j
public class CreateMySQLJdbcMappingTest {
    private static final String DB_TABLE_NAME = "typestable";
    private static final String CONNECTION_NAME = "mysql_shared";
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/db";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    public static void main(String[] args) throws Exception {
        createTable();

        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastInstanceServer = getHazelcastServerInstanceByConfig();

        createDataConnection(hazelcastInstanceServer);
        log.info("Created DataConnection");

        createMapping(hazelcastInstanceServer);
        log.info("Created Mapping");

        selectOnMapping(hazelcastInstanceServer);

        hazelcastInstanceServer.shutdown();

        log.info("Test completed");
    }

    private static void createTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
             Statement statement = connection.createStatement()) {

            // Drop the table if it exists
            String dropTableSQL = "DROP TABLE IF EXISTS " + DB_TABLE_NAME;
            statement.executeUpdate(dropTableSQL);
            log.info("Table dropped successfully.");

            // Create a new table
            String createTableSQL = "CREATE TABLE " + DB_TABLE_NAME +
                                    """
                                            (
                                              tinyint_unsigned TINYINT UNSIGNED
                                            )
                                            """;

            statement.executeUpdate(createTableSQL);
            log.info("Table created successfully.");
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

    private static void createDataConnection(HazelcastInstance hazelcastInstance) {
        String sql = String.format("CREATE DATA CONNECTION IF NOT EXISTS %s TYPE JDBC SHARED OPTIONS('jdbcUrl'='%s','user'='%s','password'='%s')",
                CONNECTION_NAME, JDBC_URL, USERNAME, PASSWORD);

        SqlService sqlService = hazelcastInstance.getSql();
        sqlService.executeUpdate(sql);
    }

    private static void createMapping(HazelcastInstance hazelcastInstance) {
        // Mapping name is the same as table name
        String format = """
                CREATE OR REPLACE MAPPING %s (
                  tinyint_unsigned SMALLINT
                ) DATA CONNECTION %s;
                """;
        String createMappingQuery = format(format, DB_TABLE_NAME, CONNECTION_NAME);

        SqlService sqlService = hazelcastInstance.getSql();
        sqlService.executeUpdate(createMappingQuery);
    }

    private static void selectOnMapping(HazelcastInstance hazelcastInstance) {
        String selectSql = "SELECT * from " + DB_TABLE_NAME;

        SqlService sqlService = hazelcastInstance.getSql();
        int counter = 0;
        try (SqlResult sqlResult = sqlService.execute(selectSql)) {
            Iterator<SqlRow> iterator = sqlResult.iterator();
            while (iterator.hasNext()) {
                counter++;
            }
        }
        log.info("Selected {} rows", counter);
    }
}
