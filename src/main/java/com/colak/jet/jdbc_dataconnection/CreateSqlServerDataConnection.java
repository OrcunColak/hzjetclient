package com.colak.jet.jdbc_dataconnection;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlService;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CreateSqlServerDataConnection {

    // Name of the connection pool
    public final String CONNECTION_NAME = "sqlserver_shared";

    public static void createDataConnection(HazelcastInstance hazelcastInstanceClient) {
        String databaseUrl = "jdbc:sqlserver://localhost:1433;databaseName=ankara;encrypt=true;trustServerCertificate=true";
        String userName = "migrator";
        String password = "migrator123!";
        createDataConnection(hazelcastInstanceClient, databaseUrl, userName, password);
    }

    // Create JDBC Connection Pool on cluster
    private static void createDataConnection(HazelcastInstance hazelcastInstanceClient,
                                             String databaseUrl,
                                             String userName,
                                             String password) {
        String sql = String.format("CREATE DATA CONNECTION IF NOT EXISTS %s TYPE JDBC SHARED OPTIONS('jdbcUrl'='%s','user'='%s','password'='%s')",
                CONNECTION_NAME, databaseUrl, userName, password);

        SqlService sqlService = hazelcastInstanceClient.getSql();
        sqlService.executeUpdate(sql);
    }
}
