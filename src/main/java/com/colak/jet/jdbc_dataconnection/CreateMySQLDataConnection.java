package com.colak.jet.jdbc_dataconnection;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CreateMySQLDataConnection {

    // Name of the connection pool
    public final String CONNECTION_NAME = "mysql_shared";

    public static void createDataConnection(HazelcastInstance hazelcastInstanceClient) {
        String databaseUrl = "jdbc:mysql://localhost:3306/db";
        String userName = "root";
        String password = "root";
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
        SqlResult sqlResult = sqlService.execute(sql);
        sqlResult.close();
    }
}
