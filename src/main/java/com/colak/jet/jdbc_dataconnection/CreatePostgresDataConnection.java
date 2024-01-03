package com.colak.jet.jdbc_dataconnection;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlService;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CreatePostgresDataConnection {

    // Name of the connection pool
    public final String CONNECTION_NAME = "postgres_shared";

    public static void createDataConnection(HazelcastInstance hazelcastInstanceClient) {
        String databaseUrl = "jdbc:postgresql://localhost:5432/db";
        String userName = "postgres";
        String password = "postgres";
        createDataConnection(hazelcastInstanceClient, databaseUrl, userName, password);
    }

    // Create JDBC Connection Pool on cluster
    private static void createDataConnection(HazelcastInstance hazelcastInstanceClient,
                                             String databaseUrl,
                                             String userName,
                                             String password) {
        // CREATE DATA CONNECTION IF NOT EXISTS postgres TYPE JDBC SHARED OPTIONS('jdbcUrl'='jdbc:postgresql://localhost:5432/db','user'='postgres','password'='postgres')
        String sql = String.format("CREATE DATA CONNECTION IF NOT EXISTS %s TYPE JDBC SHARED OPTIONS('jdbcUrl'='%s','user'='%s','password'='%s')",
                CONNECTION_NAME, databaseUrl, userName, password);

        SqlService sqlService = hazelcastInstanceClient.getSql();
        sqlService.executeUpdate(sql);
    }
}
