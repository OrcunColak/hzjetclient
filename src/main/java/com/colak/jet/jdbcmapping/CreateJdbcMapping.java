package com.colak.jet.jdbcmapping;

import com.colak.jet.jdbc_dataconnection.CreateJdbcDataConnection;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import lombok.experimental.UtilityClass;

import static java.lang.String.format;

@UtilityClass
public class CreateJdbcMapping {

    // The name of the table on DB
    public static final String DB_TABLE_NAME = "myworker";
    public static void createMapping(HazelcastInstance hazelcastInstanceClient) {
        // Mapping name is the same as table name
        // Mapping needs a pre-created DATA CONNECTION
        String createMappingQuery = format("CREATE OR REPLACE MAPPING %s (id INTEGER,name VARCHAR,ssn VARCHAR) DATA CONNECTION %s",
                DB_TABLE_NAME, CreateJdbcDataConnection.CONNECTION_NAME);

        SqlService sqlService = hazelcastInstanceClient.getSql();
        SqlResult sqlResult = sqlService.execute(createMappingQuery);
        sqlResult.close();
    }
}
