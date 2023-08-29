package com.colak.jet.show_resources;

import com.colak.jet.jdbc_dataconnection.CreateMySQLDataConnection;
import com.colak.jet.jdbc_dataconnection.CreatePostgresDataConnection;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.stream.Collectors;

@UtilityClass
@Slf4j
public class ShowResources {

    public static void submitForPostgres(HazelcastInstance hazelcastInstanceClient) {

        try {
            CreatePostgresDataConnection.createDataConnection(hazelcastInstanceClient);

            SqlService sqlService = hazelcastInstanceClient.getSql();
            try (SqlResult sqlResult = sqlService.execute("SHOW RESOURCES FOR " + CreatePostgresDataConnection.CONNECTION_NAME)) {
                SqlRowMetadata rowMetadata = sqlResult.getRowMetadata();
                String columnHeader = rowMetadata.getColumns().stream()
                        .map(sqlColumnMetadata -> Objects.toString(sqlColumnMetadata))
                        .collect(Collectors.joining(" "));


                log.info(columnHeader);

                for (SqlRow sqlRow : sqlResult) {
                    log.info(sqlRow.getObject(0) + " " + sqlRow.getObject(1));
                }
            }
        } catch (Exception exception) {
            log.error("Exception caught ", exception);
        }
    }

    public static void submitForMySql(HazelcastInstance hazelcastInstanceClient) {

        try {
            CreateMySQLDataConnection.createDataConnection(hazelcastInstanceClient);

            SqlService sqlService = hazelcastInstanceClient.getSql();
            try (SqlResult sqlResult = sqlService.execute("SHOW RESOURCES FOR " + CreateMySQLDataConnection.CONNECTION_NAME)) {
                SqlRowMetadata rowMetadata = sqlResult.getRowMetadata();
                String columnHeader = rowMetadata.getColumns().stream()
                        .map(sqlColumnMetadata -> Objects.toString(sqlColumnMetadata))
                        .collect(Collectors.joining(" "));


                log.info(columnHeader);

                for (SqlRow sqlRow : sqlResult) {
                    log.info(sqlRow.getObject(0) + " " + sqlRow.getObject(1));
                }
            }
        } catch (Exception exception) {
            log.error("Exception caught ", exception);
        }
    }
}
