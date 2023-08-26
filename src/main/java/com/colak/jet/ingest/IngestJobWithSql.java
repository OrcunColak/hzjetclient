package com.colak.jet.ingest;

import com.colak.jet.jdbcmapping.CreateJdbcMapping;
import com.colak.jet.kafkamapping.KafkaMappingConfig;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import lombok.experimental.UtilityClass;

import static java.lang.String.format;

@UtilityClass
public class IngestJobWithSql {

    public static final String INGEST_JOB_NAME = "myworker";
    public static final String SINK_MAP = "sinkMap";


    public void createSinkMap(SqlService sqlService) {
        String createSinkMappingQuery =
                format("CREATE OR REPLACE MAPPING %s (\n" +
                       "    __key BIGINT,\n" +
                       "    ticker VARCHAR,\n" +
                       "    price DECIMAL,\n" +
                       "    amount BIGINT," +
                       "    ssn VARCHAR\n" +
                       ")\n" +
                       "TYPE IMap\n" +
                       "OPTIONS (\n" +
                       "    'keyFormat'='bigint',\n" +
                       "    'valueFormat'='json-flat'\n" +
                       ");", SINK_MAP);

        sqlService.execute(createSinkMappingQuery);

        try (SqlResult sqlResult = sqlService.execute(createSinkMappingQuery)) {

        }
    }

    public void submit(SqlService sqlService) {
        createSinkMap (sqlService);

        String createJobQuery =
                format("CREATE JOB %s\n" +
                       "OPTIONS (\n" +
                       "  'processingGuarantee' = 'exactlyOnce'\n" +
                       ") AS\n" +
                       "SINK INTO %s\n" +
                       "SELECT trade.id, trade.ticker, trade.price, trade.amount, worker.ssn FROM %s AS trade JOIN %s AS worker ON trade.id = worker.id;",
                        INGEST_JOB_NAME, SINK_MAP, KafkaMappingConfig.kafkaMappingName, CreateJdbcMapping.DB_TABLE_NAME);

        try (SqlResult sqlResult = sqlService.execute(createJobQuery)) {

        }
    }
}
