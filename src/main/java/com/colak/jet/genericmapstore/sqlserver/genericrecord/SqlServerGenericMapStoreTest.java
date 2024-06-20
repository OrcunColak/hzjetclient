package com.colak.jet.genericmapstore.sqlserver.genericrecord;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.mapstore.GenericMapLoader;
import com.hazelcast.mapstore.GenericMapStore;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * Start SQL Server using docker-compose. The server populates some tables
 * Create a
 * 1. DataConnection
 * 2. MapConfig
 * 3. Test IMap using generic map store
 */
@Slf4j
class SqlServerGenericMapStoreTest {

    private static final String MAP_NAME = "generic_map";

    // Table is ankara.dbo.myworker
    private static final String DB_TABLE_NAME = "dbo.myworker";

    private static final String CONNECTION_NAME = "sqlserver_shared";

    public static void main(String[] args) {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();


        testGetFromMap(hazelcastServer);
        testPutToMap(hazelcastServer);

        hazelcastServer.shutdown();

        log.info("Test completed");
    }

    private static void testGetFromMap(HazelcastInstance hazelcastServer) {
        IMap<Integer, GenericRecord> map = hazelcastServer.getMap(MAP_NAME);
        GenericRecord sqlServerWorkerTableEntry = map.get(1);
        if (sqlServerWorkerTableEntry != null) {
            log.info("GenericMapStore get succeeded with sqlServerWorkerTableEntry {}", sqlServerWorkerTableEntry);
        } else {
            log.info("GenericMapStore get failed");
        }
    }

    private static void testPutToMap(HazelcastInstance hazelcastServer) {
        IMap<Integer, GenericRecord> map = hazelcastServer.getMap(MAP_NAME);
        int key = 1;
        GenericRecord genericRecord = map.get(key);
        // Update entry
        GenericRecord newGenericRecord = genericRecord.newBuilder()
                .setInt32("id", 1)
                .setString("name", "new name")
                .setString("ssn", "208")
                .build();

        // Put to map
        GenericRecord oldGenericRecord = map.put(key, newGenericRecord);
        if (oldGenericRecord != null) {
            log.info("GenericMapStore put succeeded with oldGenericRecord {}", oldGenericRecord);
        } else {
            log.info("GenericMapStore put failed");
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

        // MapConfig
        MapConfig mapConfig = getMapConfig();
        config.addMapConfig(mapConfig);

        return Hazelcast.newHazelcastInstance(config);
    }


    private static DataConnectionConfig getDataConnectionConfig() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setName(CONNECTION_NAME);
        dataConnectionConfig.setShared(true);
        dataConnectionConfig.setType("JDBC");

        Properties properties = new Properties();
        properties.put("jdbcUrl", "jdbc:sqlserver://localhost:1433;databaseName=ankara;encrypt=true;trustServerCertificate=true");
        properties.put("user", "migrator");
        properties.put("password", "migrator123!");
        dataConnectionConfig.setProperties(properties);
        return dataConnectionConfig;
    }


    private static MapConfig getMapConfig() {
        // Create MapStoreConfig
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(GenericMapLoader.DATA_CONNECTION_REF_PROPERTY, CONNECTION_NAME);
        mapStoreConfig.setProperty(GenericMapLoader.EXTERNAL_NAME_PROPERTY, DB_TABLE_NAME);
        mapStoreConfig.setProperty(GenericMapLoader.ID_COLUMN_PROPERTY, "id");
        mapStoreConfig.setProperty(GenericMapLoader.COLUMNS_PROPERTY, "id,name,ssn");

        // Create MapConfig
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return mapConfig;
    }

}
