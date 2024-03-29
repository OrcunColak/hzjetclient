package com.colak.jet.genericmapstore;

import com.colak.jet.jdbc_dataconnection.CreatePostgresDataConnection;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapstore.GenericMapLoader;
import com.hazelcast.mapstore.GenericMapStore;
import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class CreateGenericMapStore {

    public final String MAP_NAME = "generic_map";
    public final String DB_TABLE_NAME = "myworker";

    public static void createGenericMapStore(HazelcastInstance hazelcastInstanceClient) {
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(GenericMapLoader.DATA_CONNECTION_REF_PROPERTY, CreatePostgresDataConnection.CONNECTION_NAME);
        mapStoreConfig.setProperty(GenericMapLoader.TYPE_NAME_PROPERTY, WorkerTableEntry.class.getName());
        mapStoreConfig.setProperty(GenericMapLoader.EXTERNAL_NAME_PROPERTY, DB_TABLE_NAME);
        mapStoreConfig.setProperty(GenericMapLoader.ID_COLUMN_PROPERTY, "id");
        mapStoreConfig.setProperty(GenericMapLoader.COLUMNS_PROPERTY, "id,name,ssn");
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = hazelcastInstanceClient.getConfig();
        config.addMapConfig(mapConfig);

        Map<Integer, WorkerTableEntry> map = hazelcastInstanceClient.getMap(MAP_NAME);
        WorkerTableEntry workerTableEntry = map.get(1);
        if (workerTableEntry != null) {
            System.out.println("GenericMapStore get succeeded");
        }
    }
}
