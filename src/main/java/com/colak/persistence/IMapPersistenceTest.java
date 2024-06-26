package com.colak.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * Example for IMap persistence
 */
@Slf4j
class IMapPersistenceTest {

    private static final String MAP_NAME = "my-map";

    public static void main(String[] args) {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        testPutEntries(hazelcastServer);

        hazelcastServer.shutdown();
        log.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        String licenseKey = System.getenv("LICENSE_KEY");
        config.setLicenseKey(licenseKey);

        // enable persistence on the member
        PersistenceConfig persistenceConfig = new PersistenceConfig();
        persistenceConfig.setEnabled(true);
        persistenceConfig.setBaseDir(new File("imap_persistence"));
        config.setPersistenceConfig(persistenceConfig);

        //  Configure to persist entries on disk for a map
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        MerkleTreeConfig merkleTreeConfig = mapConfig.getMerkleTreeConfig();
        merkleTreeConfig.setEnabled(true);

        DataPersistenceConfig dataPersistenceConfig = mapConfig.getDataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        dataPersistenceConfig.setFsync(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void testPutEntries(HazelcastInstance hazelcastClient) {
        IMap<Integer, Integer> map = hazelcastClient.getMap(MAP_NAME);
        for (int index = 0; index < 100; index++) {
            map.put(index, index);
        }
    }
}
