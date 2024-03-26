package com.colak.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Example for IMap persistence
 */
class PersistenceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceTest.class);

    private static final String MAP_NAME = "mymap";

    public static void main(String[] args) {
        LOGGER.info("Starting HZ Client");

        System.setProperty(BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION, "5.3.0");
        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        testPutEntries(hazelcastServer);

        hazelcastServer.shutdown();
        LOGGER.info("Test completed");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        String licenseKey = System.getenv("LICENSE_KEY");
        config.setLicenseKey(licenseKey);

        // enable persistence on the member
        PersistenceConfig persistenceConfig = new PersistenceConfig();
        persistenceConfig.setEnabled(true);
        persistenceConfig.setBaseDir(new File("persistence1"));
        config.setPersistenceConfig(persistenceConfig);

        //  Configure to persist entries on disk for a map
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        DataPersistenceConfig dataPersistenceConfig = mapConfig.getDataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        dataPersistenceConfig.setFsync(true);
        config.addMapConfig(mapConfig);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void testPutEntries(HazelcastInstance hazelcastClient) {
        IMap<Integer, Integer> map = hazelcastClient.getMap(MAP_NAME);
        for (int index = 0; index < 100; index++) {
            map.put(index, index);
        }
    }

}
