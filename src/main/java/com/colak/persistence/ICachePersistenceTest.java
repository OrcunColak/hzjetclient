package com.colak.persistence;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * Example for IMap persistence
 */
@Slf4j
class ICachePersistenceTest {

    private static final String CACHE_NAME = "my-cache";

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
        persistenceConfig.setBaseDir(new File("icache_persistence"));
        config.setPersistenceConfig(persistenceConfig);

        //  Configure to persist entries on disk for a map
        CacheSimpleConfig cacheConfig = config.getCacheConfig(CACHE_NAME);
        MerkleTreeConfig merkleTreeConfig = cacheConfig.getMerkleTreeConfig();
        merkleTreeConfig.setEnabled(true);

        DataPersistenceConfig dataPersistenceConfig = cacheConfig.getDataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        dataPersistenceConfig.setFsync(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void testPutEntries(HazelcastInstance hazelcastClient) {
        ICacheManager cacheManager = hazelcastClient.getCacheManager();
        ICache<Integer, Integer> map = cacheManager.getCache(CACHE_NAME);
        for (int index = 0; index < 100; index++) {
            map.put(index, index);
        }
    }
}
