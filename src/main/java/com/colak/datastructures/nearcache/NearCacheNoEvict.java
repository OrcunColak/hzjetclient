package com.colak.datastructures.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.nearcache.NearCacheStats;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class NearCacheNoEvict {

    private static final String MAP_NAME = "mostlyReadMap";
    private static final int MAP_SIZE = 100;

    public static void main(String[] args) {
        log.info("Starting NearCacheNoEvict");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        testNearCacheNoEvict(hazelcastServer);
        log.info("Ending NearCacheNoEvict");
    }

    public static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();


        MapConfig mapConfig = config.getMapConfig(MAP_NAME);

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setSize(MAP_SIZE / 2);

        NearCacheConfig nearCacheConfig = getNearCacheConfig();
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static NearCacheConfig getNearCacheConfig() {
        // Create a Map having NearCacheConfig
        // The NearCacheConfig has max size but eviction is none.
        // So, first N items will be in near cache
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setSize(MAP_SIZE / 2)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);

        return new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setInvalidateOnChange(true)
                .setSerializeKeys(true)
                .setEvictionConfig(evictionConfig)
                .setCacheLocalEntries(true)
                ;
    }

    // Test that near cache does not evict because of EvictionPolicy.NONE
    public static void testNearCacheNoEvict(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Integer> map = hazelcastInstance.getMap(MAP_NAME);
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                super.entryEvicted(event);
                log.info("LocalEntryListener entryEvicted {}", event.getKey());
            }
        });

        for (int index = 0; index < MAP_SIZE; index++) {
            map.put(index, index);
        }
        for (int index = 0; index < MAP_SIZE; index++) {
            map.get(index);
        }
        map.remove(0);
        LocalMapStats localMapStats = map.getLocalMapStats();
        NearCacheStats nearCacheStats = localMapStats.getNearCacheStats();
        log.info("nearCacheStats {}", nearCacheStats);
    }
}
