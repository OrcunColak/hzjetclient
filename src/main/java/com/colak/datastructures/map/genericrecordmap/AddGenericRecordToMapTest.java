package com.colak.datastructures.map.genericrecordmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Test that we can add GenericRecords to map
 */
@UtilityClass
@Slf4j
class AddGenericRecordToMapTest {
    private final String MAP_NAME = "mostlyReadMap";
    private final int MAP_SIZE = 100;
    private final String INT_PROPERTY_NAME = "myint";

    public static void main(String[] args) {
        log.info("Starting AddGenericRecordToMapTest");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        testGenericRecordMap(hazelcastServer);

        // Shutdown server
        hazelcastServer.shutdown();
        log.info("Test completed");
    }

    private HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }


    // Test that we can add GenericRecords to map
    private void testGenericRecordMap(HazelcastInstance hazelcastInstance) {
        IMap<Integer, GenericRecord> map = hazelcastInstance.getMap(MAP_NAME);
        map.addLocalEntryListener(new EntryAdapter<Integer, GenericRecord>() {

            @Override
            public void entryAdded(EntryEvent<Integer, GenericRecord> event) {
                super.entryAdded(event);
                log.info("LocalEntryListener entryAdded {}", event.getValue().getInt32(INT_PROPERTY_NAME));
            }

        });

        for (int index = 0; index < MAP_SIZE; index++) {
            GenericRecord genericRecord = GenericRecordBuilder.compact("mytype")
                    .setInt32(INT_PROPERTY_NAME, index)
                    .build();
            map.put(index, genericRecord);
        }

    }
}
