package com.colak.datastructures.mapentrystore.writebehind;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
class WriteBehindTest {

    private static final String MAP_NAME = "my-map";

    private static final MyMapEntryStore myMapEntryStore = new MyMapEntryStore();

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting HZ Client");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        // Do test
        startThreads(hazelcastClient);

        hazelcastClient.shutdown();
        hazelcastServer.shutdown();

        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();

        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(myMapEntryStore);
        mapStoreConfig.setWriteDelaySeconds(1);
        mapStoreConfig.setWriteCoalescing(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }


    private static void startThreads(HazelcastInstance hazelcastClient) {

        Thread monitorThread = new Thread(() -> {
            IMap<Integer, MyRecord> map = hazelcastClient.getMap(MAP_NAME);
            while (!Thread.currentThread().isInterrupted()) {
                MyRecord myRecord = map.get(0);
                if (myRecord.getValue().equals("10")) {
                    log.info("Found 10");
                }


            }

        });
        monitorThread.start();

        Thread writeThread = new Thread(() -> {
            IMap<Integer, MyRecord> map = hazelcastClient.getMap(MAP_NAME);
            for (int index = 0; index < 1; index++) {
                MyRecord value = new MyRecord("value " + index);
                log.info("Putting Key : {} , Value : {}", index, value);
                map.put(index, value);
            }

        });
        writeThread.start();

        try {
            writeThread.join();
            monitorThread.interrupt();

            monitorThread.join();

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }


        log.info("testEntryStore finished");
    }

    private static void sleepSeconds(int value) {
        try {
            TimeUnit.SECONDS.sleep(value);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

}
