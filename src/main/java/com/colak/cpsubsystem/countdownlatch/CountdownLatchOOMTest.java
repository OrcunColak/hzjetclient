package com.colak.cpsubsystem.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.ICountDownLatch;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

// See https://stackoverflow.com/questions/78423020/hazelcast-5-1-7-countdownlatch-produces-outofmemory
@Slf4j
class CountdownLatchOOMTest {

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastServerInstanceByConfig();

        countdownLatch(hazelcastServer);

        hazelcastClient.shutdown();
        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setSessionTimeToLiveSeconds(30);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void countdownLatch(HazelcastInstance hazelcastServer) {

        AtomicInteger counter = new AtomicInteger();
        CPSubsystem cpSubsystem = hazelcastServer.getCPSubsystem();

        while (true) {
            String id = String.valueOf(counter.getAndIncrement());

            log.info("Creating : {}", id);
            ICountDownLatch countDownLatch = cpSubsystem.getCountDownLatch(id);

            countDownLatch.trySetCount(1);

            countDownLatch.countDown();

            countDownLatch.destroy();
        }
    }
}
