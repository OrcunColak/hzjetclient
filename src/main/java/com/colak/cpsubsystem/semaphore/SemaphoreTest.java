package com.colak.cpsubsystem.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.ISemaphore;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SemaphoreTest {


    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        testSemaphore(hazelcastServer);

        hazelcastServer.shutdown();
        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void testSemaphore(HazelcastInstance hazelcastServer) throws InterruptedException {
        CPSubsystem cpSubsystem = hazelcastServer.getCPSubsystem();
        ISemaphore mySemaphore = cpSubsystem.getSemaphore("mySemaphore");
        mySemaphore.init(5);

        mySemaphore.acquire();
        log.info("acquired semaphore");
    }
}
