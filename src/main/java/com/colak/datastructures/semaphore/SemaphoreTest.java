package com.colak.datastructures.semaphore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.ISemaphore;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SemaphoreTest {


    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        HazelcastInstance hazelcastInstanceClient = getHazelcastInstanceByConfig();
        testSemaphore(hazelcastInstanceClient);
    }

    public static HazelcastInstance getHazelcastInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void testSemaphore(HazelcastInstance hazelcastInstanceClient) throws InterruptedException {
        CPSubsystem cpSubsystem = hazelcastInstanceClient.getCPSubsystem();
        ISemaphore mySemaphore = cpSubsystem.getSemaphore("mySemaphore");
        mySemaphore.init(5);

        mySemaphore.acquire();
        log.info("acquired semaphore");
    }
}
