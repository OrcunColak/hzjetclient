package com.colak.cpsubsystem.fencedlock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import lombok.extern.slf4j.Slf4j;

/**
 * See <a href="https://stackoverflow.com/questions/78112610/how-should-hazelcast-dynamically-allocated-fencedlocks-be-destroyed">...</a>
 */
@Slf4j
class FencedLockTest {

    public static void main(String[] args) throws Exception {
        log.info("Starting HZ Client");

        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        // Start client
        HazelcastInstance hazelcastClient = getHazelcastClientInstanceByConfig();

        testFencedLock(hazelcastClient);

        hazelcastClient.shutdown();
        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setSessionTimeToLiveSeconds(30);
        return Hazelcast.newHazelcastInstance(config);
    }

    // Start client
    private static HazelcastInstance getHazelcastClientInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static void testFencedLock(HazelcastInstance hazelcastServer) {
        CPSubsystem cpSubsystem = hazelcastServer.getCPSubsystem();
        FencedLock fencedLock = cpSubsystem.getLock("myFencedLock");
        fencedLock.lock();
        try {
            // update the client
            log.info("locked FencedLock");
        } finally {
            fencedLock.unlock();
            log.info("unlocked FencedLock");
        }
    }
}
