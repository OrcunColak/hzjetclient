package com.colak.datastructures.map.clientusercodedeployment;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Test that map entry can be replaced when ClientUserCodeDeploymentConfig is used
 */
@Slf4j
class UserCodeDeploymentTest {

    private static final String MAP_NAME = "mymap";

    public static void main(String[] args) {
        log.info("Starting HZ Client");
        HazelcastInstance hazelcastInstanceClient = getHazelcastInstanceByClientUserCodeDeploymentConfig();

        testReplace(hazelcastInstanceClient);
    }

    private static HazelcastInstance getHazelcastInstanceByClientUserCodeDeploymentConfig() {
// The server must start with  UserCodeDeploymentConfig
//        Config config = new Config();
//        UserCodeDeploymentConfig userCodeDeploymentConfig = config.getUserCodeDeploymentConfig();
//        userCodeDeploymentConfig.setEnabled(true);
//
//
//        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig userCodeDeploymentConfig = clientConfig.getUserCodeDeploymentConfig();
        userCodeDeploymentConfig.setEnabled(true);
        userCodeDeploymentConfig.addClass(Worker.class);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    // Test that map entry can be replaced when ClientUserCodeDeploymentConfig is used
    private static void testReplace(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Worker> map = hazelcastInstance.getMap(MAP_NAME);
        Worker worker1 = new Worker("john");
        Worker worker2 = new Worker("jane");

        int key = 1;
        map.put(key, worker1);
        Worker receivedWorker = map.get(key);

        if (!receivedWorker.equals(worker1)) {
            throw new IllegalStateException("receivedWorker is not equal to worker1");
        }

        map.replace(key, worker1, worker2);
        receivedWorker = map.get(key);
        if (!receivedWorker.equals(worker2)) {
            throw new IllegalStateException("receivedWorker is not equal to worker2");
        }
    }
}
