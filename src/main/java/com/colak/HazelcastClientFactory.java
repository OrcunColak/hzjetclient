package com.colak;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import lombok.experimental.UtilityClass;

@UtilityClass
public class HazelcastClientFactory {

    public static HazelcastInstance getHazelcastInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public static HazelcastInstance getHazelcastInstanceByXml() {
        return HazelcastClient.newHazelcastClient();
    }

    public static HazelcastInstance getHazelcastInstanceByClientUserCodeDeploymentConfig() {
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
        userCodeDeploymentConfig.addClass(com.colak.datastructures.map.Worker.class);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }
}
