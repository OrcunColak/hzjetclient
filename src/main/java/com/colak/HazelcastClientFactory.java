package com.colak;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
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

}
