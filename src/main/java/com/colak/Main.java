package com.colak;

import com.colak.datastructures.nearcache.NearCacheTTL;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Starting HZ Client");
        HazelcastInstance hazelcastInstanceClient = getHazelcastInstanceByXml();
        NearCacheTTL.testTTL(hazelcastInstanceClient);
    }


    private static HazelcastInstance getHazelcastInstanceByConfig() {
        ClientConfig clientConfig = new ClientConfig();
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static HazelcastInstance getHazelcastInstanceByXml() {
        return HazelcastClient.newHazelcastClient();
    }

}