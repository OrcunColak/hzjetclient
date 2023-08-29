package com.colak;

import com.colak.jet.show_resources.ShowResources;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Starting HZ Client");
        HazelcastInstance hazelcastInstanceClient = HazelcastClientFactory.getHazelcastInstanceByConfig();
        ShowResources.submitForPostgres(hazelcastInstanceClient);
        ShowResources.submitForMySql(hazelcastInstanceClient);

    }




}