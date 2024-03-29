package com.colak.datastructures.map.indexconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;

/**
 * Test that we can use async methods of IMap
 */
@Slf4j
class MapIndexConfigTest {
    private static final String MAP_NAME = "my-map";
    private static final int MAP_SIZE = 100;

    public static void main(String[] args) {
        log.info("Starting MapAsyncMethodsTest");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();
        testAsyncAdd(hazelcastServer);

        // Shutdown server
        hazelcastServer.shutdown();
        log.info("Test completed");
    }

    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "age"));

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void testAsyncAdd(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Employee> map = hazelcastInstance.getMap(MAP_NAME);

        for (int index = 0; index < MAP_SIZE; index++) {
            Employee employee = new Employee(index, index);
            map.put(index, employee);

        }
        queryYoungEmployees(hazelcastInstance);
    }

    private static void queryYoungEmployees(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Employee> map = hazelcastInstance.getMap(MAP_NAME);
        Predicate<Integer, Employee> predicate = Predicates.sql("age < 18");
        Collection<Employee> employees = map.values(predicate);
        log.info("YoungEmployees : {}", employees);
    }

    @ToString
    @RequiredArgsConstructor
    private static class Employee {
        final int id;
        final int age;
    }
}
