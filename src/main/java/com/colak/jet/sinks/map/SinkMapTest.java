package com.colak.jet.sinks.map;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
class SinkMapTest {

    private static final String LIST_NAME = "myList";
    private static final String MAP_NAME = "myMap";

    @Data
    @AllArgsConstructor
    static class Order implements Serializable {
        int id;
        String name;
    }

    public static void main(String[] args) {
        log.info("Starting HZ Server");

        // Start server
        HazelcastInstance hazelcastServer = getHazelcastServerInstanceByConfig();

        populateList(hazelcastServer);
        transferFromListToMap(hazelcastServer);

        printMap(hazelcastServer);
        hazelcastServer.shutdown();

        log.info("Test completed");

    }


    private static HazelcastInstance getHazelcastServerInstanceByConfig() {
        Config config = new Config();

        // Add JetConfig
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(true);
        jetConfig.setResourceUploadEnabled(true);

        return Hazelcast.newHazelcastInstance(config);
    }

    private static void populateList(HazelcastInstance hazelcastInstance) {
        IList<Order> list = hazelcastInstance.getList(LIST_NAME);
        List<Order> orders = List.of(
                new Order(1, "1"),
                new Order(2, "2"),
                new Order(3, "3"),
                new Order(1, "4")
        );
        list.addAll(orders);
    }

    private static void transferFromListToMap(HazelcastInstance hazelcastInstance) {
        Pipeline pipeline = Pipeline.create();

        BatchSource<Order> source = Sources.list(LIST_NAME);
        Sink<Order> sink = Sinks.<Order, Integer, List<Order>>mapBuilder(MAP_NAME)
                .toKeyFn(Order::getId)
                .toValueFn(List::of)
                .mergeFn((a, b) -> {
                    ArrayList<Order> orders = new ArrayList<>(a);
                    orders.addAll(b);
                    return orders;
                })
                .build();

        pipeline.readFrom(source).writeTo(sink);

        Job job = hazelcastInstance.getJet().newJob(pipeline);
        job.join();
    }

    private static void printMap(HazelcastInstance hazelcastServer) {
        IMap<Integer, List<Order>> map = hazelcastServer.getMap(MAP_NAME);
        for (Map.Entry<Integer, List<Order>> entry : map) {
            log.info("Key : {}  Value : {}", entry.getKey(), entry.getValue());
        }
    }
}
