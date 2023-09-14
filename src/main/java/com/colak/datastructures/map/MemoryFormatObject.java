package com.colak.datastructures.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class MemoryFormatObject {

    public static void test(HazelcastInstance hazelcastInstance) {
        IMap<Integer, Worker> map = hazelcastInstance.getMap("mymap");
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
