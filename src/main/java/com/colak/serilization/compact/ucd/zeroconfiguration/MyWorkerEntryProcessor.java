package com.colak.serilization.compact.ucd.zeroconfiguration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.EntryProcessor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
// MyWorkerEntryProcessor does not need to have public access modifier
class MyWorkerEntryProcessor implements EntryProcessor<Integer, MyWorker, MyWorker>, HazelcastInstanceAware {

    private transient HazelcastInstance hazelcastInstance;

    @Override
    public MyWorker process(Map.Entry<Integer, MyWorker> entry) {
        MyWorker newWorker = new MyWorker("new-worker1");
        entry.setValue(newWorker);
        return null;
    }

    /**
     * See https://stackoverflow.com/questions/77841836/hazelcast-serialization-issue-with-multiple-nodes
     * This is called during de-serializaiton
     *
     * @param hazelcastInstance
     */
    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        // This will be called 271 times for each partition
        log.info("setHazelcastInstance is called");
        this.hazelcastInstance = hazelcastInstance;
    }
}
