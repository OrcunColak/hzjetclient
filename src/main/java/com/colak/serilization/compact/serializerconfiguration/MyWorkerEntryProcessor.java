package com.colak.serilization.compact.serializerconfiguration;

import com.hazelcast.map.EntryProcessor;

import java.util.Map;

// MyWorkerEntryProcessor does not need to have public access modifier
class MyWorkerEntryProcessor implements EntryProcessor<Integer, MyWorker, MyWorker> {
    @Override
    public MyWorker process(Map.Entry<Integer, MyWorker> entry) {
        MyWorker newWorker = new MyWorker("new-worker1");
        entry.setValue(newWorker);
        return null;
    }
}
