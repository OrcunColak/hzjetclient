package com.colak.serilization.compact.serializerconfiguration;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public class MyWorkerSerializer implements CompactSerializer<MyWorker> {
    @Override
    public MyWorker read(CompactReader compactReader) {
        String name = compactReader.readString("name");
        return new MyWorker(name);
    }

    @Override
    public void write(CompactWriter compactWriter, MyWorker myWorker) {
        compactWriter.writeString("name", myWorker.getName());
    }

    @Override
    public String getTypeName() {
        return "myworker";
    }

    @Override
    public Class<MyWorker> getCompactClass() {
        return MyWorker.class;
    }
}
