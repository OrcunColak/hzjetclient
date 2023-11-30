package com.colak.serilization.compact.zeroconfiguration;

import java.io.Serializable;

//Hazelcast tries to extract a schema out of the class.
// If successful, it registers the zero-config serializer associated with the extracted schema and uses it
// while serializing and deserializing instances of that class.
public class MyWorker implements Serializable {

    String name;

    public MyWorker(String name) {
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "MyWorker{" +
               "name='" + name + '\'' +
               '}';
    }
}
