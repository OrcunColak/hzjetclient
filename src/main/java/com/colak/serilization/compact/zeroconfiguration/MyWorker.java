package com.colak.serilization.compact.zeroconfiguration;

//Hazelcast tries to extract a schema out of the class.
// If successful, it registers the zero-config serializer associated with the extracted schema and uses it
// while serializing and deserializing instances of that class.
// MyWorker has to have public access modifier, in order for UCD  + EntryProcessor to work
public class MyWorker {

    private String name;

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
