package com.colak.datastructures.mapentrystore;

import java.io.Serializable;

class MyRecord implements Serializable {

    private final String value;

    MyRecord(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyRecord{" +
               "value='" + value + '\'' +
               '}';
    }
}
