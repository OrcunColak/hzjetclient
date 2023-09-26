package com.colak.datastructures.entrystore;

import java.io.Serializable;

class MyRecord implements Serializable {

    private String value;

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
