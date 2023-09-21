package com.colak.datastructures.map.clientusercodedeployment;

import java.io.Serializable;
import java.util.Objects;

public class Worker implements Serializable {

    String name;

    public Worker(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Worker worker = (Worker) o;
        return Objects.equals(name, worker.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
