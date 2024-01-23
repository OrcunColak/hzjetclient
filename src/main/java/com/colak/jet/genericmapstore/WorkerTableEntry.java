package com.colak.jet.genericmapstore;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WorkerTableEntry {
    private int id;
    private String name;

    private String ssn;

    public WorkerTableEntry(int id, String name, String ssn) {
        this.id = id;
        this.name = name;
        this.ssn = ssn;
    }

}
