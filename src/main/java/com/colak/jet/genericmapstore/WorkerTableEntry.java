package com.colak.jet.genericmapstore;

public class WorkerTableEntry {
    private int id;
    private String name;

    private String ssn;

    public WorkerTableEntry(int id, String name, String ssn) {
        this.id = id;
        this.name = name;
        this.ssn = ssn;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSsn() {
        return ssn;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }
}
