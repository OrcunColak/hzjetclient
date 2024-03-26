package com.colak.jet.submitjob.join_imap;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class WorkerTableEntry {
    private int id;
    private String name;
    private String ssn;

    public WorkerTableEntry(int id, String name, String ssn) {
        this.id = id;
        this.name = name;
        this.ssn = ssn;
    }

}
