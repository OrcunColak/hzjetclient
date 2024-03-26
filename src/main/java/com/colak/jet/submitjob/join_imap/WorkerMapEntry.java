package com.colak.jet.submitjob.join_imap;

import lombok.Data;

@Data
public class WorkerMapEntry {
    private int id;
    private String surname;

    public WorkerMapEntry(int id, String surname) {
        this.id = id;
        this.surname = surname;
    }

}
