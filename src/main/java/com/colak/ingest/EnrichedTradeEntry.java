package com.colak.ingest;

import com.colak.genericmapstore.WorkerTableEntry;

public class EnrichedTradeEntry {
    private TradeEntry tradeEntry;
    private WorkerTableEntry workerTableEntry;

    public EnrichedTradeEntry(TradeEntry tradeEntry, WorkerTableEntry workerTableEntry) {
        this.tradeEntry = tradeEntry;
        this.workerTableEntry = workerTableEntry;
    }

    public Integer getId() {
        return tradeEntry.getId();
    }

    public TradeEntry getTradeEntry() {
        return tradeEntry;
    }

    public void setTradeEntry(TradeEntry tradeEntry) {
        this.tradeEntry = tradeEntry;
    }

    public WorkerTableEntry getWorkerTableEntry() {
        return workerTableEntry;
    }

    public void setWorkerTableEntry(WorkerTableEntry workerTableEntry) {
        this.workerTableEntry = workerTableEntry;
    }
}
