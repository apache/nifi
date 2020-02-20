package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.util.LinkedList;

public class Event {

    private LinkedList<String> data;
    private Long lastLSN;
    private boolean isSimpleEvent;
    private boolean hasBeginCommit;
    private boolean isSnapshot;

    public Event(LinkedList<String> data, Long lsn, boolean isSimple, boolean hasBeginCommit, boolean isSnap) {
        this.data = data;
        this.lastLSN = lsn;
        this.isSimpleEvent = isSimple;
        this.hasBeginCommit =  hasBeginCommit;
        this.isSimpleEvent = isSnap;
    }

    public LinkedList<String> getData() {
        return data;
    }

    public Long getLastLSN() {
        return lastLSN;
    }

    public boolean isSimpleEvent() {
        return isSimpleEvent;
    }

    public boolean hasBeginCommit() {
        return hasBeginCommit;
    }

    public boolean isSnapshot() {
        return isSnapshot;
    }
}
