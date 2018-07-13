package org.apache.nifi.processor.util.list;

public class ListedEntity {
    /**
     * Milliseconds.
     */
    private long timestamp;
    /**
     * Bytes.
     */
    private long size;

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getSize() {
        return size;
    }
}
