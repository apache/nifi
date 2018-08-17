package org.apache.nifi.repository.schema;

public class SingleRecordIterator implements RecordIterator {
    private final Record record;
    private boolean consumed = false;

    public SingleRecordIterator(final Record record) {
        this.record = record;
    }

    @Override
    public Record next() {
        if (consumed) {
            return null;
        }

        consumed = true;
        return record;
    }

    @Override
    public void close() {
    }
}
