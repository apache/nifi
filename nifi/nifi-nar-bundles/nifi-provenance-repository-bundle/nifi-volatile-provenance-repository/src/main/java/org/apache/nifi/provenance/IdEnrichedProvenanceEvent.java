package org.apache.nifi.provenance;

final class IdEnrichedProvenanceEvent extends FilterProvenanceEventRecord {

    private long eventId;

    public IdEnrichedProvenanceEvent(final ProvenanceEventRecord record) {
        super(record);
    }

    @Override
    public long getEventId() {
        return eventId;
    }

    public void setEventId(final long eventId) {
        this.eventId = eventId;
    }

    @Override
    public String toString() {
        return "IdEnrichedProvenanceEvent [eventId=" + this.eventId + ", record=" + super.record
                + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + (int) (eventId ^ (eventId >>> 32));
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IdEnrichedProvenanceEvent other = (IdEnrichedProvenanceEvent) obj;
        if (eventId != other.eventId) {
            return false;
        }
        return true;
    }

}
