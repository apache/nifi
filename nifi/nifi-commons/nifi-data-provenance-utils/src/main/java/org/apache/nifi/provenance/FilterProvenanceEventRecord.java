package org.apache.nifi.provenance;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class FilterProvenanceEventRecord implements ProvenanceEventRecord {

    final protected ProvenanceEventRecord record;

    public FilterProvenanceEventRecord(ProvenanceEventRecord record) {
        this.record = record;
    }

    @Override
    public long getEventId() {
        return record.getEventId();
    }

    @Override
    public long getEventTime() {
        return record.getEventTime();
    }

    @Override
    public long getFlowFileEntryDate() {
        return record.getFlowFileEntryDate();
    }

    @Override
    public long getLineageStartDate() {
        return record.getLineageStartDate();
    }

    @Override
    public Set<String> getLineageIdentifiers() {
        return record.getLineageIdentifiers();
    }

    @Override
    public long getFileSize() {
        return record.getFileSize();
    }

    @Override
    public Long getPreviousFileSize() {
        return record.getPreviousFileSize();
    }

    @Override
    public long getEventDuration() {
        return record.getEventDuration();
    }

    @Override
    public ProvenanceEventType getEventType() {
        return record.getEventType();
    }

    @Override
    public Map<String, String> getAttributes() {
        return record.getAttributes();
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        return record.getPreviousAttributes();
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        return record.getUpdatedAttributes();
    }

    @Override
    public String getComponentId() {
        return record.getComponentId();
    }

    @Override
    public String getComponentType() {
        return record.getComponentType();
    }

    @Override
    public String getTransitUri() {
        return record.getTransitUri();
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        return record.getSourceSystemFlowFileIdentifier();
    }

    @Override
    public String getFlowFileUuid() {
        return record.getFlowFileUuid();
    }

    @Override
    public String getAlternateIdentifierUri() {
        return record.getAlternateIdentifierUri();
    }

    @Override
    public String getDetails() {
        return record.getDetails();
    }

    @Override
    public String getRelationship() {
        return record.getRelationship();
    }

    @Override
    public String getSourceQueueIdentifier() {
        return record.getSourceQueueIdentifier();
    }

    @Override
    public String getContentClaimSection() {
        return record.getContentClaimSection();
    }

    @Override
    public String getPreviousContentClaimSection() {
        return record.getPreviousContentClaimSection();
    }

    @Override
    public String getContentClaimContainer() {
        return record.getContentClaimContainer();
    }

    @Override
    public String getPreviousContentClaimContainer() {
        return record.getPreviousContentClaimContainer();
    }

    @Override
    public String getContentClaimIdentifier() {
        return record.getContentClaimIdentifier();
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        return record.getPreviousContentClaimIdentifier();
    }

    @Override
    public Long getContentClaimOffset() {
        return record.getContentClaimOffset();
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        return record.getPreviousContentClaimOffset();
    }

    @Override
    public List<String> getParentUuids() {
        return record.getParentUuids();
    }

    @Override
    public List<String> getChildUuids() {
        return record.getChildUuids();
    }
}
