/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandardRepositoryRecord implements RepositoryRecord {

    private RepositoryRecordType type;
    private FlowFileRecord workingFlowFileRecord = null;
    private Relationship transferRelationship = null;
    private FlowFileQueue destination = null;
    private final FlowFileRecord originalFlowFileRecord;
    private final FlowFileQueue originalQueue;
    private String swapLocation;
    private final Map<String, String> originalAttributes;
    private Map<String, String> updatedAttributes = null;
    private List<ContentClaim> transientClaims;
    private final long startNanos = System.nanoTime();


    /**
     * Creates a new record which has no original claim or flow file - it is entirely new
     *
     * @param originalQueue queue
     */
    public StandardRepositoryRecord(final FlowFileQueue originalQueue) {
        this(originalQueue, null);
        setType(RepositoryRecordType.CREATE);
    }

    /**
     * Creates a record based on given original items
     *
     * @param originalQueue queue
     * @param originalFlowFileRecord record
     */
    public StandardRepositoryRecord(final FlowFileQueue originalQueue, final FlowFileRecord originalFlowFileRecord) {
        this(originalQueue, originalFlowFileRecord, null);
        setType(RepositoryRecordType.UPDATE);
    }

    public StandardRepositoryRecord(final FlowFileQueue originalQueue, final FlowFileRecord originalFlowFileRecord, final String swapLocation) {
        this.originalQueue = originalQueue;
        this.originalFlowFileRecord = originalFlowFileRecord;
        setType(RepositoryRecordType.SWAP_OUT);
        this.swapLocation = swapLocation;
        this.originalAttributes = originalFlowFileRecord == null ? Collections.emptyMap() : originalFlowFileRecord.getAttributes();
    }

    @Override
    public FlowFileQueue getDestination() {
        return destination;
    }

    public void setDestination(final FlowFileQueue destination) {
        this.destination = destination;
    }

    @Override
    public RepositoryRecordType getType() {
        return type;
    }

    FlowFileRecord getOriginal() {
        return originalFlowFileRecord;
    }

    @Override
    public String getSwapLocation() {
        return swapLocation;
    }

    public void setSwapLocation(final String swapLocation) {
        this.swapLocation = swapLocation;
        if (type != RepositoryRecordType.SWAP_OUT) {
            setType(RepositoryRecordType.SWAP_IN); // we are swapping in a new record
        }
    }

    @Override
    public ContentClaim getOriginalClaim() {
        return (originalFlowFileRecord == null) ? null : originalFlowFileRecord.getContentClaim();
    }

    @Override
    public FlowFileQueue getOriginalQueue() {
        return originalQueue;
    }

    public void setWorking(final FlowFileRecord flowFile) {
        workingFlowFileRecord = flowFile;
    }

    private Map<String, String> initializeUpdatedAttributes() {
        if (updatedAttributes == null) {
            updatedAttributes = new HashMap<>();
        }

        return updatedAttributes;
    }

    public void setWorking(final FlowFileRecord flowFile, final String attributeKey, final String attributeValue) {
        workingFlowFileRecord = flowFile;

        // In the case that the type is CREATE, we know that all attributes are updated attributes, so no need to store them.
        if (type == RepositoryRecordType.CREATE) {
            return;
        }

        // If setting attribute to same value as original, don't add to updated attributes
        final String currentValue = originalAttributes.get(attributeKey);
        if (currentValue == null || !currentValue.equals(attributeValue)) {
            initializeUpdatedAttributes().put(attributeKey, attributeValue);
        }
    }

    public void setWorking(final FlowFileRecord flowFile, final Map<String, String> updatedAttribs) {
        workingFlowFileRecord = flowFile;

        // In the case that the type is CREATE, we know that all attributes are updated attributes, so no need to store them.
        if (type == RepositoryRecordType.CREATE) {
            return;
        }

        for (final Map.Entry<String, String> entry : updatedAttribs.entrySet()) {
            final String currentValue = originalAttributes.get(entry.getKey());
            if (currentValue == null || !currentValue.equals(entry.getValue())) {
                initializeUpdatedAttributes().put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public boolean isAttributesChanged() {
        return type == RepositoryRecordType.CREATE || (updatedAttributes != null && !updatedAttributes.isEmpty());
    }

    public void markForAbort() {
        setType(RepositoryRecordType.CONTENTMISSING);
    }

    @Override
    public boolean isMarkedForAbort() {
        return RepositoryRecordType.CONTENTMISSING.equals(type);
    }

    public void markForDelete() {
        setType(RepositoryRecordType.DELETE);
    }

    public boolean isMarkedForDelete() {
        return RepositoryRecordType.DELETE.equals(type);
    }

    public void setTransferRelationship(final Relationship relationship) {
        transferRelationship = relationship;
    }

    public Relationship getTransferRelationship() {
        return transferRelationship;
    }

    FlowFileRecord getWorking() {
        return workingFlowFileRecord;
    }

    ContentClaim getWorkingClaim() {
        return (workingFlowFileRecord == null) ? null : workingFlowFileRecord.getContentClaim();
    }

    @Override
    public FlowFileRecord getCurrent() {
        return (workingFlowFileRecord == null) ? originalFlowFileRecord : workingFlowFileRecord;
    }

    @Override
    public ContentClaim getCurrentClaim() {
        return (getCurrent() == null) ? null : getCurrent().getContentClaim();
    }

    @Override
    public long getCurrentClaimOffset() {
        return (getCurrent() == null) ? 0L : getCurrent().getContentClaimOffset();
    }

    boolean isWorking() {
        return (workingFlowFileRecord != null);
    }

    Map<String, String> getOriginalAttributes() {
        return originalAttributes;
    }

    Map<String, String> getUpdatedAttributes() {
        if (type == RepositoryRecordType.CREATE) {
            return getCurrent().getAttributes();
        }

        return updatedAttributes == null ? Collections.emptyMap() : updatedAttributes;
    }

    private void setType(final RepositoryRecordType newType) {
        if (newType == this.type) {
            return;
        }

        if (this.type == RepositoryRecordType.CREATE) {
            // Because we don't copy updated attributes to `this.updatedAttributes` for CREATE records, we need to ensure
            // that if a record is changed from CREATE to anything else that we do properly update the `this.updatedAttributes` field.
            this.updatedAttributes = new HashMap<>(getCurrent().getAttributes());
        }

        this.type = newType;
    }

    @Override
    public String toString() {
        return "StandardRepositoryRecord[UpdateType=" + getType() + ",Record=" + getCurrent() + "]";
    }

    @Override
    public List<ContentClaim> getTransientClaims() {
        return transientClaims == null ? Collections.<ContentClaim> emptyList() : Collections.unmodifiableList(transientClaims);
    }

    void addTransientClaim(final ContentClaim claim) {
        if (claim == null) {
            return;
        }

        if (transientClaims == null) {
            transientClaims = new ArrayList<>();
        }
        transientClaims.add(claim);
    }

    public long getStartNanos() {
        return startNanos;
    }
}
