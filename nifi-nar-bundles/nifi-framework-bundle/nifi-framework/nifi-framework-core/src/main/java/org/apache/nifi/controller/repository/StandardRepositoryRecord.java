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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.processor.Relationship;

public class StandardRepositoryRecord implements RepositoryRecord {

    private RepositoryRecordType type = null;
    private FlowFileRecord workingFlowFileRecord = null;
    private Relationship transferRelationship = null;
    private FlowFileQueue destination = null;
    private final FlowFileRecord originalFlowFileRecord;
    private final FlowFileQueue originalQueue;
    private String swapLocation;
    private final Map<String, String> updatedAttributes = new HashMap<>();
    private final Map<String, String> originalAttributes;
    private List<ContentClaim> transientClaims;

    /**
     * Creates a new record which has no original claim or flow file - it is entirely new
     *
     * @param originalQueue queue
     */
    public StandardRepositoryRecord(final FlowFileQueue originalQueue) {
        this(originalQueue, null);
        this.type = RepositoryRecordType.CREATE;
    }

    /**
     * Creates a record based on given original items
     *
     * @param originalQueue queue
     * @param originalFlowFileRecord record
     */
    public StandardRepositoryRecord(final FlowFileQueue originalQueue, final FlowFileRecord originalFlowFileRecord) {
        this(originalQueue, originalFlowFileRecord, null);
        this.type = RepositoryRecordType.UPDATE;
    }

    public StandardRepositoryRecord(final FlowFileQueue originalQueue, final FlowFileRecord originalFlowFileRecord, final String swapLocation) {
        this.originalQueue = originalQueue;
        this.originalFlowFileRecord = originalFlowFileRecord;
        this.type = RepositoryRecordType.SWAP_OUT;
        this.swapLocation = swapLocation;
        this.originalAttributes = originalFlowFileRecord == null ? Collections.<String, String>emptyMap() : originalFlowFileRecord.getAttributes();
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
            type = RepositoryRecordType.SWAP_IN; // we are swapping in a new record
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

    public void setWorking(final FlowFileRecord flowFile, final String attributeKey, final String attributeValue) {
        workingFlowFileRecord = flowFile;

        // If setting attribute to same value as original, don't add to updated attributes
        final String currentValue = originalAttributes.get(attributeKey);
        if (currentValue == null || !currentValue.equals(attributeValue)) {
            updatedAttributes.put(attributeKey, attributeValue);
        }
    }

    public void setWorking(final FlowFileRecord flowFile, final Map<String, String> updatedAttribs) {
        workingFlowFileRecord = flowFile;

        for (final Map.Entry<String, String> entry : updatedAttribs.entrySet()) {
            final String currentValue = originalAttributes.get(entry.getKey());
            if (currentValue == null || !currentValue.equals(entry.getValue())) {
                updatedAttributes.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public boolean isAttributesChanged() {
        return !updatedAttributes.isEmpty();
    }

    public void markForAbort() {
        type = RepositoryRecordType.CONTENTMISSING;
    }

    @Override
    public boolean isMarkedForAbort() {
        return RepositoryRecordType.CONTENTMISSING.equals(type);
    }

    public void markForDelete() {
        type = RepositoryRecordType.DELETE;
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
        return updatedAttributes;
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
}
