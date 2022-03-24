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
package org.apache.nifi.atlas.reporting;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleProvenanceRecord implements ProvenanceEventRecord {
    private long eventId;
    private String componentId;
    private String componentType;
    private String transitUri;
    private String flowFileUUID;
    private ProvenanceEventType eventType;
    private Map<String, String> attributes = new HashMap<>();

    public static SimpleProvenanceRecord pr(String componentId, String componentType, ProvenanceEventType eventType) {
        return pr(componentId, componentType, eventType, null, null);
    }
    public static SimpleProvenanceRecord pr(String componentId, String componentType, ProvenanceEventType eventType, String transitUri) {
        return pr(componentId, componentType, eventType, transitUri, null);
    }
    public static SimpleProvenanceRecord pr(String componentId, String componentType, ProvenanceEventType eventType, String transitUri, String flowFileUUID) {
        final SimpleProvenanceRecord pr = new SimpleProvenanceRecord();
        pr.componentId = componentId.length() == 18 ? componentId + "-0000-000000000000" : componentId;
        pr.componentType = componentType;
        pr.transitUri = transitUri;
        pr.eventType = eventType;
        pr.flowFileUUID = flowFileUUID;
        return pr;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    @Override
    public String getComponentType() {
        return componentType;
    }

    @Override
    public String getTransitUri() {
        return transitUri;
    }

    @Override
    public ProvenanceEventType getEventType() {
        return eventType;
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public long getEventId() {
        return eventId;
    }

    @Override
    public long getEventTime() {
        return 0;
    }

    @Override
    public long getFlowFileEntryDate() {
        return 0;
    }

    @Override
    public long getLineageStartDate() {
        return 0;
    }

    @Override
    public long getFileSize() {
        return 0;
    }

    @Override
    public Long getPreviousFileSize() {
        return null;
    }

    @Override
    public long getEventDuration() {
        return 0;
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        return null;
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        return null;
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        return null;
    }

    @Override
    public String getFlowFileUuid() {
        return null;
    }

    @Override
    public List<String> getParentUuids() {
        return null;
    }

    @Override
    public List<String> getChildUuids() {
        return null;
    }

    @Override
    public String getAlternateIdentifierUri() {
        return null;
    }

    @Override
    public String getDetails() {
        return null;
    }

    @Override
    public String getRelationship() {
        return null;
    }

    @Override
    public String getSourceQueueIdentifier() {
        return null;
    }

    @Override
    public String getContentClaimSection() {
        return null;
    }

    @Override
    public String getPreviousContentClaimSection() {
        return null;
    }

    @Override
    public String getContentClaimContainer() {
        return null;
    }

    @Override
    public String getPreviousContentClaimContainer() {
        return null;
    }

    @Override
    public String getContentClaimIdentifier() {
        return null;
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        return null;
    }

    @Override
    public Long getContentClaimOffset() {
        return null;
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        return null;
    }

    @Override
    public String getBestEventIdentifier() {
        return null;
    }
}
