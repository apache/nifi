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
package org.apache.nifi.provenance.lineage;

import java.util.List;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

public class EventNode implements ProvenanceEventLineageNode {

    private final ProvenanceEventRecord record;
    private String clusterNodeIdentifier = null;

    public EventNode(final ProvenanceEventRecord event) {
        this.record = event;
    }

    @Override
    public String getIdentifier() {
        return String.valueOf(getEventIdentifier());
    }

    @Override
    public LineageNodeType getNodeType() {
        return LineageNodeType.PROVENANCE_EVENT_NODE;
    }

    @Override
    public ProvenanceEventType getEventType() {
        return record.getEventType();
    }

    @Override
    public long getTimestamp() {
        return record.getEventTime();
    }

    @Override
    public long getEventIdentifier() {
        return record.getEventId();
    }

    @Override
    public String getFlowFileUuid() {
        return record.getAttributes().get(CoreAttributes.UUID.key());
    }

    @Override
    public List<String> getParentUuids() {
        return record.getParentUuids();
    }

    @Override
    public List<String> getChildUuids() {
        return record.getChildUuids();
    }

    @Override
    public int hashCode() {
        return 2938472 + record.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof EventNode)) {
            return false;
        }

        final EventNode other = (EventNode) obj;
        return record.equals(other.record);
    }

    @Override
    public String toString() {
        return "Event[ID=" + record.getEventId() + ", Type=" + record.getEventType() + ", UUID=" + record.getFlowFileUuid() + ", Component=" + record.getComponentId() + "]";
    }
}
