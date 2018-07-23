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
package org.apache.nifi.atlas.provenance.lineage;

import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.ArrayList;
import java.util.List;

public class LineagePath {
    private List<ProvenanceEventRecord> events = new ArrayList<>();
    private List<LineagePath> parents = new ArrayList<>();
    private DataSetRefs refs;
    private long lineagePathHash;

    /**
     * NOTE: The list contains provenance events in reversed order, i.e. the last one first.
     */
    public List<ProvenanceEventRecord> getEvents() {
        return events;
    }

    public List<LineagePath> getParents() {
        return parents;
    }

    public DataSetRefs getRefs() {
        return refs;
    }

    public void setRefs(DataSetRefs refs) {
        this.refs = refs;
    }

    public boolean shouldCreateSeparatePath(ProvenanceEventType eventType) {
        switch (eventType) {
            case CLONE:
            case JOIN:
            case FORK:
            case REPLAY:
                return true;
        }
        return false;
    }

    public boolean isComplete() {
        // If the FlowFile is DROPed right after create child FlowFile, then the path is not worth for reporting.
        final boolean isDroppedImmediately = events.size() == 2
                && events.get(0).getEventType().equals(ProvenanceEventType.DROP)
                && shouldCreateSeparatePath(events.get(1).getEventType());
        return !isDroppedImmediately && hasInput() && hasOutput();
    }

    public boolean hasInput() {
        return (refs != null && !refs.getInputs().isEmpty()) || parents.stream().anyMatch(parent -> parent.hasInput());
    }

    public boolean hasOutput() {
        return (refs != null && !refs.getOutputs().isEmpty()) || parents.stream().anyMatch(parent -> parent.hasOutput());
    }

    public long getLineagePathHash() {
        return lineagePathHash;
    }

    public void setLineagePathHash(long lineagePathHash) {
        this.lineagePathHash = lineagePathHash;
    }
}
