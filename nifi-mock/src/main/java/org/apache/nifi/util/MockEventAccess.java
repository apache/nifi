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
package org.apache.nifi.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.action.Action;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.reporting.EventAccess;

public class MockEventAccess implements EventAccess {

    private ProcessGroupStatus processGroupStatus;
    private final List<ProvenanceEventRecord> provenanceRecords = new ArrayList<>();
    private final List<Action> flowChanges = new ArrayList<>();

    public void setProcessGroupStatus(final ProcessGroupStatus status) {
        this.processGroupStatus = status;
    }

    @Override
    public ProcessGroupStatus getControllerStatus() {
        return processGroupStatus;
    }

    @Override
    public List<ProvenanceEventRecord> getProvenanceEvents(long firstEventId, int maxRecords) throws IOException {
        if (firstEventId < 0 || maxRecords < 1) {
            throw new IllegalArgumentException();
        }

        final List<ProvenanceEventRecord> records = new ArrayList<>();

        for (final ProvenanceEventRecord record : provenanceRecords) {
            if (record.getEventId() >= firstEventId) {
                records.add(record);
                if (records.size() >= maxRecords) {
                    return records;
                }
            }
        }

        return records;
    }

    public void addProvenanceEvent(final ProvenanceEventRecord record) {
        this.provenanceRecords.add(record);
    }

    public ProvenanceEventRepository getProvenanceRepository() {
        return null;
    }

    @Override
    public List<Action> getFlowChanges(int firstActionId, int maxActions) {
        if (firstActionId < 0 || maxActions < 1) {
            throw new IllegalArgumentException();
        }

        final List<Action> actions = new ArrayList<>();

        for (final Action action : flowChanges) {
            if (action.getId() >= firstActionId) {
                actions.add(action);
                if (actions.size() >= maxActions) {
                    return actions;
                }
            }
        }

        return actions;
    }

    public void addFlowChange(final Action action) {
        this.flowChanges.add(action);
    }

}
