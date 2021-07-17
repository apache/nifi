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

package org.apache.nifi.reporting.sql.provenance;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ComponentMapHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ProvenanceEnumerator implements Enumerator<Object> {
    private static final int FETCH_SIZE = 1000;
    private final ComponentLog logger;
    private final int[] fields;
    private final ProvenanceEventRepository provenanceEventRepository;
    private List<ProvenanceEventRecord> provenanceEvents;
    private final ComponentMapHolder componentMapHolder;
    private final String nodeIdentifier;

    private Object currentRow;
    private long currentId = 0;
    private int currentIndex = 0; // Index into the current fetch

    public ProvenanceEnumerator(final ReportingContext context, final ComponentLog logger, final int[] fields) {
        this.logger = logger;
        this.fields = fields;
        final EventAccess eventAccess = context.getEventAccess();
        this.provenanceEventRepository = eventAccess.getProvenanceRepository();
        final ProcessGroupStatus procGroupStatus = eventAccess.getControllerStatus();
        this.componentMapHolder = ComponentMapHolder.createComponentMap(procGroupStatus);

        final boolean isClustered = context.isClustered();
        nodeIdentifier = context.getClusterNodeIdentifier();
        if (nodeIdentifier == null && isClustered) {
            logger.warn("This instance of NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                    + "The contentPath and previousContentPath fields will be null for all rows in this query");
        }

        try {
            this.provenanceEvents = provenanceEventRepository.getEvents(0, FETCH_SIZE);
        } catch (IOException ioe) {
            logger.error("Error retrieving provenance events, queries will return no rows");
        }
        reset();
    }

    @Override
    public Object current() {
        return currentRow;
    }

    @Override
    public boolean moveNext() {
        if (provenanceEvents == null) {
            return false;
        }
        currentRow = null;
        if (currentIndex == provenanceEvents.size()) {
            // Need to fetch a new set of rows, starting with the last ID + 1
            try {
                provenanceEvents = provenanceEventRepository.getEvents(currentId + 1, FETCH_SIZE);
                currentIndex = 0;
            } catch (IOException ioe) {
                logger.error("Error retrieving provenance events, queries will return no further rows");
                return false;
            }
        }

        if (provenanceEvents.isEmpty()) {
            // If we are out of data, close the InputStream. We do this because
            // Calcite does not necessarily call our close() method.
            close();
            try {
                onFinish();
            } catch (final Exception e) {
                logger.error("Failed to perform tasks when enumerator was finished", e);
            }

            return false;
        }

        ProvenanceEventRecord provenanceEvent = provenanceEvents.get(currentIndex);
        currentRow = filterColumns(provenanceEvent);
        currentId = provenanceEvent.getEventId();
        currentIndex++;
        return true;
    }

    protected int getRecordsRead() {
        return 1;
    }

    protected void onFinish() {
    }

    private Object filterColumns(final ProvenanceEventRecord provenanceEvent) {
        if (provenanceEvent == null) {
            return null;
        }

        final ArrayList<Object> rowList = new ArrayList<>();
        rowList.add(provenanceEvent.getEventId());
        rowList.add(provenanceEvent.getEventType().name());
        rowList.add(provenanceEvent.getEventTime());
        rowList.add(provenanceEvent.getEventDuration());
        rowList.add(provenanceEvent.getLineageStartDate());
        rowList.add(provenanceEvent.getDetails());
        rowList.add(provenanceEvent.getComponentId());
        rowList.add(componentMapHolder.getComponentName(provenanceEvent.getComponentId()));
        rowList.add(provenanceEvent.getComponentType());
        final String processGroupId = componentMapHolder.getProcessGroupId(provenanceEvent.getComponentId(), provenanceEvent.getComponentType());
        rowList.add(processGroupId);
        rowList.add(componentMapHolder.getComponentName(processGroupId));
        rowList.add(provenanceEvent.getFlowFileUuid());
        rowList.add("org.apache.nifi.flowfile.FlowFile"); // entityType
        rowList.add(provenanceEvent.getFileSize());
        rowList.add(provenanceEvent.getPreviousFileSize());
        rowList.add(provenanceEvent.getUpdatedAttributes());
        rowList.add(provenanceEvent.getPreviousAttributes());
        if (nodeIdentifier != null) {
            final String contentPathBase = "/nifi-api/provenance-events/" + provenanceEvent.getEventId() + "/content/";
            final String nodeIdSuffix = "?clusterNodeId=" + nodeIdentifier;
            rowList.add(contentPathBase + "output" + nodeIdSuffix);
            rowList.add(contentPathBase + "input" + nodeIdSuffix);
        } else {
            rowList.add(null);
            rowList.add(null);
        }
        rowList.add(provenanceEvent.getParentUuids());
        rowList.add(provenanceEvent.getChildUuids());
        rowList.add(provenanceEvent.getTransitUri());
        rowList.add(provenanceEvent.getSourceSystemFlowFileIdentifier());
        rowList.add(provenanceEvent.getAlternateIdentifierUri());

        final Object[] row = rowList.toArray();

        // If we want no fields just return null
        if (fields == null) {
            return row;
        }

        // If we want only a single field, then Calcite is going to expect us to return
        // the actual value, NOT a 1-element array of values.
        if (fields.length == 1) {
            final int desiredCellIndex = fields[0];
            return row[desiredCellIndex];
        }

        // Create a new Object array that contains only the desired fields.
        final Object[] filtered = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final int indexToKeep = fields[i];
            filtered[i] = row[indexToKeep];
        }

        return filtered;
    }

    @Override
    public void reset() {
        currentId = 0;
        currentIndex = 0;
    }

    @Override
    public void close() {
    }
}
