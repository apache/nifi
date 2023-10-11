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

package org.apache.nifi.reporting.sql.datasources;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ComponentMapHolder;
import org.apache.nifi.sql.ArrayType;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.MapType;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;
import org.apache.nifi.sql.ScalarType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ProvenanceDataSource implements ResettableDataSource {

    public static final NiFiTableSchema SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema("eventId", long.class, false),
        new ColumnSchema("eventType", String.class, false),
        new ColumnSchema("timestampMillis", long.class, true),
        new ColumnSchema("durationMillis", long.class, true),
        new ColumnSchema("lineageStart", long.class, true),
        new ColumnSchema("details", String.class, true),
        new ColumnSchema("componentId", String.class, true),
        new ColumnSchema("componentName", String.class, true),
        new ColumnSchema("componentType", String.class, true),
        new ColumnSchema("processGroupId", String.class, true),
        new ColumnSchema("processGroupName", String.class, true),
        new ColumnSchema("entityId", String.class, true),
        new ColumnSchema("entityType", String.class, true),
        new ColumnSchema("entitySize", long.class, true),
        new ColumnSchema("previousEntitySize", long.class, true),
        new ColumnSchema("updatedAttributes", new MapType(ScalarType.STRING, ScalarType.STRING), true),
        new ColumnSchema("previousAttributes", new MapType(ScalarType.STRING, ScalarType.STRING), true),
        new ColumnSchema("contentPath", String.class, true),
        new ColumnSchema("previousContentPath", String.class, true),
        new ColumnSchema("parentIds", new ArrayType(ScalarType.STRING), true),
        new ColumnSchema("childIds", new ArrayType(ScalarType.STRING), true),
        new ColumnSchema("transitUri", String.class, true),
        new ColumnSchema("remoteIdentifier", String.class, true),
        new ColumnSchema("alternateIdentifier", String.class, true)
    ));

    private static final String ENTITY_TYPE = "org.apache.nifi.flowfile.FlowFile";
    private static final int FETCH_SIZE = 1_000;
    private final ReportingContext reportingContext;
    private final ComponentMapHolder componentMapHolder;

    private Iterator<ProvenanceEventRecord> iterator;
    private long lastId = -1L;


    public ProvenanceDataSource(final ReportingContext reportingContext) {
        this.reportingContext = reportingContext;

        final ProcessGroupStatus procGroupStatus = reportingContext.getEventAccess().getControllerStatus();
        this.componentMapHolder = ComponentMapHolder.createComponentMap(procGroupStatus);
    }

    @Override
    public NiFiTableSchema getSchema() {
        return SCHEMA;
    }

    @Override
    public RowStream reset() throws IOException {
        lastId = -1L;
        iterator = null;

        return new RowStream() {
            @Override
            public Object[] nextRow() throws IOException {
                if (iterator == null || !iterator.hasNext()) {
                    iterator = fetchEvents();
                }

                if (iterator.hasNext()) {
                    final ProvenanceEventRecord event = iterator.next();
                    lastId = event.getEventId();
                    return toArray(event);
                }

                return null;
            }

            @Override
            public void close() {
            }
        };
    }

    private Object[] toArray(final ProvenanceEventRecord provenanceEvent) {
        final String nodeIdentifier = reportingContext.getClusterNodeIdentifier();
        final String processGroupId = componentMapHolder.getProcessGroupId(provenanceEvent.getComponentId(), provenanceEvent.getComponentType());
        final String groupName = componentMapHolder.getComponentName(processGroupId);

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
        rowList.add(processGroupId);
        rowList.add(groupName);
        rowList.add(provenanceEvent.getFlowFileUuid());
        rowList.add(ENTITY_TYPE); // entityType
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

        return rowList.toArray();
    }

    private Iterator<ProvenanceEventRecord> fetchEvents() throws IOException {
        final List<ProvenanceEventRecord> events = reportingContext.getEventAccess().getProvenanceEvents(lastId + 1, FETCH_SIZE);
        return events.iterator();
    }
}
