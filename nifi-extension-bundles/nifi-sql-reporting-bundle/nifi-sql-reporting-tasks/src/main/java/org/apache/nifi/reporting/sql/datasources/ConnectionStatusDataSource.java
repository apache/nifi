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

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.IterableRowStream;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.util.ArrayList;
import java.util.List;

public class ConnectionStatusDataSource implements ResettableDataSource {
    private static final NiFiTableSchema SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema("id", String.class, false),
        new ColumnSchema("groupId", String.class, false),
        new ColumnSchema("name", String.class, false),
        new ColumnSchema("sourceId", String.class, false),
        new ColumnSchema("sourceName", String.class, true),
        new ColumnSchema("destinationId", String.class, false),
        new ColumnSchema("destinationName", String.class, true),
        new ColumnSchema("backPressureDataSizeThreshold", String.class, true),
        new ColumnSchema("backPressureBytesThreshold", Long.class, true),
        new ColumnSchema("backPressureObjectThreshold", Long.class, true),
        new ColumnSchema("isBackPressureEnabled", boolean.class, false),
        new ColumnSchema("inputCount", long.class, false),
        new ColumnSchema("inputBytes", long.class, false),
        new ColumnSchema("queuedCount", int.class, false),
        new ColumnSchema("queuedBytes", long.class, false),
        new ColumnSchema("outputCount", long.class, false),
        new ColumnSchema("outputBytes", long.class, false),
        new ColumnSchema("maxQueuedCount", Integer.class, true),
        new ColumnSchema("maxQueuedBytes", Long.class, true)
    ));

    private final ReportingContext reportingContext;
    private final GroupStatusCache groupStatusCache;
    private ProcessGroupStatus lastFetchedStatus = null;
    private List<ConnectionStatus> lastConnectionStatuses = null;

    public ConnectionStatusDataSource(final ReportingContext reportingContext, final GroupStatusCache groupStatusCache) {
        this.reportingContext = reportingContext;
        this.groupStatusCache = groupStatusCache;
    }

    @Override
    public NiFiTableSchema getSchema() {
        return SCHEMA;
    }

    @Override
    public RowStream reset() {
        final ProcessGroupStatus groupStatus = groupStatusCache.getGroupStatus(reportingContext);

        final List<ConnectionStatus> connectionStatuses;
        if (groupStatus == lastFetchedStatus) {
            connectionStatuses = lastConnectionStatuses;
        } else {
            connectionStatuses = lastConnectionStatuses = gatherConnectionStatuses(groupStatus);
        }

        lastFetchedStatus = groupStatus;
        return new IterableRowStream<>(connectionStatuses, this::toArray);
    }

    private List<ConnectionStatus> gatherConnectionStatuses(final ProcessGroupStatus groupStatus) {
        final List<ConnectionStatus> allStatuses = new ArrayList<>();
        gatherConnectionStatuses(groupStatus, allStatuses);
        return allStatuses;
    }

    private void gatherConnectionStatuses(final ProcessGroupStatus groupStatus, final List<ConnectionStatus> connectionStatuses) {
        connectionStatuses.addAll(groupStatus.getConnectionStatus());
        for (final ProcessGroupStatus childStatus : groupStatus.getProcessGroupStatus()) {
            gatherConnectionStatuses(childStatus, connectionStatuses);
        }
    }

    private Object[] toArray(final ConnectionStatus status) {
        return new Object[] {
            status.getId(),
            status.getGroupId(),
            status.getName(),
            status.getSourceId(),
            status.getSourceName(),
            status.getDestinationId(),
            status.getDestinationName(),
            status.getBackPressureDataSizeThreshold(),
            status.getBackPressureBytesThreshold(),
            status.getBackPressureObjectThreshold(),
            // isBackPressureEnabled
            ((status.getBackPressureObjectThreshold() > 0 && status.getBackPressureObjectThreshold() <= status.getQueuedCount())
                || (status.getBackPressureBytesThreshold() > 0 && status.getBackPressureBytesThreshold() <= status.getQueuedBytes())),
            status.getInputCount(),
            status.getInputBytes(),
            status.getQueuedCount(),
            status.getQueuedBytes(),
            status.getOutputCount(),
            status.getOutputBytes(),
            status.getMaxQueuedCount(),
            status.getMaxQueuedBytes()
        };
    }
}
