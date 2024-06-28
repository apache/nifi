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
import org.apache.nifi.controller.status.analytics.ConnectionStatusPredictions;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.IterableRowStream;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.util.ArrayList;
import java.util.List;

public class ConnectionStatusPredictionDataSource implements ResettableDataSource {
    private static final NiFiTableSchema SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema("connectionId", String.class, false),
        new ColumnSchema("predictedQueuedBytes", long.class, true),
        new ColumnSchema("predictedQueuedCount", int.class, true),
        new ColumnSchema("predictedPercentBytes", int.class, true),
        new ColumnSchema("predictedPercentCount", int.class, true),
        new ColumnSchema("predictedTimeToBytesBackpressureMillis", long.class, true),
        new ColumnSchema("predictedTimeToCountBackpressureMillis", long.class, true),
        new ColumnSchema("predictionIntervalMillis", long.class, true)
    ));

    private final ReportingContext reportingContext;
    private final GroupStatusCache groupStatusCache;
    private ProcessGroupStatus lastFetchedStatus = null;
    private List<ConnectionStatus> lastConnectionStatuses = null;

    public ConnectionStatusPredictionDataSource(final ReportingContext reportingContext, final GroupStatusCache groupStatusCache) {
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
        final ConnectionStatusPredictions predictions = status.getPredictions();

        if (predictions == null) {
            return new Object[8];
        }

        return new Object[] {
            status.getId(),
            predictions.getNextPredictedQueuedBytes(),
            predictions.getNextPredictedQueuedCount(),
            predictions.getPredictedPercentBytes(),
            predictions.getPredictedPercentCount(),
            predictions.getPredictedTimeToBytesBackpressureMillis(),
            predictions.getPredictedTimeToCountBackpressureMillis(),
            predictions.getPredictionIntervalMillis()
        };
    }
}
