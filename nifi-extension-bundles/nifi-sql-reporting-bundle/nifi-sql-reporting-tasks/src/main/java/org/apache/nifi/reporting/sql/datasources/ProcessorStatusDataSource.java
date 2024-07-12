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
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.IterableRowStream;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProcessorStatusDataSource implements ResettableDataSource {
    private static final NiFiTableSchema SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema("id", String.class, false),
        new ColumnSchema("groupId", String.class, false),
        new ColumnSchema("name", String.class, false),
        new ColumnSchema("processorType", String.class, false),
        new ColumnSchema("averageLineageDuration", long.class, false),
        new ColumnSchema("bytesRead", long.class, false),
        new ColumnSchema("bytesWritten", long.class, false),
        new ColumnSchema("bytesReceived", long.class, false),
        new ColumnSchema("bytesSent", long.class, false),
        new ColumnSchema("flowFilesRemoved", int.class, false),
        new ColumnSchema("flowFilesReceived", int.class, false),
        new ColumnSchema("flowFilesSent", int.class, false),
        new ColumnSchema("inputCount", int.class, false),
        new ColumnSchema("inputBytes", long.class, false),
        new ColumnSchema("outputCount", int.class, false),
        new ColumnSchema("outputBytes", long.class, false),
        new ColumnSchema("activeThreadCount", int.class, false),
        new ColumnSchema("terminatedThreadCount", int.class, false),
        new ColumnSchema("invocations", int.class, false),
        new ColumnSchema("processingNanos", long.class, false),
        new ColumnSchema("runStatus", String.class, false),
        new ColumnSchema("executionNode", String.class, false),
        new ColumnSchema("cpuDuration", long.class, false),
        new ColumnSchema("contentReadDuration", long.class, false),
        new ColumnSchema("contentWriteDuration", long.class, false),
        new ColumnSchema("sessionCommitDuration", long.class, false),
        new ColumnSchema("garbageCollectionDuration", long.class, false)
    ));


    private final ReportingContext reportingContext;
    private final GroupStatusCache groupStatusCache;
    private ProcessGroupStatus lastFetchedStatus = null;
    private List<ProcessorStatus> lastStatuses = null;

    public ProcessorStatusDataSource(final ReportingContext reportingContext, final GroupStatusCache groupStatusCache) {
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

        final List<ProcessorStatus> processorStatuses;
        if (groupStatus == lastFetchedStatus) {
            processorStatuses = lastStatuses;
        } else {
            processorStatuses = lastStatuses = gatherProcessorStatuses(groupStatus);
        }

        lastFetchedStatus = groupStatus;
        return new IterableRowStream<>(processorStatuses, this::toArray);
    }

    private List<ProcessorStatus> gatherProcessorStatuses(final ProcessGroupStatus groupStatus) {
        final List<ProcessorStatus> allStatuses = new ArrayList<>();
        gatherProcessorStatuses(groupStatus, allStatuses);
        return allStatuses;
    }

    private void gatherProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> processorStatuses) {
        processorStatuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childStatus : groupStatus.getProcessGroupStatus()) {
            gatherProcessorStatuses(childStatus, processorStatuses);
        }
    }

    private Object[] toArray(final ProcessorStatus status) {
        return new Object[] {
            status.getId(),
            status.getGroupId(),
            status.getName(),
            status.getType(),
            status.getAverageLineageDuration(TimeUnit.MILLISECONDS),
            status.getBytesRead(),
            status.getBytesWritten(),
            status.getBytesReceived(),
            status.getBytesSent(),
            status.getFlowFilesRemoved(),
            status.getFlowFilesReceived(),
            status.getFlowFilesSent(),
            status.getInputCount(),
            status.getInputBytes(),
            status.getOutputCount(),
            status.getOutputBytes(),
            status.getActiveThreadCount(),
            status.getTerminatedThreadCount(),
            status.getInvocations(),
            status.getProcessingNanos(),
            status.getRunStatus().name(),
            status.getExecutionNode() == null ? null : status.getExecutionNode().name(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getCpuDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getContentReadDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getContentWriteDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getSessionCommitDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getGarbageCollectionDuration()
        };
    }
}