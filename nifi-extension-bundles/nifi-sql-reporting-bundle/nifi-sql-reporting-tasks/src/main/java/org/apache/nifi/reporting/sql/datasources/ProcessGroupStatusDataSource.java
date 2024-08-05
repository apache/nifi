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
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.IterableRowStream;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.util.ArrayList;
import java.util.List;

public class ProcessGroupStatusDataSource implements ResettableDataSource {
    private static final NiFiTableSchema SCHEMA = new NiFiTableSchema(List.of(
        new ColumnSchema("id", String.class, false),
        new ColumnSchema("groupId", String.class, false),
        new ColumnSchema("name", String.class, false),
        new ColumnSchema("bytesRead", long.class, false),
        new ColumnSchema("bytesWritten", long.class, false),
        new ColumnSchema("bytesReceived", long.class, false),
        new ColumnSchema("bytesSent", long.class, false),
        new ColumnSchema("bytesTransferred", long.class, false),
        new ColumnSchema("flowFilesReceived", int.class, false),
        new ColumnSchema("flowFilesSent", int.class, false),
        new ColumnSchema("flowFilesTransferred", int.class, false),
        new ColumnSchema("inputContentSize", long.class, false),
        new ColumnSchema("inputCount", int.class, false),
        new ColumnSchema("outputContentSize", long.class, false),
        new ColumnSchema("outputCount", int.class, false),
        new ColumnSchema("queuedCount", int.class, false),
        new ColumnSchema("queuedContentSize", long.class, false),
        new ColumnSchema("activeThreadCount", int.class, false),
        new ColumnSchema("terminatedThreadCount", int.class, false),
        new ColumnSchema("versionedFlowState", String.class, false),
        new ColumnSchema("processingNanos", long.class, false),
        new ColumnSchema("cpuDuration", long.class, false),
        new ColumnSchema("contentReadDuration", long.class, false),
        new ColumnSchema("contentWriteDuration", long.class, false),
        new ColumnSchema("sessionCommitDuration", long.class, false),
        new ColumnSchema("garbageCollectionDuration", long.class, false)
    ));


    private final ReportingContext reportingContext;
    private final GroupStatusCache groupStatusCache;
    private ProcessGroupStatus lastFetchedStatus = null;
    private List<GroupStatusAndParentId> lastStatuses = null;

    public ProcessGroupStatusDataSource(final ReportingContext reportingContext, final GroupStatusCache groupStatusCache) {
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

        final List<GroupStatusAndParentId> groupStatuses;
        if (groupStatus == lastFetchedStatus) {
            groupStatuses = lastStatuses;
        } else {
            groupStatuses = lastStatuses = gatherProcessGroupStatuses(groupStatus);
        }

        lastFetchedStatus = groupStatus;
        return new IterableRowStream<>(groupStatuses, this::toArray);
    }

    private List<GroupStatusAndParentId> gatherProcessGroupStatuses(final ProcessGroupStatus groupStatus) {
        final List<GroupStatusAndParentId> allStatuses = new ArrayList<>();
        gatherProcessGroupStatuses(null, groupStatus, allStatuses);
        return allStatuses;
    }

    private void gatherProcessGroupStatuses(final String parentGroupId, final ProcessGroupStatus groupStatus, final List<GroupStatusAndParentId> groupStatuses) {
        groupStatuses.add(new GroupStatusAndParentId(parentGroupId, groupStatus));

        for (final ProcessGroupStatus childStatus : groupStatus.getProcessGroupStatus()) {
            gatherProcessGroupStatuses(groupStatus.getId(), childStatus, groupStatuses);
        }
    }

    private Object[] toArray(final GroupStatusAndParentId groupStatusAndParentId) {
        final ProcessGroupStatus status = groupStatusAndParentId.status();
        final String parentId = groupStatusAndParentId.parentId();

        return new Object[] {
            status.getId(),
            parentId,
            status.getName(),
            status.getBytesRead(),
            status.getBytesWritten(),
            status.getBytesReceived(),
            status.getBytesSent(),
            status.getBytesTransferred(),
            status.getFlowFilesReceived(),
            status.getFlowFilesSent(),
            status.getFlowFilesTransferred(),
            status.getInputContentSize(),
            status.getInputCount(),
            status.getOutputContentSize(),
            status.getOutputCount(),
            status.getQueuedContentSize(),
            status.getActiveThreadCount(),
            status.getTerminatedThreadCount(),
            status.getQueuedCount(),
            status.getVersionedFlowState() == null ? null : status.getVersionedFlowState().name(),
            status.getProcessingNanos(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getCpuDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getContentReadDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getContentWriteDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getSessionCommitDuration(),
            status.getProcessingPerformanceStatus() == null ? -1 : status.getProcessingPerformanceStatus().getGarbageCollectionDuration()
        };
    }

    private record GroupStatusAndParentId(String parentId, ProcessGroupStatus status) {
    }
}