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

package org.apache.nifi.cluster.manager;

import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.CountersSnapshotDTO;
import org.apache.nifi.web.api.dto.NodeCountersSnapshotDTO;
import org.apache.nifi.web.api.dto.NodeSystemDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsSnapshotDTO.GarbageCollectionDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsSnapshotDTO.StorageUsageDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.NodePortStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.NodeRemoteProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.PortStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ConnectionStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.PortStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusSnapshotEntity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StatusMerger {
    private static final String ZERO_COUNT = "0";
    private static final String ZERO_BYTES = "0 bytes";
    private static final String ZERO_COUNT_AND_BYTES = "0 (0 bytes)";
    private static final String EMPTY_COUNT = "-";
    private static final String EMPTY_BYTES = "-";

    public static void merge(final ControllerStatusDTO target, final ControllerStatusDTO toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        target.setBytesQueued(target.getBytesQueued() + toMerge.getBytesQueued());
        target.setFlowFilesQueued(target.getFlowFilesQueued() + toMerge.getFlowFilesQueued());

        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final ControllerStatusDTO target) {
        target.setQueued(prettyPrint(target.getFlowFilesQueued(), target.getBytesQueued()));
    }

    public static void merge(final ProcessGroupStatusDTO target, final boolean targetReadablePermission, final ProcessGroupStatusDTO toMerge, final boolean toMergeReadablePermission,
                             final String nodeId, final String nodeAddress, final Integer nodeApiPort) {
        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
        }

        merge(target.getAggregateSnapshot(), targetReadablePermission, toMerge.getAggregateSnapshot(), toMergeReadablePermission);

        if (target.getNodeSnapshots() != null) {
            final NodeProcessGroupStatusSnapshotDTO nodeSnapshot = new NodeProcessGroupStatusSnapshotDTO();
            nodeSnapshot.setStatusSnapshot(toMerge.getAggregateSnapshot());
            nodeSnapshot.setAddress(nodeAddress);
            nodeSnapshot.setApiPort(nodeApiPort);
            nodeSnapshot.setNodeId(nodeId);

            target.getNodeSnapshots().add(nodeSnapshot);
        }
    }

    public static void merge(final ProcessGroupStatusSnapshotEntity target, ProcessGroupStatusSnapshotEntity toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        merge(target.getProcessGroupStatusSnapshot(), target.getCanRead(), toMerge.getProcessGroupStatusSnapshot(), toMerge.getCanRead());
    }

    public static void merge(final ProcessGroupStatusSnapshotDTO target, final boolean targetReadablePermission, final ProcessGroupStatusSnapshotDTO toMerge,
                             final boolean toMergeReadablePermission) {
        if (target == null || toMerge == null) {
            return;
        }

        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
        }

        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());

        target.setBytesQueued(target.getBytesQueued() + toMerge.getBytesQueued());
        target.setFlowFilesQueued(target.getFlowFilesQueued() + toMerge.getFlowFilesQueued());

        target.setBytesRead(target.getBytesRead() + toMerge.getBytesRead());
        target.setBytesWritten(target.getBytesWritten() + toMerge.getBytesWritten());

        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());

        target.setBytesTransferred(target.getBytesTransferred() + toMerge.getBytesTransferred());
        target.setFlowFilesTransferred(target.getFlowFilesTransferred() + toMerge.getFlowFilesTransferred());

        target.setBytesReceived(target.getBytesReceived() + toMerge.getBytesReceived());
        target.setFlowFilesReceived(target.getFlowFilesReceived() + toMerge.getFlowFilesReceived());

        target.setBytesSent(target.getBytesSent() + toMerge.getBytesSent());
        target.setFlowFilesSent(target.getFlowFilesSent() + toMerge.getFlowFilesSent());

        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        updatePrettyPrintedFields(target);

        // connection status
        // sort by id
        final Map<String, ConnectionStatusSnapshotEntity> mergedConnectionMap = new HashMap<>();
        for (final ConnectionStatusSnapshotEntity status : replaceNull(target.getConnectionStatusSnapshots())) {
            mergedConnectionMap.put(status.getId(), status);
        }

        for (final ConnectionStatusSnapshotEntity statusToMerge : replaceNull(toMerge.getConnectionStatusSnapshots())) {
            ConnectionStatusSnapshotEntity merged = mergedConnectionMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedConnectionMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setConnectionStatusSnapshots(mergedConnectionMap.values());

        // processor status
        final Map<String, ProcessorStatusSnapshotEntity> mergedProcessorMap = new HashMap<>();
        for (final ProcessorStatusSnapshotEntity status : replaceNull(target.getProcessorStatusSnapshots())) {
            mergedProcessorMap.put(status.getId(), status);
        }

        for (final ProcessorStatusSnapshotEntity statusToMerge : replaceNull(toMerge.getProcessorStatusSnapshots())) {
            ProcessorStatusSnapshotEntity merged = mergedProcessorMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedProcessorMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setProcessorStatusSnapshots(mergedProcessorMap.values());


        // input ports
        final Map<String, PortStatusSnapshotEntity> mergedInputPortMap = new HashMap<>();
        for (final PortStatusSnapshotEntity status : replaceNull(target.getInputPortStatusSnapshots())) {
            mergedInputPortMap.put(status.getId(), status);
        }

        for (final PortStatusSnapshotEntity statusToMerge : replaceNull(toMerge.getInputPortStatusSnapshots())) {
            PortStatusSnapshotEntity merged = mergedInputPortMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedInputPortMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setInputPortStatusSnapshots(mergedInputPortMap.values());

        // output ports
        final Map<String, PortStatusSnapshotEntity> mergedOutputPortMap = new HashMap<>();
        for (final PortStatusSnapshotEntity status : replaceNull(target.getOutputPortStatusSnapshots())) {
            mergedOutputPortMap.put(status.getId(), status);
        }

        for (final PortStatusSnapshotEntity statusToMerge : replaceNull(toMerge.getOutputPortStatusSnapshots())) {
            PortStatusSnapshotEntity merged = mergedOutputPortMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedOutputPortMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setOutputPortStatusSnapshots(mergedOutputPortMap.values());

        // child groups
        final Map<String, ProcessGroupStatusSnapshotEntity> mergedGroupMap = new HashMap<>();
        for (final ProcessGroupStatusSnapshotEntity status : replaceNull(target.getProcessGroupStatusSnapshots())) {
            mergedGroupMap.put(status.getId(), status);
        }

        for (final ProcessGroupStatusSnapshotEntity statusToMerge : replaceNull(toMerge.getProcessGroupStatusSnapshots())) {
            ProcessGroupStatusSnapshotEntity merged = mergedGroupMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedGroupMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setOutputPortStatusSnapshots(mergedOutputPortMap.values());

        // remote groups
        final Map<String, RemoteProcessGroupStatusSnapshotEntity> mergedRemoteGroupMap = new HashMap<>();
        for (final RemoteProcessGroupStatusSnapshotEntity status : replaceNull(target.getRemoteProcessGroupStatusSnapshots())) {
            mergedRemoteGroupMap.put(status.getId(), status);
        }

        for (final RemoteProcessGroupStatusSnapshotEntity statusToMerge : replaceNull(toMerge.getRemoteProcessGroupStatusSnapshots())) {
            RemoteProcessGroupStatusSnapshotEntity merged = mergedRemoteGroupMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedRemoteGroupMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setRemoteProcessGroupStatusSnapshots(mergedRemoteGroupMap.values());
    }

    private static <T> Collection<T> replaceNull(final Collection<T> collection) {
        return (collection == null) ? Collections.<T>emptyList() : collection;
    }


    /**
     * Updates the fields that are "pretty printed" based on the raw values currently set. For example,
     * {@link ProcessGroupStatusSnapshotDTO#setInput(String)} will be called with the pretty-printed form of the
     * FlowFile counts and sizes retrieved via {@link ProcessGroupStatusSnapshotDTO#getFlowFilesIn()} and
     * {@link ProcessGroupStatusSnapshotDTO#getBytesIn()}.
     * <p>
     * This logic is performed here, rather than in the DTO itself because the DTO needs to be kept purely
     * getters & setters - otherwise the automatic marshalling and unmarshalling to/from JSON becomes very
     * complicated.
     *
     * @param target the DTO to update
     */
    public static void updatePrettyPrintedFields(final ProcessGroupStatusSnapshotDTO target) {
        target.setQueued(prettyPrint(target.getFlowFilesQueued(), target.getBytesQueued()));
        target.setQueuedCount(formatCount(target.getFlowFilesQueued()));
        target.setQueuedSize(formatDataSize(target.getBytesQueued()));
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setRead(formatDataSize(target.getBytesRead()));
        target.setWritten(formatDataSize(target.getBytesWritten()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));
        target.setTransferred(prettyPrint(target.getFlowFilesTransferred(), target.getBytesTransferred()));
        target.setReceived(prettyPrint(target.getFlowFilesReceived(), target.getBytesReceived()));
        target.setSent(prettyPrint(target.getFlowFilesSent(), target.getBytesSent()));
    }

    public static void merge(final RemoteProcessGroupStatusDTO target, final boolean targetReadablePermission, final RemoteProcessGroupStatusDTO toMerge,
                             final boolean toMergeReadablePermission, final String nodeId, final String nodeAddress, final Integer nodeApiPort) {
        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
            target.setTargetUri(toMerge.getTargetUri());
        }

        merge(target.getAggregateSnapshot(), targetReadablePermission, toMerge.getAggregateSnapshot(), toMergeReadablePermission);

        if (target.getNodeSnapshots() != null) {
            final NodeRemoteProcessGroupStatusSnapshotDTO nodeSnapshot = new NodeRemoteProcessGroupStatusSnapshotDTO();
            nodeSnapshot.setStatusSnapshot(toMerge.getAggregateSnapshot());
            nodeSnapshot.setAddress(nodeAddress);
            nodeSnapshot.setApiPort(nodeApiPort);
            nodeSnapshot.setNodeId(nodeId);

            target.getNodeSnapshots().add(nodeSnapshot);
        }
    }

    public static void merge(final PortStatusDTO target, final boolean targetReadablePermission, final PortStatusDTO toMerge, final boolean toMergeReadablePermission, final String nodeId,
                             final String nodeAddress, final Integer nodeApiPort) {
        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
        }

        merge(target.getAggregateSnapshot(), targetReadablePermission, toMerge.getAggregateSnapshot(), toMergeReadablePermission);

        target.setTransmitting(Boolean.TRUE.equals(target.isTransmitting()) || Boolean.TRUE.equals(toMerge.isTransmitting()));

        if (target.getNodeSnapshots() != null) {
            final NodePortStatusSnapshotDTO nodeSnapshot = new NodePortStatusSnapshotDTO();
            nodeSnapshot.setStatusSnapshot(toMerge.getAggregateSnapshot());
            nodeSnapshot.setAddress(nodeAddress);
            nodeSnapshot.setApiPort(nodeApiPort);
            nodeSnapshot.setNodeId(nodeId);

            target.getNodeSnapshots().add(nodeSnapshot);
        }
    }

    public static void merge(final ConnectionStatusDTO target, final boolean targetReadablePermission, final ConnectionStatusDTO toMerge, final boolean toMergeReadablePermission,
                             final String nodeId, final String nodeAddress, final Integer nodeApiPort) {
        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
            target.setSourceId(toMerge.getSourceId());
            target.setSourceName(toMerge.getSourceName());
            target.setDestinationId(toMerge.getDestinationId());
            target.setDestinationName(toMerge.getDestinationName());
        }

        merge(target.getAggregateSnapshot(), targetReadablePermission, toMerge.getAggregateSnapshot(), toMergeReadablePermission);

        if (target.getNodeSnapshots() != null) {
            final NodeConnectionStatusSnapshotDTO nodeSnapshot = new NodeConnectionStatusSnapshotDTO();
            nodeSnapshot.setStatusSnapshot(toMerge.getAggregateSnapshot());
            nodeSnapshot.setAddress(nodeAddress);
            nodeSnapshot.setApiPort(nodeApiPort);
            nodeSnapshot.setNodeId(nodeId);

            target.getNodeSnapshots().add(nodeSnapshot);
        }
    }

    public static void merge(final ProcessorStatusDTO target, final boolean targetReadablePermission, final ProcessorStatusDTO toMerge, final boolean toMergeReadablePermission,
                             final String nodeId, final String nodeAddress, final Integer nodeApiPort) {
        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
            target.setType(toMerge.getType());
        }

        merge(target.getAggregateSnapshot(), targetReadablePermission, toMerge.getAggregateSnapshot(), toMergeReadablePermission);

        if (target.getNodeSnapshots() != null) {
            final NodeProcessorStatusSnapshotDTO nodeSnapshot = new NodeProcessorStatusSnapshotDTO();
            nodeSnapshot.setStatusSnapshot(toMerge.getAggregateSnapshot());
            nodeSnapshot.setAddress(nodeAddress);
            nodeSnapshot.setApiPort(nodeApiPort);
            nodeSnapshot.setNodeId(nodeId);

            target.getNodeSnapshots().add(nodeSnapshot);
        }
    }

    public static void merge(final ProcessorStatusSnapshotEntity target, ProcessorStatusSnapshotEntity toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        merge(target.getProcessorStatusSnapshot(), target.getCanRead(), toMerge.getProcessorStatusSnapshot(), toMerge.getCanRead());
    }

    public static void merge(final ProcessorStatusSnapshotDTO target, final boolean targetReadablePermission, final ProcessorStatusSnapshotDTO toMerge,
                             final boolean toMergeReadablePermission) {
        if (target == null || toMerge == null) {
            return;
        }

        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
            target.setType(toMerge.getType());
        }

        // if the status to merge is invalid allow it to take precedence. whether the
        // processor run status is disabled/stopped/running is part of the flow configuration
        // and should not differ amongst nodes. however, whether a processor is invalid
        // can be driven by environmental conditions. this check allows any of those to
        // take precedence over the configured run status.
        if (RunStatus.Invalid.name().equals(toMerge.getRunStatus())) {
            target.setRunStatus(RunStatus.Invalid.name());
        }

        target.setBytesRead(target.getBytesRead() + toMerge.getBytesRead());
        target.setBytesWritten(target.getBytesWritten() + toMerge.getBytesWritten());
        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());
        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());
        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setTaskCount(target.getTaskCount() + toMerge.getTaskCount());
        target.setTasksDurationNanos(target.getTasksDurationNanos() + toMerge.getTasksDurationNanos());
        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final ProcessorStatusSnapshotDTO target) {
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setRead(formatDataSize(target.getBytesRead()));
        target.setWritten(formatDataSize(target.getBytesWritten()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));

        final Integer taskCount = target.getTaskCount();
        final String tasks = (taskCount == null) ? "-" : formatCount(taskCount);
        target.setTasks(tasks);

        target.setTasksDuration(FormatUtils.formatHoursMinutesSeconds(target.getTasksDurationNanos(), TimeUnit.NANOSECONDS));
    }

    public static void merge(final ConnectionStatusSnapshotEntity target, ConnectionStatusSnapshotEntity toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        merge(target.getConnectionStatusSnapshot(), target.getCanRead(), toMerge.getConnectionStatusSnapshot(), toMerge.getCanRead());
    }

    public static void merge(final ConnectionStatusSnapshotDTO target, final boolean targetReadablePermission, final ConnectionStatusSnapshotDTO toMerge,
                             final boolean toMergeReadablePermission) {
        if (target == null || toMerge == null) {
            return;
        }

        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
            target.setSourceId(toMerge.getSourceId());
            target.setSourceName(toMerge.getSourceName());
            target.setDestinationId(toMerge.getDestinationId());
            target.setDestinationName(toMerge.getDestinationName());
        }

        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());
        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());
        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setFlowFilesQueued(target.getFlowFilesQueued() + toMerge.getFlowFilesQueued());
        target.setBytesQueued(target.getBytesQueued() + toMerge.getBytesQueued());

        if (target.getPercentUseBytes() == null) {
            target.setPercentUseBytes(toMerge.getPercentUseBytes());
        } else if (toMerge.getPercentUseBytes() != null) {
            target.setPercentUseBytes(Math.max(target.getPercentUseBytes(), toMerge.getPercentUseBytes()));
        }
        if (target.getPercentUseCount() == null) {
            target.setPercentUseCount(toMerge.getPercentUseCount());
        } else if (toMerge.getPercentUseCount() != null) {
            target.setPercentUseCount(Math.max(target.getPercentUseCount(), toMerge.getPercentUseCount()));
        }

        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final ConnectionStatusSnapshotDTO target) {
        target.setQueued(prettyPrint(target.getFlowFilesQueued(), target.getBytesQueued()));
        target.setQueuedCount(formatCount(target.getFlowFilesQueued()));
        target.setQueuedSize(formatDataSize(target.getBytesQueued()));
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));
    }


    public static void merge(final RemoteProcessGroupStatusSnapshotEntity target, RemoteProcessGroupStatusSnapshotEntity toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        merge(target.getRemoteProcessGroupStatusSnapshot(), target.getCanRead(), toMerge.getRemoteProcessGroupStatusSnapshot(), toMerge.getCanRead());
    }

    public static void merge(final RemoteProcessGroupStatusSnapshotDTO target, final boolean targetReadablePermission, final RemoteProcessGroupStatusSnapshotDTO toMerge,
                             final boolean toMergeReadablePermission) {
        if (target == null || toMerge == null) {
            return;
        }

        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
            target.setTargetUri(toMerge.getTargetUri());
        }

        final String transmittingValue = TransmissionStatus.Transmitting.name();
        if (transmittingValue.equals(target.getTransmissionStatus()) || transmittingValue.equals(toMerge.getTransmissionStatus())) {
            target.setTransmissionStatus(transmittingValue);
        }

        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());

        target.setFlowFilesSent(target.getFlowFilesSent() + toMerge.getFlowFilesSent());
        target.setBytesSent(target.getBytesSent() + toMerge.getBytesSent());
        target.setFlowFilesReceived(target.getFlowFilesReceived() + toMerge.getFlowFilesReceived());
        target.setBytesReceived(target.getBytesReceived() + toMerge.getBytesReceived());
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final RemoteProcessGroupStatusSnapshotDTO target) {
        target.setReceived(prettyPrint(target.getFlowFilesReceived(), target.getBytesReceived()));
        target.setSent(prettyPrint(target.getFlowFilesSent(), target.getBytesSent()));
    }


    public static void merge(final PortStatusSnapshotEntity target, PortStatusSnapshotEntity toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        merge(target.getPortStatusSnapshot(), target.getCanRead(), toMerge.getPortStatusSnapshot(), toMerge.getCanRead());
    }

    public static void merge(final PortStatusSnapshotDTO target, final boolean targetReadablePermission, final PortStatusSnapshotDTO toMerge, final boolean toMergeReadablePermission) {
        if (target == null || toMerge == null) {
            return;
        }

        if (targetReadablePermission && !toMergeReadablePermission) {
            target.setGroupId(toMerge.getGroupId());
            target.setId(toMerge.getId());
            target.setName(toMerge.getName());
        }

        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        target.setFlowFilesIn(target.getFlowFilesIn() + toMerge.getFlowFilesIn());
        target.setBytesIn(target.getBytesIn() + toMerge.getBytesIn());
        target.setFlowFilesOut(target.getFlowFilesOut() + toMerge.getFlowFilesOut());
        target.setBytesOut(target.getBytesOut() + toMerge.getBytesOut());
        target.setTransmitting(Boolean.TRUE.equals(target.isTransmitting()) || Boolean.TRUE.equals(toMerge.isTransmitting()));

        // should be unnecessary here since ports run status not should be affected by
        // environmental conditions but doing so in case that changes
        if (RunStatus.Invalid.name().equals(toMerge.getRunStatus())) {
            target.setRunStatus(RunStatus.Invalid.name());
        }

        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final PortStatusSnapshotDTO target) {
        target.setInput(prettyPrint(target.getFlowFilesIn(), target.getBytesIn()));
        target.setOutput(prettyPrint(target.getFlowFilesOut(), target.getBytesOut()));
    }


    public static void merge(final SystemDiagnosticsDTO target, final SystemDiagnosticsDTO toMerge, final String nodeId, final String nodeAddress, final Integer nodeApiPort) {
        merge(target.getAggregateSnapshot(), toMerge.getAggregateSnapshot());

        List<NodeSystemDiagnosticsSnapshotDTO> nodeSnapshots = target.getNodeSnapshots();
        if (nodeSnapshots == null) {
            nodeSnapshots = new ArrayList<>();
        }

        final NodeSystemDiagnosticsSnapshotDTO nodeSnapshot = new NodeSystemDiagnosticsSnapshotDTO();
        nodeSnapshot.setAddress(nodeAddress);
        nodeSnapshot.setApiPort(nodeApiPort);
        nodeSnapshot.setNodeId(nodeId);
        nodeSnapshot.setSnapshot(toMerge.getAggregateSnapshot());

        nodeSnapshots.add(nodeSnapshot);
        target.setNodeSnapshots(nodeSnapshots);
    }

    public static void merge(final SystemDiagnosticsSnapshotDTO target, final SystemDiagnosticsSnapshotDTO toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setAvailableProcessors(target.getAvailableProcessors() + toMerge.getAvailableProcessors());
        target.setDaemonThreads(target.getDaemonThreads() + toMerge.getDaemonThreads());
        target.setFreeHeapBytes(target.getFreeHeapBytes() + toMerge.getFreeHeapBytes());
        target.setFreeNonHeapBytes(target.getFreeNonHeapBytes() + toMerge.getFreeNonHeapBytes());
        target.setMaxHeapBytes(target.getMaxHeapBytes() + toMerge.getMaxHeapBytes());
        target.setMaxNonHeapBytes(target.getMaxNonHeapBytes() + toMerge.getMaxNonHeapBytes());
        double systemLoad = target.getProcessorLoadAverage();
        double toMergeSystemLoad = toMerge.getProcessorLoadAverage();
        if (systemLoad >= 0 && toMergeSystemLoad >= 0) {
            systemLoad += toMergeSystemLoad;
        } else if (systemLoad < 0 && toMergeSystemLoad >= 0) {
            systemLoad = toMergeSystemLoad;
        }
        target.setProcessorLoadAverage(systemLoad);
        target.setTotalHeapBytes(target.getTotalHeapBytes() + toMerge.getTotalHeapBytes());
        target.setTotalNonHeapBytes(target.getTotalNonHeapBytes() + toMerge.getTotalNonHeapBytes());
        target.setTotalThreads(target.getTotalThreads() + toMerge.getTotalThreads());
        target.setUsedHeapBytes(target.getUsedHeapBytes() + toMerge.getUsedHeapBytes());
        target.setUsedNonHeapBytes(target.getUsedNonHeapBytes() + toMerge.getUsedNonHeapBytes());

        merge(target.getContentRepositoryStorageUsage(), toMerge.getContentRepositoryStorageUsage());
        merge(target.getProvenanceRepositoryStorageUsage(), toMerge.getProvenanceRepositoryStorageUsage());
        merge(target.getFlowFileRepositoryStorageUsage(), toMerge.getFlowFileRepositoryStorageUsage());
        mergeGarbageCollection(target.getGarbageCollection(), toMerge.getGarbageCollection());

        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final SystemDiagnosticsSnapshotDTO target) {
        // heap
        target.setMaxHeap(FormatUtils.formatDataSize(target.getMaxHeapBytes()));
        target.setTotalHeap(FormatUtils.formatDataSize(target.getTotalHeapBytes()));
        target.setUsedHeap(FormatUtils.formatDataSize(target.getUsedHeapBytes()));
        target.setFreeHeap(FormatUtils.formatDataSize(target.getFreeHeapBytes()));
        if (target.getMaxHeapBytes() != -1) {
            target.setHeapUtilization(FormatUtils.formatUtilization(getUtilization(target.getUsedHeapBytes(), target.getMaxHeapBytes())));
        }

        // non heap
        target.setMaxNonHeap(FormatUtils.formatDataSize(target.getMaxNonHeapBytes()));
        target.setTotalNonHeap(FormatUtils.formatDataSize(target.getTotalNonHeapBytes()));
        target.setUsedNonHeap(FormatUtils.formatDataSize(target.getUsedNonHeapBytes()));
        target.setFreeNonHeap(FormatUtils.formatDataSize(target.getFreeNonHeapBytes()));
        if (target.getMaxNonHeapBytes() != -1) {
            target.setNonHeapUtilization(FormatUtils.formatUtilization(getUtilization(target.getUsedNonHeapBytes(), target.getMaxNonHeapBytes())));
        }
    }

    public static void merge(final Set<StorageUsageDTO> targetSet, final Set<StorageUsageDTO> toMerge) {
        final Map<String, StorageUsageDTO> storageById = new HashMap<>();
        for (final StorageUsageDTO targetUsage : targetSet) {
            storageById.put(targetUsage.getIdentifier(), targetUsage);
        }

        for (final StorageUsageDTO usageToMerge : toMerge) {
            final StorageUsageDTO targetUsage = storageById.get(usageToMerge.getIdentifier());
            if (targetUsage == null) {
                storageById.put(usageToMerge.getIdentifier(), usageToMerge);
            } else {
                merge(targetUsage, usageToMerge);
            }
        }

        targetSet.clear();
        targetSet.addAll(storageById.values());
    }

    public static void merge(final StorageUsageDTO target, final StorageUsageDTO toMerge) {
        target.setFreeSpaceBytes(target.getFreeSpaceBytes() + toMerge.getFreeSpaceBytes());
        target.setTotalSpaceBytes(target.getTotalSpaceBytes() + toMerge.getTotalSpaceBytes());
        target.setUsedSpaceBytes(target.getUsedSpaceBytes() + toMerge.getUsedSpaceBytes());
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final StorageUsageDTO target) {
        target.setFreeSpace(FormatUtils.formatDataSize(target.getFreeSpaceBytes()));
        target.setTotalSpace(FormatUtils.formatDataSize(target.getTotalSpaceBytes()));
        target.setUsedSpace(FormatUtils.formatDataSize(target.getUsedSpaceBytes()));

        if (target.getTotalSpaceBytes() != -1) {
            target.setUtilization(FormatUtils.formatUtilization(getUtilization(target.getUsedSpaceBytes(), target.getTotalSpaceBytes())));
        }
    }


    public static void mergeGarbageCollection(final Set<GarbageCollectionDTO> targetSet, final Set<GarbageCollectionDTO> toMerge) {
        final Map<String, GarbageCollectionDTO> storageById = new HashMap<>();
        for (final GarbageCollectionDTO targetUsage : targetSet) {
            storageById.put(targetUsage.getName(), targetUsage);
        }

        for (final GarbageCollectionDTO usageToMerge : toMerge) {
            final GarbageCollectionDTO targetUsage = storageById.get(usageToMerge.getName());
            if (targetUsage == null) {
                storageById.put(usageToMerge.getName(), usageToMerge);
            } else {
                merge(targetUsage, usageToMerge);
            }
        }

        targetSet.clear();
        targetSet.addAll(storageById.values());
    }

    public static void merge(final GarbageCollectionDTO target, final GarbageCollectionDTO toMerge) {
        target.setCollectionCount(target.getCollectionCount() + toMerge.getCollectionCount());
        target.setCollectionMillis(target.getCollectionMillis() + toMerge.getCollectionMillis());
        updatePrettyPrintedFields(target);
    }

    public static void updatePrettyPrintedFields(final GarbageCollectionDTO target) {
        target.setCollectionTime(FormatUtils.formatHoursMinutesSeconds(target.getCollectionMillis(), TimeUnit.MILLISECONDS));
    }

    public static void merge(final CountersDTO target, final CountersDTO toMerge, final String nodeId, final String nodeAddress, final Integer nodeApiPort) {
        merge(target.getAggregateSnapshot(), toMerge.getAggregateSnapshot());

        List<NodeCountersSnapshotDTO> nodeSnapshots = target.getNodeSnapshots();
        if (nodeSnapshots == null) {
            nodeSnapshots = new ArrayList<>();
        }

        final NodeCountersSnapshotDTO nodeCountersSnapshot = new NodeCountersSnapshotDTO();
        nodeCountersSnapshot.setNodeId(nodeId);
        nodeCountersSnapshot.setAddress(nodeAddress);
        nodeCountersSnapshot.setApiPort(nodeApiPort);
        nodeCountersSnapshot.setSnapshot(toMerge.getAggregateSnapshot());

        nodeSnapshots.add(nodeCountersSnapshot);

        target.setNodeSnapshots(nodeSnapshots);
    }

    public static void merge(final CountersSnapshotDTO target, final CountersSnapshotDTO toMerge) {
        final Map<String, CounterDTO> counters = new HashMap<>();

        for (final CounterDTO counter : target.getCounters()) {
            counters.put(counter.getId(), counter);
        }

        for (final CounterDTO counter : toMerge.getCounters()) {
            final CounterDTO existing = counters.get(counter.getId());
            if (existing == null) {
                counters.put(counter.getId(), counter);
            } else {
                merge(existing, counter);
            }
        }

        target.setCounters(counters.values());
    }

    public static void merge(final CounterDTO target, final CounterDTO toMerge) {
        target.setValueCount(target.getValueCount() + toMerge.getValueCount());
        target.setValue(FormatUtils.formatCount(target.getValueCount()));
    }


    public static int getUtilization(final double used, final double total) {
        return (int) Math.round((used / total) * 100);
    }

    public static String formatCount(final Integer intStatus) {
        if (intStatus == null) {
            return EMPTY_COUNT;
        }
        if (intStatus == 0) {
            return ZERO_COUNT;
        }

        return FormatUtils.formatCount(intStatus);
    }

    public static String formatDataSize(final Long longStatus) {
        if (longStatus == null) {
            return EMPTY_BYTES;
        }
        if (longStatus == 0L) {
            return ZERO_BYTES;
        }

        return FormatUtils.formatDataSize(longStatus);
    }

    public static String prettyPrint(final Integer count, final Long bytes) {
        if (count != null && bytes != null && count == 0 && bytes == 0L) {
            return ZERO_COUNT_AND_BYTES;
        }

        return formatCount(count) + " (" + formatDataSize(bytes) + ")";
    }

}
