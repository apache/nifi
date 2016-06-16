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
package org.apache.nifi.controller.status;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class ProcessGroupStatus implements Cloneable {

    private String id;
    private String name;
    private Integer inputCount;
    private Long inputContentSize;
    private Integer outputCount;
    private Long outputContentSize;
    private Integer activeThreadCount;
    private Integer queuedCount;
    private Long queuedContentSize;
    private Long bytesRead;
    private Long bytesWritten;
    private int flowFilesReceived;
    private long bytesReceived;
    private int flowFilesSent;
    private long bytesSent;
    private int flowFilesTransferred;
    private long bytesTransferred;

    private Collection<ConnectionStatus> connectionStatus = new ArrayList<>();
    private Collection<ProcessorStatus> processorStatus = new ArrayList<>();
    private Collection<ProcessGroupStatus> processGroupStatus = new ArrayList<>();
    private Collection<RemoteProcessGroupStatus> remoteProcessGroupStatus = new ArrayList<>();
    private Collection<PortStatus> inputPortStatus = new ArrayList<>();
    private Collection<PortStatus> outputPortStatus = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getInputCount() {
        return inputCount;
    }

    public void setInputCount(final Integer inputCount) {
        this.inputCount = inputCount;
    }

    public Long getInputContentSize() {
        return inputContentSize;
    }

    public void setInputContentSize(final Long inputContentSize) {
        this.inputContentSize = inputContentSize;
    }

    public Integer getOutputCount() {
        return outputCount;
    }

    public void setOutputCount(final Integer outputCount) {
        this.outputCount = outputCount;
    }

    public Long getOutputContentSize() {
        return outputContentSize;
    }

    public void setOutputContentSize(final Long outputContentSize) {
        this.outputContentSize = outputContentSize;
    }

    public Long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(final Long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public Long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(final Long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public Integer getQueuedCount() {
        return this.queuedCount;
    }

    public void setQueuedCount(final Integer queuedCount) {
        this.queuedCount = queuedCount;
    }

    public Long getQueuedContentSize() {
        return queuedContentSize;
    }

    public void setQueuedContentSize(final Long queuedContentSize) {
        this.queuedContentSize = queuedContentSize;
    }

    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(final Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    public Collection<ConnectionStatus> getConnectionStatus() {
        return connectionStatus;
    }

    public void setConnectionStatus(final Collection<ConnectionStatus> connectionStatus) {
        this.connectionStatus = connectionStatus;
    }

    public Collection<ProcessorStatus> getProcessorStatus() {
        return processorStatus;
    }

    public void setProcessorStatus(final Collection<ProcessorStatus> processorStatus) {
        this.processorStatus = processorStatus;
    }

    public Collection<ProcessGroupStatus> getProcessGroupStatus() {
        return processGroupStatus;
    }

    public void setProcessGroupStatus(final Collection<ProcessGroupStatus> processGroupStatus) {
        this.processGroupStatus = processGroupStatus;
    }

    public Collection<PortStatus> getInputPortStatus() {
        return inputPortStatus;
    }

    public void setInputPortStatus(Collection<PortStatus> inputPortStatus) {
        this.inputPortStatus = inputPortStatus;
    }

    public Collection<PortStatus> getOutputPortStatus() {
        return outputPortStatus;
    }

    public void setOutputPortStatus(Collection<PortStatus> outputPortStatus) {
        this.outputPortStatus = outputPortStatus;
    }

    public Collection<RemoteProcessGroupStatus> getRemoteProcessGroupStatus() {
        return remoteProcessGroupStatus;
    }

    public void setRemoteProcessGroupStatus(final Collection<RemoteProcessGroupStatus> remoteProcessGroupStatus) {
        this.remoteProcessGroupStatus = remoteProcessGroupStatus;
    }

    public int getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public void setFlowFilesReceived(final int flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
    }

    public long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(final long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    public int getFlowFilesSent() {
        return flowFilesSent;
    }

    public void setFlowFilesSent(final int flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(final long bytesSent) {
        this.bytesSent = bytesSent;
    }

    public int getFlowFilesTransferred() {
        return flowFilesTransferred;
    }

    public void setFlowFilesTransferred(int flowFilesTransferred) {
        this.flowFilesTransferred = flowFilesTransferred;
    }

    public long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    @Override
    public ProcessGroupStatus clone() {

        final ProcessGroupStatus clonedObj = new ProcessGroupStatus();

        clonedObj.id = id;
        clonedObj.name = name;
        clonedObj.outputContentSize = outputContentSize;
        clonedObj.outputCount = outputCount;
        clonedObj.inputContentSize = inputContentSize;
        clonedObj.inputCount = inputCount;
        clonedObj.activeThreadCount = activeThreadCount;
        clonedObj.queuedContentSize = queuedContentSize;
        clonedObj.queuedCount = queuedCount;
        clonedObj.bytesRead = bytesRead;
        clonedObj.bytesWritten = bytesWritten;
        clonedObj.flowFilesReceived = flowFilesReceived;
        clonedObj.bytesReceived = bytesReceived;
        clonedObj.flowFilesSent = flowFilesSent;
        clonedObj.bytesSent = bytesSent;
        clonedObj.flowFilesTransferred = flowFilesTransferred;
        clonedObj.bytesTransferred = bytesTransferred;

        if (connectionStatus != null) {
            final Collection<ConnectionStatus> statusList = new ArrayList<>();
            clonedObj.setConnectionStatus(statusList);
            for (final ConnectionStatus status : connectionStatus) {
                statusList.add(status.clone());
            }
        }

        if (processorStatus != null) {
            final Collection<ProcessorStatus> statusList = new ArrayList<>();
            clonedObj.setProcessorStatus(statusList);
            for (final ProcessorStatus status : processorStatus) {
                statusList.add(status.clone());
            }
        }

        if (inputPortStatus != null) {
            final Collection<PortStatus> statusList = new ArrayList<>();
            clonedObj.setInputPortStatus(statusList);
            for (final PortStatus status : inputPortStatus) {
                statusList.add(status.clone());
            }
        }

        if (outputPortStatus != null) {
            final Collection<PortStatus> statusList = new ArrayList<>();
            clonedObj.setOutputPortStatus(statusList);
            for (final PortStatus status : outputPortStatus) {
                statusList.add(status.clone());
            }
        }

        if (processGroupStatus != null) {
            final Collection<ProcessGroupStatus> statusList = new ArrayList<>();
            clonedObj.setProcessGroupStatus(statusList);
            for (final ProcessGroupStatus status : processGroupStatus) {
                statusList.add(status.clone());
            }
        }

        if (remoteProcessGroupStatus != null) {
            final Collection<RemoteProcessGroupStatus> statusList = new ArrayList<>();
            clonedObj.setRemoteProcessGroupStatus(statusList);
            for (final RemoteProcessGroupStatus status : remoteProcessGroupStatus) {
                statusList.add(status.clone());
            }
        }

        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProcessGroupStatus [id=");
        builder.append(id);
        builder.append(", inputCount=");
        builder.append(inputCount);
        builder.append(", inputBytes=");
        builder.append(inputContentSize);
        builder.append(", outputCount=");
        builder.append(outputCount);
        builder.append(", outputBytes=");
        builder.append(outputContentSize);
        builder.append(", activeThreadCount=");
        builder.append(activeThreadCount);
        builder.append(", flowFilesTransferred=");
        builder.append(flowFilesTransferred);
        builder.append(", bytesTransferred=");
        builder.append(bytesTransferred);
        builder.append(", flowFilesReceived=");
        builder.append(flowFilesReceived);
        builder.append(", bytesReceived=");
        builder.append(bytesReceived);
        builder.append(", flowFilesSent=");
        builder.append(flowFilesSent);
        builder.append(", bytesSent=");
        builder.append(bytesSent);
        builder.append(",\n\tconnectionStatus=");

        for (final ConnectionStatus status : connectionStatus) {
            builder.append("\n\t\t");
            builder.append(status);
        }

        builder.append(",\n\tprocessorStatus=");

        for (final ProcessorStatus status : processorStatus) {
            builder.append("\n\t\t");
            builder.append(status);
        }

        builder.append(",\n\tprocessGroupStatus=");

        for (final ProcessGroupStatus status : processGroupStatus) {
            builder.append("\n\t\t");
            builder.append(status);
        }

        builder.append(",\n\tremoteProcessGroupStatus=");
        for (final RemoteProcessGroupStatus status : remoteProcessGroupStatus) {
            builder.append("\n\t\t");
            builder.append(status);
        }

        builder.append(",\n\tinputPortStatus=");
        for (final PortStatus status : inputPortStatus) {
            builder.append("\n\t\t");
            builder.append(status);
        }

        builder.append(",\n\toutputPortStatus=");
        for (final PortStatus status : outputPortStatus) {
            builder.append("\n\t\t");
            builder.append(status);
        }

        builder.append("]");
        return builder.toString();
    }

    public static void merge(final ProcessGroupStatus target, final ProcessGroupStatus toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setInputCount(target.getInputCount() + toMerge.getInputCount());
        target.setInputContentSize(target.getInputContentSize() + toMerge.getInputContentSize());
        target.setOutputCount(target.getOutputCount() + toMerge.getOutputCount());
        target.setOutputContentSize(target.getOutputContentSize() + toMerge.getOutputContentSize());
        target.setQueuedCount(target.getQueuedCount() + toMerge.getQueuedCount());
        target.setQueuedContentSize(target.getQueuedContentSize() + toMerge.getQueuedContentSize());
        target.setBytesRead(target.getBytesRead() + toMerge.getBytesRead());
        target.setBytesWritten(target.getBytesWritten() + toMerge.getBytesWritten());
        target.setActiveThreadCount(target.getActiveThreadCount() + toMerge.getActiveThreadCount());
        target.setFlowFilesTransferred(target.getFlowFilesTransferred() + toMerge.getFlowFilesTransferred());
        target.setBytesTransferred(target.getBytesTransferred() + toMerge.getBytesTransferred());
        target.setFlowFilesReceived(target.getFlowFilesReceived() + toMerge.getFlowFilesReceived());
        target.setBytesReceived(target.getBytesReceived() + toMerge.getBytesReceived());
        target.setFlowFilesSent(target.getFlowFilesSent() + toMerge.getFlowFilesSent());
        target.setBytesSent(target.getBytesSent() + toMerge.getBytesSent());

        // connection status
        // sort by id
        final Map<String, ConnectionStatus> mergedConnectionMap = new HashMap<>();
        for (final ConnectionStatus status : target.getConnectionStatus()) {
            mergedConnectionMap.put(status.getId(), status);
        }

        for (final ConnectionStatus statusToMerge : toMerge.getConnectionStatus()) {
            ConnectionStatus merged = mergedConnectionMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedConnectionMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merged.setQueuedCount(merged.getQueuedCount() + statusToMerge.getQueuedCount());
            merged.setQueuedBytes(merged.getQueuedBytes() + statusToMerge.getQueuedBytes());
            merged.setInputCount(merged.getInputCount() + statusToMerge.getInputCount());
            merged.setInputBytes(merged.getInputBytes() + statusToMerge.getInputBytes());
            merged.setOutputCount(merged.getOutputCount() + statusToMerge.getOutputCount());
            merged.setOutputBytes(merged.getOutputBytes() + statusToMerge.getOutputBytes());
        }
        target.setConnectionStatus(mergedConnectionMap.values());

        // processor status
        final Map<String, ProcessorStatus> mergedProcessorMap = new HashMap<>();
        for (final ProcessorStatus status : target.getProcessorStatus()) {
            mergedProcessorMap.put(status.getId(), status);
        }

        for (final ProcessorStatus statusToMerge : toMerge.getProcessorStatus()) {
            ProcessorStatus merged = mergedProcessorMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedProcessorMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merged.setActiveThreadCount(merged.getActiveThreadCount() + statusToMerge.getActiveThreadCount());
            merged.setBytesRead(merged.getBytesRead() + statusToMerge.getBytesRead());
            merged.setBytesWritten(merged.getBytesWritten() + statusToMerge.getBytesWritten());
            merged.setInputBytes(merged.getInputBytes() + statusToMerge.getInputBytes());
            merged.setInputCount(merged.getInputCount() + statusToMerge.getInputCount());
            merged.setInvocations(merged.getInvocations() + statusToMerge.getInvocations());
            merged.setOutputBytes(merged.getOutputBytes() + statusToMerge.getOutputBytes());
            merged.setOutputCount(merged.getOutputCount() + statusToMerge.getOutputCount());
            merged.setProcessingNanos(merged.getProcessingNanos() + statusToMerge.getProcessingNanos());
            merged.setFlowFilesRemoved(merged.getFlowFilesRemoved() + statusToMerge.getFlowFilesRemoved());

            // if the status to merge is invalid allow it to take precedence. whether the
            // processor run status is disabled/stopped/running is part of the flow configuration
            // and should not differ amongst nodes. however, whether a processor is invalid
            // can be driven by environmental conditions. this check allows any of those to
            // take precedence over the configured run status.
            if (RunStatus.Invalid.equals(statusToMerge.getRunStatus())) {
                merged.setRunStatus(RunStatus.Invalid);
            }
        }
        target.setProcessorStatus(mergedProcessorMap.values());

        // input ports
        final Map<String, PortStatus> mergedInputPortMap = new HashMap<>();
        for (final PortStatus status : target.getInputPortStatus()) {
            mergedInputPortMap.put(status.getId(), status);
        }

        for (final PortStatus statusToMerge : toMerge.getInputPortStatus()) {
            PortStatus merged = mergedInputPortMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedInputPortMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merged.setInputBytes(merged.getInputBytes() + statusToMerge.getInputBytes());
            merged.setInputCount(merged.getInputCount() + statusToMerge.getInputCount());
            merged.setOutputBytes(merged.getOutputBytes() + statusToMerge.getOutputBytes());
            merged.setOutputCount(merged.getOutputCount() + statusToMerge.getOutputCount());
            merged.setActiveThreadCount(merged.getActiveThreadCount() + statusToMerge.getActiveThreadCount());
            if (statusToMerge.isTransmitting() != null && statusToMerge.isTransmitting()) {
                merged.setTransmitting(true);
            }

            // should be unnecessary here since ports run status should not be affected by
            // environmental conditions but doing so in case that changes
            if (RunStatus.Invalid.equals(statusToMerge.getRunStatus())) {
                merged.setRunStatus(RunStatus.Invalid);
            }
        }
        target.setInputPortStatus(mergedInputPortMap.values());

        // output ports
        final Map<String, PortStatus> mergedOutputPortMap = new HashMap<>();
        for (final PortStatus status : target.getOutputPortStatus()) {
            mergedOutputPortMap.put(status.getId(), status);
        }

        for (final PortStatus statusToMerge : toMerge.getOutputPortStatus()) {
            PortStatus merged = mergedOutputPortMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedOutputPortMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merged.setInputBytes(merged.getInputBytes() + statusToMerge.getInputBytes());
            merged.setInputCount(merged.getInputCount() + statusToMerge.getInputCount());
            merged.setOutputBytes(merged.getOutputBytes() + statusToMerge.getOutputBytes());
            merged.setOutputCount(merged.getOutputCount() + statusToMerge.getOutputCount());
            merged.setActiveThreadCount(merged.getActiveThreadCount() + statusToMerge.getActiveThreadCount());
            if (statusToMerge.isTransmitting() != null && statusToMerge.isTransmitting()) {
                merged.setTransmitting(true);
            }

            // should be unnecessary here since ports run status not should be affected by
            // environmental conditions but doing so in case that changes
            if (RunStatus.Invalid.equals(statusToMerge.getRunStatus())) {
                merged.setRunStatus(RunStatus.Invalid);
            }
        }
        target.setOutputPortStatus(mergedOutputPortMap.values());

        // child groups
        final Map<String, ProcessGroupStatus> mergedGroupMap = new HashMap<>();
        for (final ProcessGroupStatus status : target.getProcessGroupStatus()) {
            mergedGroupMap.put(status.getId(), status);
        }

        for (final ProcessGroupStatus statusToMerge : toMerge.getProcessGroupStatus()) {
            ProcessGroupStatus merged = mergedGroupMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedGroupMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            merge(merged, statusToMerge);
        }
        target.setOutputPortStatus(mergedOutputPortMap.values());

        // remote groups
        final Map<String, RemoteProcessGroupStatus> mergedRemoteGroupMap = new HashMap<>();
        for (final RemoteProcessGroupStatus status : target.getRemoteProcessGroupStatus()) {
            mergedRemoteGroupMap.put(status.getId(), status);
        }

        for (final RemoteProcessGroupStatus statusToMerge : toMerge.getRemoteProcessGroupStatus()) {
            RemoteProcessGroupStatus merged = mergedRemoteGroupMap.get(statusToMerge.getId());
            if (merged == null) {
                mergedRemoteGroupMap.put(statusToMerge.getId(), statusToMerge.clone());
                continue;
            }

            // NOTE - active/inactive port counts are not merged since that state is considered part of the flow (like runStatus)
            merged.setReceivedContentSize(merged.getReceivedContentSize() + statusToMerge.getReceivedContentSize());
            merged.setReceivedCount(merged.getReceivedCount() + statusToMerge.getReceivedCount());
            merged.setSentContentSize(merged.getSentContentSize() + statusToMerge.getSentContentSize());
            merged.setSentCount(merged.getSentCount() + statusToMerge.getSentCount());
            merged.setActiveThreadCount(merged.getActiveThreadCount() + statusToMerge.getActiveThreadCount());
        }

        target.setRemoteProcessGroupStatus(mergedRemoteGroupMap.values());
    }
}
