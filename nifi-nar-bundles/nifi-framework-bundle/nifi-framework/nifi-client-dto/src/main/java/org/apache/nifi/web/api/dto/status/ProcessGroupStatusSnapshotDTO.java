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
package org.apache.nifi.web.api.dto.status;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.entity.ConnectionStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.PortStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusSnapshotEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The status for a process group in this NiFi.
 */
@XmlType(name = "processGroupStatusSnapshot")
public class ProcessGroupStatusSnapshotDTO implements Cloneable {

    private String id;
    private String name;
    private Collection<ConnectionStatusSnapshotEntity> connectionStatusSnapshots;
    private Collection<ProcessorStatusSnapshotEntity> processorStatusSnapshots;
    private Collection<ProcessGroupStatusSnapshotEntity> processGroupStatusSnapshots;
    private Collection<RemoteProcessGroupStatusSnapshotEntity> remoteProcessGroupStatusSnapshots;
    private Collection<PortStatusSnapshotEntity> inputPortStatusSnapshots;
    private Collection<PortStatusSnapshotEntity> outputPortStatusSnapshots;

    private Integer flowFilesIn = 0;
    private Long bytesIn = 0L;
    private String input;

    private Integer flowFilesQueued = 0;
    private Long bytesQueued = 0L;
    private String queued;
    private String queuedCount;
    private String queuedSize;

    private Long bytesRead = 0L;
    private String read;
    private Long bytesWritten = 0L;
    private String written;

    private Integer flowFilesOut = 0;
    private Long bytesOut = 0L;
    private String output;

    private Integer flowFilesTransferred = 0;
    private Long bytesTransferred = 0L;
    private String transferred;

    private Long bytesReceived = 0L;
    private Integer flowFilesReceived = 0;
    private String received;

    private Long bytesSent = 0L;
    private Integer flowFilesSent = 0;
    private String sent;

    private Integer activeThreadCount = 0;

    /**
     * The id for the process group.
     *
     * @return The id for the process group
     */
    @ApiModelProperty("The id of the process group.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return name of this process group
     */
    @ApiModelProperty("The name of this process group.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return active thread count for this process group
     */
    @ApiModelProperty("The active thread count for this process group.")
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * The status of all connections in this process group.
     *
     * @return The status of all connections
     */
    @ApiModelProperty("The status of all conenctions in the process group.")
    public Collection<ConnectionStatusSnapshotEntity> getConnectionStatusSnapshots() {
        return connectionStatusSnapshots;
    }

    public void setConnectionStatusSnapshots(Collection<ConnectionStatusSnapshotEntity> connectionStatus) {
        this.connectionStatusSnapshots = connectionStatus;
    }

    /**
     * The status of all process groups in this process group.
     *
     * @return The status of all process groups
     */
    @ApiModelProperty("The status of all process groups in the process group.")
    public Collection<ProcessGroupStatusSnapshotEntity> getProcessGroupStatusSnapshots() {
        return processGroupStatusSnapshots;
    }

    public void setProcessGroupStatusSnapshots(Collection<ProcessGroupStatusSnapshotEntity> processGroupStatus) {
        this.processGroupStatusSnapshots = processGroupStatus;
    }

    /**
     * The status of all remote process groups in this process group.
     *
     * @return The status of all remote process groups
     */
    @ApiModelProperty("The status of all remote process groups in the process group.")
    public Collection<RemoteProcessGroupStatusSnapshotEntity> getRemoteProcessGroupStatusSnapshots() {
        return remoteProcessGroupStatusSnapshots;
    }

    public void setRemoteProcessGroupStatusSnapshots(final Collection<RemoteProcessGroupStatusSnapshotEntity> remoteProcessGroupStatus) {
        this.remoteProcessGroupStatusSnapshots = remoteProcessGroupStatus;
    }

    /**
     * The status of all processors in this process group.
     *
     * @return The status of all processors
     */
    @ApiModelProperty("The status of all processors in the process group.")
    public Collection<ProcessorStatusSnapshotEntity> getProcessorStatusSnapshots() {
        return processorStatusSnapshots;
    }

    public void setProcessorStatusSnapshots(Collection<ProcessorStatusSnapshotEntity> processorStatus) {
        this.processorStatusSnapshots = processorStatus;
    }

    /**
     * The status of all input ports in this process group.
     *
     * @return The status of all input ports
     */
    @ApiModelProperty("The status of all input ports in the process group.")
    public Collection<PortStatusSnapshotEntity> getInputPortStatusSnapshots() {
        return inputPortStatusSnapshots;
    }

    public void setInputPortStatusSnapshots(Collection<PortStatusSnapshotEntity> inputPortStatus) {
        this.inputPortStatusSnapshots = inputPortStatus;
    }

    /**
     * The status of all output ports in this process group.
     *
     * @return The status of all output ports
     */
    @ApiModelProperty("The status of all output ports in the process group.")
    public Collection<PortStatusSnapshotEntity> getOutputPortStatusSnapshots() {
        return outputPortStatusSnapshots;
    }

    public void setOutputPortStatusSnapshots(Collection<PortStatusSnapshotEntity> outputPortStatus) {
        this.outputPortStatusSnapshots = outputPortStatus;
    }

    /**
     * The output stats for this process group.
     *
     * @return The output stats
     */
    @ApiModelProperty("The output count/size for the process group in the last 5 minutes.")
    public String getOutput() {
        return output;
    }

    /**
     * The transferred stats for this process group. This represents the count/size of flowfiles transferred to/from queues.
     *
     * @return The transferred status for this process group
     */
    @ApiModelProperty("The count/size transferred to/from queues in the process group in the last 5 minutes.")
    public String getTransferred() {
        return transferred;
    }

    /**
     * The received stats for this process group. This represents the count/size of flowfiles received.
     *
     * @return The received stats for this process group
     */
    @ApiModelProperty("The count/size sent to the process group in the last 5 minutes.")
    public String getReceived() {
        return received;
    }


    /**
     * The sent stats for this process group. This represents the count/size of flowfiles sent.
     *
     * @return The sent stats for this process group
     */
    @ApiModelProperty("The count/size sent from this process group in the last 5 minutes.")
    public String getSent() {
        return sent;
    }


    /**
     * The queued count for this process group.
     *
     * @return The queued count for this process group
     */
    @ApiModelProperty("The count that is queued for the process group.")
    public String getQueuedCount() {
        return queuedCount;
    }


    /**
     * The queued size for this process group.
     *
     * @return The queued size for this process group
     */
    @ApiModelProperty("The size that is queued for the process group.")
    public String getQueuedSize() {
        return queuedSize;
    }


    /**
     * The queued stats for this process group.
     *
     * @return The queued stats
     */
    @ApiModelProperty("The count/size that is queued in the the process group.")
    public String getQueued() {
        return queued;
    }


    /**
     * The read stats for this process group.
     *
     * @return The read stats
     */
    @ApiModelProperty("The number of bytes read in the last 5 minutes.")
    public String getRead() {
        return read;
    }


    /**
     * The written stats for this process group.
     *
     * @return The written stats
     */
    @ApiModelProperty("The number of bytes written in the last 5 minutes.")
    public String getWritten() {
        return written;
    }


    /**
     * The input stats for this process group.
     *
     * @return The input stats
     */
    @ApiModelProperty("The input count/size for the process group in the last 5 minutes (pretty printed).")
    public String getInput() {
        return input;
    }


    @ApiModelProperty("The number of FlowFiles that have come into this ProcessGroup in the last 5 minutes")
    public Integer getFlowFilesIn() {
        return flowFilesIn;
    }

    public void setFlowFilesIn(Integer flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
    }

    @ApiModelProperty("The number of bytes that have come into this ProcessGroup in the last 5 minutes")
    public Long getBytesIn() {
        return bytesIn;
    }

    public void setBytesIn(Long bytesIn) {
        this.bytesIn = bytesIn;
    }

    @ApiModelProperty("The number of FlowFiles that are queued up in this ProcessGroup right now")
    public Integer getFlowFilesQueued() {
        return flowFilesQueued;
    }

    public void setFlowFilesQueued(Integer flowFilesQueued) {
        this.flowFilesQueued = flowFilesQueued;
    }

    @ApiModelProperty("The number of bytes that are queued up in this ProcessGroup right now")
    public Long getBytesQueued() {
        return bytesQueued;
    }

    public void setBytesQueued(Long bytesQueued) {
        this.bytesQueued = bytesQueued;
    }

    @ApiModelProperty("The number of bytes read by components in this ProcessGroup in the last 5 minutes")
    public Long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(Long bytesRead) {
        this.bytesRead = bytesRead;
    }

    @ApiModelProperty("The number of bytes written by components in this ProcessGroup in the last 5 minutes")
    public Long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(Long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    @ApiModelProperty("The number of FlowFiles transferred out of this ProcessGroup in the last 5 minutes")
    public Integer getFlowFilesOut() {
        return flowFilesOut;
    }

    public void setFlowFilesOut(Integer flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
    }

    @ApiModelProperty("The number of bytes transferred out of this ProcessGroup in the last 5 minutes")
    public Long getBytesOut() {
        return bytesOut;
    }

    public void setBytesOut(Long bytesOut) {
        this.bytesOut = bytesOut;
    }

    @ApiModelProperty("The number of FlowFiles transferred in this ProcessGroup in the last 5 minutes")
    public Integer getFlowFilesTransferred() {
        return flowFilesTransferred;
    }

    public void setFlowFilesTransferred(Integer flowFilesTransferred) {
        this.flowFilesTransferred = flowFilesTransferred;
    }

    @ApiModelProperty("The number of bytes transferred in this ProcessGroup in the last 5 minutes")
    public Long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(Long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    @ApiModelProperty("The number of bytes received from external sources by components within this ProcessGroup in the last 5 minutes")
    public Long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(Long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    @ApiModelProperty("The number of bytes sent to an external sink by components within this ProcessGroup in the last 5 minutes")
    public Long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(Long bytesSent) {
        this.bytesSent = bytesSent;
    }

    @ApiModelProperty("The number of FlowFiles sent to an external sink by components within this ProcessGroup in the last 5 minutes")
    public Integer getFlowFilesSent() {
        return flowFilesSent;
    }

    public void setFlowFilesSent(Integer flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
    }

    @ApiModelProperty("The number of FlowFiles received from external sources by components within this ProcessGroup in the last 5 minutes")
    public Integer getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public void setFlowFilesReceived(Integer flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    public void setQueuedCount(String queuedCount) {
        this.queuedCount = queuedCount;
    }

    public void setQueuedSize(String queuedSize) {
        this.queuedSize = queuedSize;
    }

    public void setRead(String read) {
        this.read = read;
    }

    public void setWritten(String written) {
        this.written = written;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public void setTransferred(String transferred) {
        this.transferred = transferred;
    }

    public void setReceived(String received) {
        this.received = received;
    }

    public void setSent(String sent) {
        this.sent = sent;
    }

    @Override
    public ProcessGroupStatusSnapshotDTO clone() {
        final ProcessGroupStatusSnapshotDTO other = new ProcessGroupStatusSnapshotDTO();
        other.setId(getId());
        other.setName(getName());

        other.setBytesIn(getBytesIn());
        other.setFlowFilesIn(getFlowFilesIn());
        other.setInput(getInput());

        other.setBytesQueued(getBytesQueued());
        other.setFlowFilesQueued(getFlowFilesQueued());
        other.setQueued(getQueued());
        other.setQueuedCount(getQueuedCount());
        other.setQueuedSize(getQueuedSize());

        other.setBytesRead(getBytesRead());
        other.setRead(getRead());
        other.setBytesWritten(getBytesWritten());
        other.setWritten(getWritten());

        other.setBytesOut(getBytesOut());
        other.setFlowFilesOut(getFlowFilesOut());
        other.setOutput(getOutput());

        other.setBytesTransferred(getBytesTransferred());
        other.setFlowFilesTransferred(getFlowFilesTransferred());
        other.setTransferred(getTransferred());

        other.setBytesReceived(getBytesReceived());
        other.setFlowFilesReceived(getFlowFilesReceived());
        other.setReceived(getReceived());
        other.setBytesSent(getBytesSent());
        other.setFlowFilesSent(getFlowFilesSent());
        other.setSent(getSent());

        other.setActiveThreadCount(getActiveThreadCount());

        other.setConnectionStatusSnapshots(copy(getConnectionStatusSnapshots()));
        other.setProcessorStatusSnapshots(copy(getProcessorStatusSnapshots()));
        other.setRemoteProcessGroupStatusSnapshots(copy(getRemoteProcessGroupStatusSnapshots()));
        other.setInputPortStatusSnapshots(copy(getInputPortStatusSnapshots()));
        other.setOutputPortStatusSnapshots(copy(getOutputPortStatusSnapshots()));

        if (processGroupStatusSnapshots != null) {
            final List<ProcessGroupStatusSnapshotEntity> childGroups = new ArrayList<>();
            for (final ProcessGroupStatusSnapshotEntity procGroupStatus : processGroupStatusSnapshots) {
                childGroups.add(procGroupStatus.clone());
            }
            other.setProcessGroupStatusSnapshots(childGroups);
        }

        return other;
    }

    private <T> Collection<T> copy(final Collection<T> original) {
        if (original == null) {
            return null;
        }

        return new ArrayList<T>(original);
    }
}
