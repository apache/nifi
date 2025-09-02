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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the status snapshot of a connector.
 */
@XmlType(name = "connectorStatusSnapshot")
public class ConnectorStatusSnapshotDTO implements Cloneable {

    private String id;
    private String groupId;
    private String name;
    private String type;
    private String runStatus;

    private Integer flowFilesSent = 0;
    private Long bytesSent = 0L;
    private Integer flowFilesReceived = 0;
    private Long bytesReceived = 0L;
    private Long bytesRead = 0L;
    private Long bytesWritten = 0L;
    private String sent;
    private String received;
    private String read;
    private String written;

    private Integer flowFilesQueued = 0;
    private Long bytesQueued = 0L;
    private String queued;
    private String queuedCount;
    private String queuedSize;

    private Integer activeThreadCount = 0;

    private ProcessingPerformanceStatusDTO processingPerformanceStatus;

    private Boolean idle;
    private Long idleDurationMillis;
    private String idleDuration;

    @Schema(description = "The id of the connector.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Schema(description = "The id of the parent process group to which the connector belongs.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    @Schema(description = "The name of the connector.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Schema(description = "The type of the connector.")
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    @Schema(description = "The run status of the connector.")
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(final String runStatus) {
        this.runStatus = runStatus;
    }

    @Schema(description = "The number of FlowFiles sent by this connector's managed process group.")
    public Integer getFlowFilesSent() {
        return flowFilesSent;
    }

    public void setFlowFilesSent(final Integer flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
    }

    @Schema(description = "The number of bytes sent by this connector's managed process group.")
    public Long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(final Long bytesSent) {
        this.bytesSent = bytesSent;
    }

    @Schema(description = "The number of FlowFiles received by this connector's managed process group.")
    public Integer getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public void setFlowFilesReceived(final Integer flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
    }

    @Schema(description = "The number of bytes received by this connector's managed process group.")
    public Long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(final Long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    @Schema(description = "The number of bytes read by processors in this connector's managed process group.")
    public Long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(final Long bytesRead) {
        this.bytesRead = bytesRead;
    }

    @Schema(description = "The number of bytes written by processors in this connector's managed process group.")
    public Long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(final Long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    @Schema(description = "The count/size of data that has been sent by this connector, pretty-printed.")
    public String getSent() {
        return sent;
    }

    public void setSent(final String sent) {
        this.sent = sent;
    }

    @Schema(description = "The count/size of data that has been received by this connector, pretty-printed.")
    public String getReceived() {
        return received;
    }

    public void setReceived(final String received) {
        this.received = received;
    }

    @Schema(description = "The number of bytes read, pretty-printed.")
    public String getRead() {
        return read;
    }

    public void setRead(final String read) {
        this.read = read;
    }

    @Schema(description = "The number of bytes written, pretty-printed.")
    public String getWritten() {
        return written;
    }

    public void setWritten(final String written) {
        this.written = written;
    }

    @Schema(description = "The number of FlowFiles queued in this connector's managed process group.")
    public Integer getFlowFilesQueued() {
        return flowFilesQueued;
    }

    public void setFlowFilesQueued(final Integer flowFilesQueued) {
        this.flowFilesQueued = flowFilesQueued;
    }

    @Schema(description = "The number of bytes queued in this connector's managed process group.")
    public Long getBytesQueued() {
        return bytesQueued;
    }

    public void setBytesQueued(final Long bytesQueued) {
        this.bytesQueued = bytesQueued;
    }

    @Schema(description = "The count/size of queued data, pretty-printed.")
    public String getQueued() {
        return queued;
    }

    public void setQueued(final String queued) {
        this.queued = queued;
    }

    @Schema(description = "The count of queued FlowFiles, pretty-printed.")
    public String getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(final String queuedCount) {
        this.queuedCount = queuedCount;
    }

    @Schema(description = "The size of queued data, pretty-printed.")
    public String getQueuedSize() {
        return queuedSize;
    }

    public void setQueuedSize(final String queuedSize) {
        this.queuedSize = queuedSize;
    }

    @Schema(description = "The number of active threads for the connector.")
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(final Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    @Schema(description = "The processing performance status of the processors in this connector's managed process group.")
    public ProcessingPerformanceStatusDTO getProcessingPerformanceStatus() {
        return processingPerformanceStatus;
    }

    public void setProcessingPerformanceStatus(final ProcessingPerformanceStatusDTO processingPerformanceStatus) {
        this.processingPerformanceStatus = processingPerformanceStatus;
    }

    @Schema(description = "Whether or not the connector is currently idle (no FlowFiles queued and no FlowFiles processed recently).")
    public Boolean getIdle() {
        return idle;
    }

    public void setIdle(final Boolean idle) {
        this.idle = idle;
    }

    @Schema(description = "The number of milliseconds the connector has been idle, or null if the connector is not idle.")
    public Long getIdleDurationMillis() {
        return idleDurationMillis;
    }

    public void setIdleDurationMillis(final Long idleDurationMillis) {
        this.idleDurationMillis = idleDurationMillis;
    }

    @Schema(description = "A human-readable representation of how long the connector has been idle, or null if the connector is not idle.")
    public String getIdleDuration() {
        return idleDuration;
    }

    public void setIdleDuration(final String idleDuration) {
        this.idleDuration = idleDuration;
    }

    @Override
    public ConnectorStatusSnapshotDTO clone() {
        final ConnectorStatusSnapshotDTO other = new ConnectorStatusSnapshotDTO();
        other.setId(getId());
        other.setGroupId(getGroupId());
        other.setName(getName());
        other.setType(getType());
        other.setRunStatus(getRunStatus());

        other.setFlowFilesSent(getFlowFilesSent());
        other.setBytesSent(getBytesSent());
        other.setFlowFilesReceived(getFlowFilesReceived());
        other.setBytesReceived(getBytesReceived());
        other.setBytesRead(getBytesRead());
        other.setBytesWritten(getBytesWritten());
        other.setSent(getSent());
        other.setReceived(getReceived());
        other.setRead(getRead());
        other.setWritten(getWritten());

        other.setFlowFilesQueued(getFlowFilesQueued());
        other.setBytesQueued(getBytesQueued());
        other.setQueued(getQueued());
        other.setQueuedCount(getQueuedCount());
        other.setQueuedSize(getQueuedSize());

        other.setActiveThreadCount(getActiveThreadCount());

        if (getProcessingPerformanceStatus() != null) {
            other.setProcessingPerformanceStatus(getProcessingPerformanceStatus().clone());
        }

        other.setIdle(getIdle());
        other.setIdleDurationMillis(getIdleDurationMillis());
        other.setIdleDuration(getIdleDuration());

        return other;
    }
}
