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

import javax.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the status of a connection.
 */
@XmlType(name = "connectionStatusSnapshot")
public class ConnectionStatusSnapshotDTO implements Cloneable {

    private String id;
    private String groupId;
    private String name;

    private String sourceId;
    private String sourceName;
    private String destinationId;
    private String destinationName;

    private Integer flowFilesIn = 0;
    private Long bytesIn = 0L;
    private String input;
    private Integer flowFilesOut = 0;
    private Long bytesOut = 0L;
    private String output;
    private Integer flowFilesQueued = 0;
    private Long bytesQueued = 0L;
    private String queued;
    private String queuedSize;
    private String queuedCount;
    private Integer percentUseCount;
    private Integer percentUseBytes;

    /* getters / setters */
    /**
     * @return The connection id
     */
    @ApiModelProperty("The id of the connection.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the ID of the Process Group to which this connection belongs.
     */
    @ApiModelProperty("The id of the process group the connection belongs to.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of this connection
     */
    @ApiModelProperty("The name of the connection.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return total count of flow files that are queued
     */
    @ApiModelProperty("The number of flowfiles that are queued, pretty printed.")
    public String getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(String queuedCount) {
        this.queuedCount = queuedCount;
    }


    /**
     * @return total size of flow files that are queued
     */
    @ApiModelProperty("The total size of flowfiles that are queued formatted.")
    public String getQueuedSize() {
        return queuedSize;
    }


    public void setInput(String input) {
        this.input = input;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    public void setQueuedSize(String queuedSize) {
        this.queuedSize = queuedSize;
    }

    /**
     * @return The total count and size of queued flow files
     */
    @ApiModelProperty("The total count and size of queued flowfiles formatted.")
    public String getQueued() {
        return queued;
    }


    /**
     * @return id of the source of this connection
     */
    @ApiModelProperty("The id of the source of the connection.")
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return name of the source of this connection
     */
    @ApiModelProperty("The name of the source of the connection.")
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    /**
     * @return id of the destination of this connection
     */
    @ApiModelProperty("The id of the destination of the connection.")
    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    /**
     * @return name of the destination of this connection
     */
    @ApiModelProperty("The name of the destination of the connection.")
    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    /**
     * @return input for this connection
     */
    @ApiModelProperty("The input count/size for the connection in the last 5 minutes, pretty printed.")
    public String getInput() {
        return input;
    }


    /**
     * @return output for this connection
     */
    @ApiModelProperty("The output count/sie for the connection in the last 5 minutes, pretty printed.")
    public String getOutput() {
        return output;
    }


    @ApiModelProperty("The number of FlowFiles that have come into the connection in the last 5 minutes.")
    public Integer getFlowFilesIn() {
        return flowFilesIn;
    }

    public void setFlowFilesIn(Integer flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
    }

    @ApiModelProperty("The size of the FlowFiles that have come into the connection in the last 5 minutes.")
    public Long getBytesIn() {
        return bytesIn;
    }

    public void setBytesIn(Long bytesIn) {
        this.bytesIn = bytesIn;
    }

    @ApiModelProperty("The number of FlowFiles that have left the connection in the last 5 minutes.")
    public Integer getFlowFilesOut() {
        return flowFilesOut;
    }

    public void setFlowFilesOut(Integer flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
    }

    @ApiModelProperty("The number of bytes that have left the connection in the last 5 minutes.")
    public Long getBytesOut() {
        return bytesOut;
    }

    public void setBytesOut(Long bytesOut) {
        this.bytesOut = bytesOut;
    }

    @ApiModelProperty("The number of FlowFiles that are currently queued in the connection.")
    public Integer getFlowFilesQueued() {
        return flowFilesQueued;
    }

    public void setFlowFilesQueued(Integer flowFilesQueued) {
        this.flowFilesQueued = flowFilesQueued;
    }

    @ApiModelProperty("The size of the FlowFiles that are currently queued in the connection.")
    public Long getBytesQueued() {
        return bytesQueued;
    }

    public void setBytesQueued(Long bytesQueued) {
        this.bytesQueued = bytesQueued;
    }

    @ApiModelProperty("Connection percent use regarding queued flow files count and backpressure threshold if configured.")
    public Integer getPercentUseCount() {
        return percentUseCount;
    }

    public void setPercentUseCount(Integer percentUseCount) {
        this.percentUseCount = percentUseCount;
    }

    @ApiModelProperty("Connection percent use regarding queued flow files size and backpressure threshold if configured.")
    public Integer getPercentUseBytes() {
        return percentUseBytes;
    }

    public void setPercentUseBytes(Integer percentUseBytes) {
        this.percentUseBytes = percentUseBytes;
    }

    @Override
    public ConnectionStatusSnapshotDTO clone() {
        final ConnectionStatusSnapshotDTO other = new ConnectionStatusSnapshotDTO();
        other.setDestinationId(getDestinationId());
        other.setDestinationName(getDestinationName());
        other.setGroupId(getGroupId());
        other.setId(getId());
        other.setName(getName());
        other.setSourceId(getSourceId());
        other.setSourceName(getSourceName());

        other.setFlowFilesIn(getFlowFilesIn());
        other.setBytesIn(getBytesIn());
        other.setInput(getInput());
        other.setFlowFilesOut(getFlowFilesOut());
        other.setBytesOut(getBytesOut());
        other.setOutput(getOutput());
        other.setFlowFilesQueued(getFlowFilesQueued());
        other.setBytesQueued(getBytesQueued());
        other.setQueued(getQueued());
        other.setQueuedCount(getQueuedCount());
        other.setQueuedSize(getQueuedSize());
        other.setPercentUseBytes(getPercentUseBytes());
        other.setPercentUseCount(getPercentUseCount());

        return other;
    }
}
