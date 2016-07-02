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
 * The status for a port in this NiFi.
 */
@XmlType(name = "portStatusSnapshot")
public class PortStatusSnapshotDTO implements Cloneable {

    private String id;
    private String groupId;
    private String name;

    private Integer activeThreadCount = 0;
    private Integer flowFilesIn = 0;
    private Long bytesIn = 0L;
    private String input;
    private Integer flowFilesOut = 0;
    private Long bytesOut = 0L;
    private String output;

    private Boolean transmitting;
    private String runStatus;

    /**
     * @return whether this port has incoming or outgoing connections to a remote NiFi
     */
    @ApiModelProperty("Whether the port has incoming or outgoing connections to a remote NiFi.")
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * @return the active thread count for this port
     */
    @ApiModelProperty("The active thread count for the port.")
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * @return id of this port
     */
    @ApiModelProperty("The id of the port.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return id of the group this port resides in
     */
    @ApiModelProperty("The id of the parent process group of the port.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of this port
     */
    @ApiModelProperty("The name of the port.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return run status of this port
     */
    @ApiModelProperty("The run status of the port.")
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    /**
     * @return The total count and size of flow files that have been accepted in the last five minutes
     */
    @ApiModelProperty("The count/size of flowfiles that have been accepted in the last 5 minutes.")
    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    /**
     * @return The total count and size of flow files that have been processed in the last five minutes
     */
    @ApiModelProperty("The count/size of flowfiles that have been processed in the last 5 minutes.")
    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    @ApiModelProperty("The number of FlowFiles that have been accepted in the last 5 minutes.")
    public Integer getFlowFilesIn() {
        return flowFilesIn;
    }

    public void setFlowFilesIn(Integer flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
    }

    @ApiModelProperty("The size of hte FlowFiles that have been accepted in the last 5 minutes.")
    public Long getBytesIn() {
        return bytesIn;
    }

    public void setBytesIn(Long bytesIn) {
        this.bytesIn = bytesIn;
    }

    @ApiModelProperty("The number of FlowFiles that have been processed in the last 5 minutes.")
    public Integer getFlowFilesOut() {
        return flowFilesOut;
    }

    public void setFlowFilesOut(Integer flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
    }

    @ApiModelProperty("The number of bytes that have been processed in the last 5 minutes.")
    public Long getBytesOut() {
        return bytesOut;
    }

    public void setBytesOut(Long bytesOut) {
        this.bytesOut = bytesOut;
    }

    @Override
    public PortStatusSnapshotDTO clone() {
        final PortStatusSnapshotDTO other = new PortStatusSnapshotDTO();
        other.setId(getId());
        other.setGroupId(getGroupId());
        other.setName(getName());
        other.setActiveThreadCount(getActiveThreadCount());
        other.setFlowFilesIn(getFlowFilesIn());
        other.setBytesIn(getBytesIn());
        other.setFlowFilesOut(getFlowFilesOut());
        other.setBytesOut(getBytesOut());
        other.setTransmitting(isTransmitting());
        other.setRunStatus(getRunStatus());
        other.setInput(getInput());
        other.setOutput(getOutput());

        return other;
    }
}
