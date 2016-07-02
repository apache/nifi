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
 * The status of a remote process group in this NiFi.
 */
@XmlType(name = "remoteProcessGroupStatusSnapshot")
public class RemoteProcessGroupStatusSnapshotDTO implements Cloneable {

    private String id;
    private String groupId;
    private String name;
    private String targetUri;
    private String transmissionStatus;
    private Integer activeThreadCount = 0;

    private Integer flowFilesSent = 0;
    private Long bytesSent = 0L;
    private String sent;

    private Integer flowFilesReceived = 0;
    private Long bytesReceived = 0L;
    private String received;

    /**
     * @return The id for the remote process group
     */
    @ApiModelProperty("The id of the remote process group.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return id of the group this remote process group is in
     */
    @ApiModelProperty("The id of the parent process group the remote process group resides in.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return URI of the target system
     */
    @ApiModelProperty("The URI of the target system.")
    public String getTargetUri() {
        return targetUri;
    }

    public void setTargetUri(String targetUri) {
        this.targetUri = targetUri;
    }

    /**
     * @return name of this remote process group
     */
    @ApiModelProperty("The name of the remote process group.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return transmission status of this remote process group
     */
    @ApiModelProperty("The transmission status of the remote process group.")
    public String getTransmissionStatus() {
        return transmissionStatus;
    }

    public void setTransmissionStatus(String transmissionStatus) {
        this.transmissionStatus = transmissionStatus;
    }

    /**
     * @return number of active threads
     */
    @ApiModelProperty("The number of active threads for the remote process group.")
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * @return Formatted description of the amount of data sent to this remote process group
     */
    @ApiModelProperty("The count/size of the flowfiles sent to the remote process group in the last 5 minutes.")
    public String getSent() {
        return sent;
    }

    public void setSent(String sent) {
        this.sent = sent;
    }


    /**
     * @return Formatted description of the amount of data received from this remote process group
     */
    @ApiModelProperty("The count/size of the flowfiles received from the remote process group in the last 5 minutes.")
    public String getReceived() {
        return received;
    }

    public void setReceived(String received) {
        this.received = received;
    }

    @ApiModelProperty("The number of FlowFiles sent to the remote process group in the last 5 minutes.")
    public Integer getFlowFilesSent() {
        return flowFilesSent;
    }

    public void setFlowFilesSent(Integer flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
    }

    @ApiModelProperty("The size of the FlowFiles sent to the remote process group in the last 5 minutes.")
    public Long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(Long bytesSent) {
        this.bytesSent = bytesSent;
    }

    @ApiModelProperty("The number of FlowFiles received from the remote process group in the last 5 minutes.")
    public Integer getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public void setFlowFilesReceived(Integer flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
    }

    @ApiModelProperty("The size of the FlowFiles received from the remote process group in the last 5 minutes.")
    public Long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(Long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    @Override
    public RemoteProcessGroupStatusSnapshotDTO clone() {
        final RemoteProcessGroupStatusSnapshotDTO other = new RemoteProcessGroupStatusSnapshotDTO();
        other.setId(getId());
        other.setGroupId(getGroupId());
        other.setName(getName());
        other.setTargetUri(getTargetUri());
        other.setTransmissionStatus(getTransmissionStatus());
        other.setActiveThreadCount(getActiveThreadCount());
        other.setFlowFilesSent(getFlowFilesSent());
        other.setBytesSent(getBytesSent());
        other.setFlowFilesReceived(getFlowFilesReceived());
        other.setBytesReceived(getBytesReceived());
        other.setReceived(getReceived());
        other.setSent(getSent());

        return other;
    }
}
