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

public class NodeProcessGroupStatusSnapshotDTO implements Cloneable {

    private String nodeId;
    private String address;
    private Integer apiPort;

    private ProcessGroupStatusSnapshotDTO statusSnapshot;

    @ApiModelProperty("The unique ID that identifies the node")
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    @ApiModelProperty("The API address of the node")
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @ApiModelProperty("The API port used to communicate with the node")
    public Integer getApiPort() {
        return apiPort;
    }

    public void setApiPort(Integer apiPort) {
        this.apiPort = apiPort;
    }

    @ApiModelProperty("The process group status snapshot from the node.")
    public ProcessGroupStatusSnapshotDTO getStatusSnapshot() {
        return statusSnapshot;
    }

    public void setStatusSnapshot(ProcessGroupStatusSnapshotDTO statusSnapshot) {
        this.statusSnapshot = statusSnapshot;
    }

    @Override
    public NodeProcessGroupStatusSnapshotDTO clone() {
        final NodeProcessGroupStatusSnapshotDTO other = new NodeProcessGroupStatusSnapshotDTO();
        other.setNodeId(getNodeId());
        other.setAddress(getAddress());
        other.setApiPort(getApiPort());
        other.setStatusSnapshot(getStatusSnapshot().clone());
        return other;
    }

}
