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
import java.util.List;

/**
 * DTO for serializing the status history of a single component across the cluster.
 */
@XmlType(name = "nodeStatusSnapshots")
public class NodeStatusSnapshotsDTO {

    private String nodeId;
    private String address;
    private Integer apiPort;
    private List<StatusSnapshotDTO> statusSnapshots;

    /**
     * @return node's host/IP address
     */
    @ApiModelProperty(
        value = "The node's host/ip address."
    )
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * @return node ID
     */
    @ApiModelProperty(
        value = "The id of the node."
    )
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return port the node is listening for API requests
     */
    @ApiModelProperty(
        value = "The port the node is listening for API requests."
    )
    public Integer getApiPort() {
        return apiPort;
    }

    public void setApiPort(Integer port) {
        this.apiPort = port;
    }

    @ApiModelProperty("A list of StatusSnapshotDTO objects that provide the actual metric values for the component for this node.")
    public List<StatusSnapshotDTO> getStatusSnapshots() {
        return statusSnapshots;
    }

    public void setStatusSnapshots(List<StatusSnapshotDTO> statusSnapshots) {
        this.statusSnapshots = statusSnapshots;
    }
}
