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
package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Details of a node within this NiFi.
 */
@XmlType(name = "node")
public class NodeDTO {

    private String nodeId;
    private String address;
    private Integer apiPort;
    private String status;
    private Date heartbeat;
    private Date connectionRequested;
    private Set<String> roles;
    private Integer activeThreadCount;
    private String queued;
    private List<NodeEventDTO> events;
    private Date nodeStartTime;
    private Integer flowFilesQueued;
    private Long bytesQueued;

    /**
     * @return node's last heartbeat timestamp
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @Schema(description = "the time of the nodes's last heartbeat.",
            accessMode = Schema.AccessMode.READ_ONLY,
            type = "string"
    )
    public Date getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(Date heartbeat) {
        this.heartbeat = heartbeat;
    }

    /**
     * @return time of the node's last connection request
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @Schema(description = "The time of the node's last connection request.",
            accessMode = Schema.AccessMode.READ_ONLY,
            type = "string"
    )
    public Date getConnectionRequested() {
        return connectionRequested;
    }

    public void setConnectionRequested(Date connectionRequested) {
        this.connectionRequested = connectionRequested;
    }

    /**
     * The active thread count.
     *
     * @return The active thread count
     */
    @Schema(description = "The active threads for the NiFi on the node.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * @return queue for the controller
     */
    @Schema(description = "The queue the NiFi on the node.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public String getQueued() {
        return queued;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    /**
     * @return node's host/IP address
     */
    @Schema(description = "The node's host/ip address.",
            accessMode = Schema.AccessMode.READ_ONLY
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
    @Schema(description = "The id of the node.",
            accessMode = Schema.AccessMode.READ_ONLY
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
    @Schema(description = "The port the node is listening for API requests.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public Integer getApiPort() {
        return apiPort;
    }

    public void setApiPort(Integer port) {
        this.apiPort = port;
    }

    /**
     * @return node's status
     */
    @Schema(description = "The node's status."
    )
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * @return node's events
     */
    @Schema(description = "The node's events.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public List<NodeEventDTO> getEvents() {
        return events;
    }

    public void setEvents(List<NodeEventDTO> events) {
        this.events = events;
    }

    /**
     * @return the roles of the node
     */
    @Schema(description = "The roles of this node.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public Set<String> getRoles() {
        return roles;
    }

    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }

    /**
     * @return time at which this Node was last restarted
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @Schema(description = "The time at which this Node was last refreshed.",
            accessMode = Schema.AccessMode.READ_ONLY,
            type = "string"
    )
    public Date getNodeStartTime() {
        return nodeStartTime;
    }

    public void setNodeStartTime(Date nodeStartTime) {
        this.nodeStartTime = nodeStartTime;
    }

    /**
     * @return the number of FlowFiles that are queued up on the node
     */
    @Schema(description = "The number of FlowFiles that are queued up on the node",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public Integer getFlowFilesQueued() {
        return flowFilesQueued;
    }

    public void setFlowFilesQueued(Integer flowFilesQueued) {
        this.flowFilesQueued = flowFilesQueued;
    }

    /**
     * @return the total size of all FlowFiles that are queued up on the node
     */
    @Schema(description = "The total size of all FlowFiles that are queued up on the node",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public Long getBytesQueued() {
        return bytesQueued;
    }

    public void setFlowFileBytes(Long bytesQueued) {
        this.bytesQueued = bytesQueued;
    }
}
