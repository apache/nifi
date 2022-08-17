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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * An object representation of a Heartbeat in the C2 protocol
 */
@ApiModel
public class C2Heartbeat implements Serializable {
    private static final long serialVersionUID = 1L;

    private String identifier;
    private Long created;

    private DeviceInfo deviceInfo;
    private AgentInfo agentInfo;
    private FlowInfo flowInfo;

    @ApiModelProperty(hidden = true)
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @ApiModelProperty(hidden = true)
    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    @ApiModelProperty("Metadata for the device")
    public DeviceInfo getDeviceInfo() {
        return deviceInfo;
    }

    public void setDeviceInfo(DeviceInfo deviceInfo) {
        this.deviceInfo = deviceInfo;
    }

    @ApiModelProperty("Metadata for the agent installed on the device")
    public AgentInfo getAgentInfo() {
        return agentInfo;
    }

    public void setAgentInfo(AgentInfo agentInfo) {
        this.agentInfo = agentInfo;
    }

    @ApiModelProperty("Metadata for the flow currently deployed to the agent")
    public FlowInfo getFlowInfo() {
        return flowInfo;
    }

    public void setFlowInfo(FlowInfo flowInfo) {
        this.flowInfo = flowInfo;
    }

    // Convenience getters
    @ApiModelProperty(hidden = true)
    public String getDeviceId() {
        return deviceInfo != null ? deviceInfo.getIdentifier() : null;
    }

    @ApiModelProperty(hidden = true)
    public String getAgentId() {
        return agentInfo != null ? agentInfo.getIdentifier() : null;
    }

    @ApiModelProperty(hidden = true)
    public String getAgentClass() {
        return agentInfo != null ? agentInfo.getAgentClass() : null;
    }

    @ApiModelProperty(hidden = true)
    public String getAgentManifestId() {
        if (agentInfo != null && agentInfo.getAgentManifest() != null) {
            return agentInfo.getAgentManifest().getIdentifier();
        }
        return null;
    }

    @ApiModelProperty(hidden = true)
    public String getFlowId() {
        return flowInfo != null ? flowInfo.getFlowId() : null;
    }

    @Override
    public String toString() {
        return "C2Heartbeat{" +
            "identifier='" + identifier + '\'' +
            ", agentClass=" + (agentInfo != null ? agentInfo.getAgentClass() : "null") +
            ", agent=" + (agentInfo != null ? agentInfo.getIdentifier() : "null") +
            ", device=" + (deviceInfo != null ? deviceInfo.getIdentifier() : "null") +
            ", containsManifest=" + (agentInfo != null && agentInfo.getAgentManifest() != null) +
            ", created=" + created +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final C2Heartbeat that = (C2Heartbeat) o;
        return Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier);
    }

}
