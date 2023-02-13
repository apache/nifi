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

@ApiModel
public class C2OperationAck implements Serializable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty("The id of the requested operation that is being acknowledged")
    private String operationId;

    @ApiModelProperty("The agent's status response for this operation ID")
    private C2OperationState operationState;

    // Optional, additional details that can be included in an ACK
    @ApiModelProperty("Optionally, an ack can include device info that is relevant to the operation being acknowledged")
    private DeviceInfo deviceInfo;

    @ApiModelProperty("Optionally, an ack can include agent info that is relevant to the operation being acknowledged")
    private AgentInfo agentInfo;

    @ApiModelProperty("Optionally, an ack can include flow info that is relevant to the operation being acknowledged")
    private FlowInfo flowInfo;


    public String getOperationId() {
        return operationId;
    }

    public C2OperationState getOperationState() {
        return operationState;
    }

    public void setOperationId(String operationId) {
        this.operationId = operationId;
    }

    public void setOperationState(final C2OperationState state) {
        this.operationState = state;
    }

    public DeviceInfo getDeviceInfo() {
        return deviceInfo;
    }

    public void setDeviceInfo(final DeviceInfo deviceInfo) {
        this.deviceInfo = deviceInfo;
    }

    public AgentInfo getAgentInfo() {
        return agentInfo;
    }

    public void setAgentInfo(final AgentInfo agentInfo) {
        this.agentInfo = agentInfo;
    }

    public FlowInfo getFlowInfo() {
        return flowInfo;
    }

    public void setFlowInfo(final FlowInfo flowInfo) {
        this.flowInfo = flowInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        C2OperationAck that = (C2OperationAck) o;
        return Objects.equals(operationId, that.operationId) && Objects.equals(operationState, that.operationState) && Objects.equals(deviceInfo,
            that.deviceInfo) && Objects.equals(agentInfo, that.agentInfo) && Objects.equals(flowInfo, that.flowInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationId, operationState, deviceInfo, agentInfo, flowInfo);
    }

    @Override
    public String toString() {
        return "C2OperationAck{" +
            "operationId='" + operationId + '\'' +
            ", operationState=" + operationState +
            ", deviceInfo=" + deviceInfo +
            ", agentInfo=" + agentInfo +
            ", flowInfo=" + flowInfo +
            '}';
    }
}
