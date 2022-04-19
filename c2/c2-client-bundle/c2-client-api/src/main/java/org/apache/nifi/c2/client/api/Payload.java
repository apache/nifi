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
package org.apache.nifi.c2.client.api;

import org.apache.nifi.c2.protocol.api.AgentInfo;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.DeviceInfo;
import org.apache.nifi.c2.protocol.api.FlowInfo;

public class Payload {

    private String operation;
    private AgentInfo agentInfo;
    private DeviceInfo deviceInfo;
    private FlowInfo flowInfo;

    public Payload() {
    }

    public Payload(C2Heartbeat heartbeat) {
        this.operation = "heartbeat";
        this.agentInfo = heartbeat.getAgentInfo();
        this.deviceInfo = heartbeat.getDeviceInfo();
        this.flowInfo = heartbeat.getFlowInfo();
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public AgentInfo getAgentInfo() {
        return agentInfo;
    }

    public void setAgentInfo(AgentInfo agentInfo) {
        this.agentInfo = agentInfo;
    }

    public DeviceInfo getDeviceInfo() {
        return deviceInfo;
    }

    public void setDeviceInfo(DeviceInfo deviceInfo) {
        this.deviceInfo = deviceInfo;
    }

    public FlowInfo getFlowInfo() {
        return flowInfo;
    }

    public void setFlowInfo(FlowInfo flowInfo) {
        this.flowInfo = flowInfo;
    }
}
