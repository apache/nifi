/*
 * Apache NiFi - MiNiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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
import javax.validation.constraints.NotBlank;

@ApiModel
public class AgentInfo implements Serializable {
    private static final long serialVersionUID = -8812289319080770084L;

    @NotBlank
    private String identifier;
    // TODO, do we also need identity. e.g., cert DN
    private String agentClass;
    private AgentManifest agentManifest;
    private AgentStatus status;
    // private Map<String, String> configProperties;  // TODO we should add this information eventually, but we need to handle the best way to handle sharing sensitive properties.

    @ApiModelProperty(
        value = "A unique identifier for the Agent",
        notes = "Usually set when the agent is provisioned and deployed",
        required = true)
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @ApiModelProperty(
        value = "The class or category label of the agent, e.g., 'sensor-collector'",
        notes = "Usually set when the agent is provisioned and deployed")
    public String getAgentClass() {
        return agentClass;
    }

    public void setAgentClass(String agentClass) {
        this.agentClass = agentClass;
    }

    @ApiModelProperty("The specification of the agent's capabilities")
    public AgentManifest getAgentManifest() {
        return agentManifest;
    }

    public void setAgentManifest(AgentManifest agentManifest) {
        this.agentManifest = agentManifest;
    }

    @ApiModelProperty("A summary of the runtime status of the agent")
    public AgentStatus getStatus() {
        return status;
    }

    public void setStatus(AgentStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "AgentInfo{" +
            "identifier='" + identifier + '\'' +
            ", agentClass='" + agentClass + '\'' +
            '}';
    }
}
