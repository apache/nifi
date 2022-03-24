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
import java.util.Map;

/**
 * Status of the aspects of the agent, including any agent components that are controllable by the C2 server, ie:
 * - Repositories that can be cleared and their current state
 */
@ApiModel
public class AgentStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long uptime;
    private AgentRepositories repositories;
    private Map<String, ComponentStatus> components;
    private AgentResourceConsumption resourceConsumption;

    @ApiModelProperty("The number of milliseconds since the agent started.")
    public Long getUptime() {
        return uptime;
    }

    public void setUptime(Long uptime) {
        this.uptime = uptime;
    }

    @ApiModelProperty("Status and metrics for the agent repositories")
    public AgentRepositories getRepositories() {
        return repositories;
    }

    public void setRepositories(AgentRepositories repositories) {
        this.repositories = repositories;
    }

    @ApiModelProperty("Status for shared agent components (that is, components that exist outside the context of a specific flow).")
    public Map<String, ComponentStatus> getComponents() {
        return components;
    }

    public void setComponents(Map<String, ComponentStatus> components) {
        this.components = components;
    }

    @ApiModelProperty("Resource consumption details of the agent.")
    public AgentResourceConsumption getResourceConsumption() {
        return resourceConsumption;
    }

    public void setResourceConsumption(AgentResourceConsumption resourceConsumption) {
        this.resourceConsumption = resourceConsumption;
    }
}
