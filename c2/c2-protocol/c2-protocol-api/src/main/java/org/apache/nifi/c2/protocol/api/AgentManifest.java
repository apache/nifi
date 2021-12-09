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

import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.SchedulingDefaults;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

@ApiModel
public class AgentManifest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String identifier;
    private String agentType;
    private String version;
    private BuildInfo buildInfo;
    private List<Bundle> bundles;
    private ComponentManifest componentManifest;
    private SchedulingDefaults schedulingDefaults;

    @ApiModelProperty("A unique identifier for the manifest")
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @ApiModelProperty(
        value = "The type of the agent binary, e.g., 'minifi-java' or 'minifi-cpp'",
        notes = "Usually set when the agent is built.")
    public String getAgentType() {
        return agentType;
    }

    public void setAgentType(String agentType) {
        this.agentType = agentType;
    }

    @ApiModelProperty(
        value = "The version of the agent binary, e.g., '1.0.1'",
        notes = "Usually set when the agent is built.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @ApiModelProperty("Build summary for this agent binary")
    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    public void setBuildInfo(BuildInfo buildInfo) {
        this.buildInfo = buildInfo;
    }

    @ApiModelProperty("All extension bundles included with this agent")
    public List<Bundle> getBundles() {
        return (bundles != null ? Collections.unmodifiableList(bundles) : null);
    }

    public void setBundles(List<Bundle> bundles) {
        this.bundles = bundles;
    }

    @ApiModelProperty("All components of this agent that are not part of a bundle.")
    public ComponentManifest getComponentManifest() {
        return componentManifest;
    }

    public void setComponentManifest(ComponentManifest componentManifest) {
        this.componentManifest = componentManifest;
    }

    @ApiModelProperty("Scheduling defaults for components defined in this manifest")
    public SchedulingDefaults getSchedulingDefaults() {
        return schedulingDefaults;
    }

    public void setSchedulingDefaults(SchedulingDefaults schedulingDefaults) {
        this.schedulingDefaults = schedulingDefaults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AgentManifest that = (AgentManifest) o;

        return new EqualsBuilder()
            .append(identifier, that.identifier)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(identifier)
            .toHashCode();
    }

    @Override
    public String toString() {
        return "AgentManifest{" +
            "identifier='" + identifier + '\'' +
            ", agentType='" + agentType + '\'' +
            ", version='" + version + '\'' +
            ", buildInfo=" + buildInfo +
            '}';
    }
}
