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

package org.apache.nifi.registry.diff;

import io.swagger.annotations.ApiModelProperty;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a group of differences related to a specific component in a flow.
 */
public class ComponentDifferenceGroup {
    private String componentId;
    private String componentName;
    private String componentType;
    private String processGroupId;
    private Set<ComponentDifference> differences = new HashSet<>();

    @ApiModelProperty("The id of the component whose changes are grouped together.")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    @ApiModelProperty("The name of the component whose changes are grouped together.")
    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    @ApiModelProperty("The type of component these changes relate to.")
    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(String componentType) {
        this.componentType = componentType;
    }

    @ApiModelProperty("The process group id for this component.")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @ApiModelProperty("The list of changes related to this component between the 2 versions.")
    public Set<ComponentDifference> getDifferences() {
        return differences;
    }

    public void setDifferences(Set<ComponentDifference> differences) {
        this.differences = differences;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComponentDifferenceGroup that = (ComponentDifferenceGroup) o;
        return Objects.equals(componentId, that.componentId)
                && Objects.equals(componentName, that.componentName)
                && Objects.equals(componentType, that.componentType)
                && Objects.equals(processGroupId, that.processGroupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentId, componentName, componentType, processGroupId);
    }
}
