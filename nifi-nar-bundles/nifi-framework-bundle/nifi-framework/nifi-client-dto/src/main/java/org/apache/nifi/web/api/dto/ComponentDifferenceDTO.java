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

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.List;
import java.util.Objects;

@XmlType(name = "componentDifference")
public class ComponentDifferenceDTO {
    private String componentType;
    private String componentId;
    private String componentName;
    private String processGroupId;
    private List<DifferenceDTO> differences;

    @ApiModelProperty("The type of component")
    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(String componentType) {
        this.componentType = componentType;
    }

    @ApiModelProperty("The ID of the component")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    @ApiModelProperty("The name of the component")
    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    @ApiModelProperty("The ID of the Process Group that the component belongs to")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @ApiModelProperty("The differences in the component between the two flows")
    public List<DifferenceDTO> getDifferences() {
        return differences;
    }

    public void setDifferences(List<DifferenceDTO> differences) {
        this.differences = differences;
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentId);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ComponentDifferenceDTO)) {
            return false;
        }

        final ComponentDifferenceDTO other = (ComponentDifferenceDTO) obj;
        return componentId.equals(other.getComponentId());
    }
}
