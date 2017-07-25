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

import javax.xml.bind.annotation.XmlType;

import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(name = "affectedComponent")
public class AffectedComponentDTO {
    public static final String COMPONENT_TYPE_PROCESSOR = "PROCESSOR";
    public static final String COMPONENT_TYPE_CONTROLLER_SERVICE = "CONTROLLER_SERVICE";

    private String parentGroupId;
    private String componentId;
    private String componentType;

    @ApiModelProperty("The UUID of the Process Group that this component is in")
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(final String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    @ApiModelProperty("The UUID of this component")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(final String componentId) {
        this.componentId = componentId;
    }

    @ApiModelProperty(value = "The type of this component", allowableValues = COMPONENT_TYPE_PROCESSOR + "," + COMPONENT_TYPE_CONTROLLER_SERVICE)
    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(final String componentType) {
        this.componentType = componentType;
    }
}
