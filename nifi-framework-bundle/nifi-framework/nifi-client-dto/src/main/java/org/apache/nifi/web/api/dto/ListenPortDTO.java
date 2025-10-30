/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

import java.util.List;

@XmlType(name = "listenPort")
public class ListenPortDTO {

    // Port definition
    private int portNumber;
    private String transportProtocol;
    private List<String> applicationProtocols;

    // Contextual information about the component providing the port, and the PG containing the component
    private String componentType;
    private String componentId;
    private String componentName;
    private String componentClass;

    private String parentGroupId;
    private String parentGroupName;

    // TODO not sure this is needed
    // The idea is that if the ListenComponent belongs to a nested PG, the "ancestor" group would identify the highest level PG that is not the root group
    private String ancestorGroupId;
    private String ancestorGroupName;

    @Schema(description = "The ingress port number")
    public int getPortNumber() {
        return portNumber;
    }

    public void setPortNumber(final int portNumber) {
        this.portNumber = portNumber;
    }

    @Schema(description = "The ingress transport protocol (TCP or UDP)")
    public String getTransportProtocol() {
        return transportProtocol;
    }

    public void setTransportProtocol(final String transportProtocol) {
        this.transportProtocol = transportProtocol;
    }

    @Schema(description = "Supported application protocols, if applicable")
    public List<String> getApplicationProtocols() {
        return applicationProtocols;
    }

    public void setApplicationProtocols(final List<String> applicationProtocols) {
        this.applicationProtocols = applicationProtocols;
    }

    @Schema(description = "The type of component providing the listen port (e.g., Processor, ControllerService)")
    public String getComponentType() {
        return componentType;
    }

    public void setComponentType(final String componentType) {
        this.componentType = componentType;
    }

    @Schema(description = "The id of the component providing the listen port")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(final String componentId) {
        this.componentId = componentId;
    }

    @Schema(description = "The name of the component providing the listen port")
    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(final String componentName) {
        this.componentName = componentName;
    }

    @Schema(description = "The class type of the component providing the listen port")
    public String getComponentClass() {
        return componentClass;
    }

    public void setComponentClass(final String componentClass) {
        this.componentClass = componentClass;
    }

    @Schema(description = "The id of the process group containing the component providing the listen port, if applicable")
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(final String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    @Schema(description = "The name of the process group containing the component providing the listen port, if applicable")
    public String getParentGroupName() {
        return parentGroupName;
    }

    public void setParentGroupName(final String parentGroupName) {
        this.parentGroupName = parentGroupName;
    }

    @Schema(description = "The id of the highest level process group containing the component providing the listen port that is not the root group, if applicable")
    public String getAncestorGroupId() {
        return ancestorGroupId;
    }

    public void setAncestorGroupId(final String ancestorGroupId) {
        this.ancestorGroupId = ancestorGroupId;
    }

    @Schema(description = "The name of the highest level process group containing the component providing the listen port that is not the root group, if applicable")
    public String getAncestorGroupName() {
        return ancestorGroupName;
    }

    public void setAncestorGroupName(final String ancestorGroupName) {
        this.ancestorGroupName = ancestorGroupName;
    }

    @Override
    public String toString() {
        return "ListenPortDTO{" +
            "portNumber=" + portNumber +
            ", transportProtocol='" + transportProtocol + '\'' +
            ", applicationProtocols=" + applicationProtocols +
            ", componentType='" + componentType + '\'' +
            ", componentId='" + componentId + '\'' +
            ", componentName='" + componentName + '\'' +
            ", componentClass='" + componentClass + '\'' +
            '}';
    }
}
