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

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * State for a given component.
 */
@XmlType(name = "componentState")
public class ComponentStateDTO {

    private String componentId;
    private String stateDescription;
    private StateMapDTO clusterState;
    private StateMapDTO localState;

    /**
     * @return The component identifier
     */
    @ApiModelProperty(
        value = "The component identifier."
    )
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(String componentId) {
        this.componentId = componentId;
    }

    /**
     * @return Description of the state this component persists.
     */
    @ApiModelProperty(
        value = "Description of the state this component persists."
    )
    public String getStateDescription() {
        return stateDescription;
    }

    public void setStateDescription(String stateDescription) {
        this.stateDescription = stateDescription;
    }

    /**
     * @return The cluster state for this component, or null if this NiFi is a standalone instance
     */
    @ApiModelProperty(
        value = "The cluster state for this component, or null if this NiFi is a standalone instance."
    )
    public StateMapDTO getClusterState() {
        return clusterState;
    }

    public void setClusterState(StateMapDTO clusterState) {
        this.clusterState = clusterState;
    }

    /**
     * @return The local state for this component
     */
    @ApiModelProperty(
        value = "The local state for this component."
    )
    public StateMapDTO getLocalState() {
        return localState;
    }

    public void setLocalState(StateMapDTO localState) {
        this.localState = localState;
    }
}
