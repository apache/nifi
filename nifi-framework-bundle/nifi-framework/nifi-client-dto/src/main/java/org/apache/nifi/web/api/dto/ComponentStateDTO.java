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

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * State for a given component.
 */
@XmlType(name = "componentState")
public class ComponentStateDTO {

    private String componentId;
    private String stateDescription;
    private StateMapDTO clusterState;
    private StateMapDTO localState;
    private Boolean dropStateKeySupported;

    /**
     * @return The component identifier
     */
    @Schema(description = "The component identifier."
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
    @Schema(description = "Description of the state this component persists."
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
    @Schema(description = "The cluster state for this component, or null if this NiFi is a standalone instance."
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
    @Schema(description = "The local state for this component."
    )
    public StateMapDTO getLocalState() {
        return localState;
    }

    public void setLocalState(StateMapDTO localState) {
        this.localState = localState;
    }

    /**
     * @return Whether dropping state by key is supported for this component.
     */
    @Schema(description = "Whether dropping state by key is supported for this component. Defaults to false when not specified by the component.")
    public Boolean isDropStateKeySupported() {
        return dropStateKeySupported;
    }

    public void setDropStateKeySupported(final Boolean dropStateKeySupported) {
        this.dropStateKeySupported = dropStateKeySupported;
    }
}
