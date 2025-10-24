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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API.
 * This particular entity holds a reference to a ConfigurationStepConfigurationDTO.
 * Note that this entity does not extend ComponentEntity because a configuration step is not itself an updatable component.
 * Instead, it is updated through the parent Connector, so the client must provide the parent connector ID and revision.
 * Permissions are not included on this entity since they are managed at the parent Connector level.
 */
@XmlRootElement(name = "configurationStepEntity")
public class ConfigurationStepEntity extends Entity {

    private ConfigurationStepConfigurationDTO configurationStep;
    private String parentConnectorId;
    private RevisionDTO parentConnectorRevision;
    private Boolean disconnectedNodeAcknowledged;

    /**
     * @return the configuration step configuration
     */
    @Schema(description = "The configuration step configuration.")
    public ConfigurationStepConfigurationDTO getConfigurationStep() {
        return configurationStep;
    }

    public void setConfigurationStep(final ConfigurationStepConfigurationDTO configurationStep) {
        this.configurationStep = configurationStep;
    }

    @Schema(description = "The id of the parent connector.")
    public String getParentConnectorId() {
        return parentConnectorId;
    }

    public void setParentConnectorId(final String parentConnectorId) {
        this.parentConnectorId = parentConnectorId;
    }

    @Schema(description = "The revision of the parent connector that this configuration step belongs to.")
    public RevisionDTO getParentConnectorRevision() {
        return parentConnectorRevision;
    }

    public void setParentConnectorRevision(final RevisionDTO parentConnectorRevision) {
        this.parentConnectorRevision = parentConnectorRevision;
    }

    @Schema(description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.")
    public Boolean isDisconnectedNodeAcknowledged() {
        return disconnectedNodeAcknowledged;
    }

    public void setDisconnectedNodeAcknowledged(final Boolean disconnectedNodeAcknowledged) {
        this.disconnectedNodeAcknowledged = disconnectedNodeAcknowledged;
    }
}
