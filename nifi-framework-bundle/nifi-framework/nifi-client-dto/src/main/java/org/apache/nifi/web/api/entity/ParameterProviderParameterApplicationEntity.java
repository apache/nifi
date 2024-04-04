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
import org.apache.nifi.web.api.dto.RevisionDTO;

import jakarta.xml.bind.annotation.XmlType;
import java.util.Collection;

/**
 * Request for applying fetched parameters from a Parameter Provider.
 */
@XmlType(name = "parameterProviderParameterApplication")
public class ParameterProviderParameterApplicationEntity extends Entity {

    private String id;
    private RevisionDTO revision;
    private Boolean disconnectedNodeAcknowledged;
    private Collection<ParameterGroupConfigurationEntity> parameterGroupConfigurations;

    @Schema(description = "The id of the parameter provider."
    )
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * @return revision for this request/response
     */
    @Schema(description = "The revision for this request/response. The revision is required for any mutable flow requests and is included in all responses."
    )
    public RevisionDTO getRevision() {
        return revision;
    }

    public void setRevision(final RevisionDTO revision) {
        this.revision = revision;
    }

    @Schema(description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
    )
    public Boolean isDisconnectedNodeAcknowledged() {
        return disconnectedNodeAcknowledged;
    }

    public void setDisconnectedNodeAcknowledged(final Boolean disconnectedNodeAcknowledged) {
        this.disconnectedNodeAcknowledged = disconnectedNodeAcknowledged;
    }

    /**
     * @return Specifies per group which parameter names should be applied to the Parameter Contexts.
     */
    @Schema(description = "Configuration for the fetched Parameter Groups"
    )
    public Collection<ParameterGroupConfigurationEntity> getParameterGroupConfigurations() {
        return parameterGroupConfigurations;
    }

    public void setParameterGroupConfigurations(Collection<ParameterGroupConfigurationEntity> parameterGroupConfigurations) {
        this.parameterGroupConfigurations = parameterGroupConfigurations;
    }
}
