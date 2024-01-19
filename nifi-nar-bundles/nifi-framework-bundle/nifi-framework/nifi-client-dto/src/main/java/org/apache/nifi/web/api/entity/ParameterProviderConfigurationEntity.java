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
import org.apache.nifi.web.api.dto.ParameterProviderConfigurationDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;

import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API.
 * This particular entity holds a reference to a ParameterProviderConfigurationDTO.
 */
@XmlRootElement(name = "parameterProviderConfigurationEntity")
public class ParameterProviderConfigurationEntity implements Permissible<ParameterProviderConfigurationDTO> {

    private String id;
    private PermissionsDTO permissions;
    private ParameterProviderConfigurationDTO component;

    /**
     * The id for this component.
     *
     * @return The id
     */
    @Schema(description = "The id of the component."
    )
    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The permissions for this component.
     *
     * @return The permissions
     */
    @Schema(description = "The permissions for this component."
    )
    public PermissionsDTO getPermissions() {
        return permissions;
    }

    public void setPermissions(PermissionsDTO permissions) {
        this.permissions = permissions;
    }

    /**
     * The ParameterProviderConfigurationDTO that is being serialized.
     *
     * @return The ParameterProviderConfigurationDTO object
     */
    public ParameterProviderConfigurationDTO getComponent() {
        return component;
    }

    public void setComponent(ParameterProviderConfigurationDTO component) {
        this.component = component;
    }
}
