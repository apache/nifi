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
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;

import jakarta.xml.bind.annotation.XmlRootElement;


/**
 * A serialized representation of this class can be placed in the entity body of a response to the API. This particular entity holds a reference to a parameter provider.
 */
@XmlRootElement(name = "parameterProviderEntity")
public class ParameterProviderEntity extends ComponentEntity implements Permissible<ParameterProviderDTO> {

    private ParameterProviderDTO component;
    private PermissionsDTO permissions;

    /**
     * @return parameter provider that is being serialized
     */
    @Override
    public ParameterProviderDTO getComponent() {
        return component;
    }

    @Override
    public void setComponent(ParameterProviderDTO component) {
        this.component = component;
    }

    /**
     * @return The permissions for this component operations
     */
    @Schema(description = "The permissions for this component."
    )
    @Override
    public PermissionsDTO getPermissions() {
        return permissions;
    }

    @Override
    public void setPermissions(PermissionsDTO permissions) {
        this.permissions = permissions;
    }
}
