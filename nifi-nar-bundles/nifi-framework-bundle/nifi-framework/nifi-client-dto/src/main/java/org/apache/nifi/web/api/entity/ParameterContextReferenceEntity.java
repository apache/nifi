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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ParameterContextReferenceDTO.
 */
@XmlRootElement(name = "parameterContextReferenceEntity")
public class ParameterContextReferenceEntity implements Permissible<ParameterContextReferenceDTO> {

    private String id;
    private PermissionsDTO permissions;
    private ParameterContextReferenceDTO component;

    /**
     * The id for this component.
     *
     * @return The id
     */
    @ApiModelProperty(
            value = "The id of the component."
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
    @ApiModelProperty(
            value = "The permissions for this component."
    )
    public PermissionsDTO getPermissions() {
        return permissions;
    }

    public void setPermissions(PermissionsDTO permissions) {
        this.permissions = permissions;
    }

    /**
     * The ParameterContextReferenceDTO that is being serialized.
     *
     * @return The ParameterContextReferenceDTO object
     */
    public ParameterContextReferenceDTO getComponent() {
        return component;
    }

    public void setComponent(ParameterContextReferenceDTO component) {
        this.component = component;
    }
}
