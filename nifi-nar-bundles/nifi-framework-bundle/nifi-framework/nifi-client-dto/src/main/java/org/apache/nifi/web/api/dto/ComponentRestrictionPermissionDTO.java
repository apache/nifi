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
import java.util.Objects;

/**
 * Class used for providing details about a components usage restriction.
 */
@XmlType(name = "componentRestrictionPermission")
public class ComponentRestrictionPermissionDTO {

    private RequiredPermissionDTO requiredPermission;
    private PermissionsDTO permissions;

    /**
     * @return The required permission necessary for this restriction.
     */
    @ApiModelProperty(
            value = "The required permission necessary for this restriction."
    )
    public RequiredPermissionDTO getRequiredPermission() {
        return requiredPermission;
    }

    public void setRequiredPermission(RequiredPermissionDTO requiredPermission) {
        this.requiredPermission = requiredPermission;
    }

    /**
     * @return The permissions for this component restriction.
     */
    @ApiModelProperty(
            value = "The permissions for this component restriction. Note: the read permission are not used and will always be false."
    )
    public PermissionsDTO getPermissions() {
        return permissions;
    }

    public void setPermissions(PermissionsDTO permissions) {
        this.permissions = permissions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requiredPermission);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }

        return Objects.equals(requiredPermission, ((ComponentRestrictionPermissionDTO)obj).requiredPermission);
    }
}
