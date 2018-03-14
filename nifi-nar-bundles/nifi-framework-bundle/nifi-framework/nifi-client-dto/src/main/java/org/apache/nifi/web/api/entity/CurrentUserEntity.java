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
import org.apache.nifi.web.api.dto.ComponentRestrictionPermissionDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Set;

/**
 * A serialized representation of this class can be placed in the entity body of a response to the API. This particular entity holds the users identity.
 */
@XmlRootElement(name = "currentEntity")
public class CurrentUserEntity extends Entity {

    private String identity;
    private boolean anonymous;

    private PermissionsDTO provenancePermissions;
    private PermissionsDTO countersPermissions;
    private PermissionsDTO tenantsPermissions;
    private PermissionsDTO controllerPermissions;
    private PermissionsDTO policiesPermissions;
    private PermissionsDTO systemPermissions;
    private PermissionsDTO restrictedComponentsPermissions;
    private Set<ComponentRestrictionPermissionDTO> componentRestrictionPermissions;

    private boolean canVersionFlows;

    /**
     * @return the user identity being serialized
     */
    @ApiModelProperty("The user identity being serialized.")
    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    /**
     * @return if the user is anonymous
     */
    @ApiModelProperty("Whether the current user is anonymous.")
    public boolean isAnonymous() {
        return anonymous;
    }

    public void setAnonymous(boolean anonymous) {
        this.anonymous = anonymous;
    }

    /**
     * @return if the use can query provenance
     */
    @ApiModelProperty("Permissions for querying provenance.")
    public PermissionsDTO getProvenancePermissions() {
        return provenancePermissions;
    }

    public void setProvenancePermissions(PermissionsDTO provenancePermissions) {
        this.provenancePermissions = provenancePermissions;
    }

    /**
     * @return permissions for accessing counters
     */
    @ApiModelProperty("Permissions for accessing counters.")
    public PermissionsDTO getCountersPermissions() {
        return countersPermissions;
    }

    public void setCountersPermissions(PermissionsDTO countersPermissions) {
        this.countersPermissions = countersPermissions;
    }

    /**
     * @return permissions for accessing users
     */
    @ApiModelProperty("Permissions for accessing tenants.")
    public PermissionsDTO getTenantsPermissions() {
        return tenantsPermissions;
    }

    public void setTenantsPermissions(PermissionsDTO tenantsPermissions) {
        this.tenantsPermissions = tenantsPermissions;
    }

    /**
     * @return permissions for accessing the controller
     */
    @ApiModelProperty("Permissions for accessing the controller.")
    public PermissionsDTO getControllerPermissions() {
        return controllerPermissions;
    }

    public void setControllerPermissions(PermissionsDTO controllerPermissions) {
        this.controllerPermissions = controllerPermissions;
    }

    /**
     * @return permissions for accessing the all policies
     */
    @ApiModelProperty("Permissions for accessing the policies.")
    public PermissionsDTO getPoliciesPermissions() {
        return policiesPermissions;
    }

    public void setPoliciesPermissions(PermissionsDTO policiesPermissions) {
        this.policiesPermissions = policiesPermissions;
    }

    /**
     * @return permissions for accessing the system
     */
    @ApiModelProperty("Permissions for accessing system.")
    public PermissionsDTO getSystemPermissions() {
        return systemPermissions;
    }

    public void setSystemPermissions(PermissionsDTO systemPermissions) {
        this.systemPermissions = systemPermissions;
    }

    /**
     * @return permissions for accessing the restricted components
     */
    @ApiModelProperty("Permissions for accessing restricted components. Note: the read permission are not used and will always be false.")
    public PermissionsDTO getRestrictedComponentsPermissions() {
        return restrictedComponentsPermissions;
    }

    public void setRestrictedComponentsPermissions(PermissionsDTO restrictedComponentsPermissions) {
        this.restrictedComponentsPermissions = restrictedComponentsPermissions;
    }

    /**
     * @return permissions for specific component restrictions
     */
    @ApiModelProperty("Permissions for specific component restrictions.")
    public Set<ComponentRestrictionPermissionDTO> getComponentRestrictionPermissions() {
        return componentRestrictionPermissions;
    }

    public void setComponentRestrictionPermissions(Set<ComponentRestrictionPermissionDTO> componentRestrictionPermissions) {
        this.componentRestrictionPermissions = componentRestrictionPermissions;
    }

    /**
     * @return whether the current user can version flows
     */
    @ApiModelProperty("Whether the current user can version flows.")
    public boolean isCanVersionFlows() {
        return canVersionFlows;
    }

    public void setCanVersionFlows(boolean canVersionFlows) {
        this.canVersionFlows = canVersionFlows;
    }
}
