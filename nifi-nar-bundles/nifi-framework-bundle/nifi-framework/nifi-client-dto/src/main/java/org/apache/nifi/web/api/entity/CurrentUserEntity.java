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

import org.apache.nifi.web.api.dto.AccessPolicyDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A serialized representation of this class can be placed in the entity body of a response to the API. This particular entity holds the users identity.
 */
@XmlRootElement(name = "currentEntity")
public class CurrentUserEntity extends Entity {

    private String identity;
    private boolean anonymous;

    private AccessPolicyDTO provenancePermissions;
    private AccessPolicyDTO countersPermissions;
    private AccessPolicyDTO tenantsPermissions;
    private AccessPolicyDTO controllerPermissions;

    /**
     * @return the user identity being serialized
     */
    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    /**
     * @return if the user is anonymous
     */
    public boolean isAnonymous() {
        return anonymous;
    }

    public void setAnonymous(boolean anonymous) {
        this.anonymous = anonymous;
    }

    /**
     * @return if the use can query provenance
     */
    public AccessPolicyDTO getProvenancePermissions() {
        return provenancePermissions;
    }

    public void setProvenancePermissions(AccessPolicyDTO provenancePermissions) {
        this.provenancePermissions = provenancePermissions;
    }

    /**
     * @return permissions for accessing counters
     */
    public AccessPolicyDTO getCountersPermissions() {
        return countersPermissions;
    }

    public void setCountersPermissions(AccessPolicyDTO countersPermissions) {
        this.countersPermissions = countersPermissions;
    }

    /**
     * @return permissions for accessing users
     */
    public AccessPolicyDTO getTenantsPermissions() {
        return tenantsPermissions;
    }

    public void setTenantsPermissions(AccessPolicyDTO tenantsPermissions) {
        this.tenantsPermissions = tenantsPermissions;
    }

    /**
     * @return permissions for accessing the controller
     */
    public AccessPolicyDTO getControllerPermissions() {
        return controllerPermissions;
    }

    public void setControllerPermissions(AccessPolicyDTO controllerPermissions) {
        this.controllerPermissions = controllerPermissions;
    }
}
