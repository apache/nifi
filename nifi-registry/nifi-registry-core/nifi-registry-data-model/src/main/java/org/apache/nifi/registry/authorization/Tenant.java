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
package org.apache.nifi.registry.authorization;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.revision.entity.RevisableEntity;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A tenant of this NiFi Registry
 */
@ApiModel
public class Tenant implements RevisableEntity {

    private String identifier;
    private String identity;
    private Boolean configurable;
    private ResourcePermissions resourcePermissions;
    private Set<AccessPolicySummary> accessPolicies;
    private RevisionInfo revision;

    public Tenant() {}

    public Tenant(String identifier, String identity) {
        this.identifier = identifier;
        this.identity = identity;
    }

    /**
     * @return tenant's unique identifier
     */
    @ApiModelProperty(
            value = "The computer-generated identifier of the tenant.",
            readOnly = true)
    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    /**
     * @return tenant's identity
     */
    @ApiModelProperty(
            value = "The human-facing identity of the tenant. This can only be changed if the tenant is configurable.",
            required = true)
    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    @ApiModelProperty(
            value = "Indicates if this tenant is configurable, based on which UserGroupProvider has been configured to manage it.",
            readOnly = true)
    public Boolean getConfigurable() {
        return configurable;
    }

    public void setConfigurable(Boolean configurable) {
        this.configurable = configurable;
    }

    @ApiModelProperty(
            value = "A summary top-level resource access policies granted to this tenant.",
            readOnly = true
    )
    public ResourcePermissions getResourcePermissions() {
        return resourcePermissions;
    }

    public void setResourcePermissions(ResourcePermissions resourcePermissions) {
        this.resourcePermissions = resourcePermissions;
    }

    @ApiModelProperty(
            value = "The access policies granted to this tenant.",
            readOnly = true
    )
    public Set<AccessPolicySummary> getAccessPolicies() {
        return accessPolicies;
    }

    public void setAccessPolicies(Set<AccessPolicySummary> accessPolicies) {
        this.accessPolicies = accessPolicies;
    }

    public void addAccessPolicies(Collection<AccessPolicySummary> accessPolicies) {
        if (accessPolicies != null) {
            if (this.accessPolicies == null) {
                this.accessPolicies = new HashSet<>();
            }
            this.accessPolicies.addAll(accessPolicies);
        }
    }

    @ApiModelProperty(
            value = "The revision of this entity used for optimistic-locking during updates.",
            readOnly = true
    )
    @Override
    public RevisionInfo getRevision() {
        return revision;
    }

    @Override
    public void setRevision(RevisionInfo revision) {
        this.revision = revision;
    }
}
