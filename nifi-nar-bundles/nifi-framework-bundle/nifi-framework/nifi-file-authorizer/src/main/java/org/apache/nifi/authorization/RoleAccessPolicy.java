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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.ResourceType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Defines the mapping from legacy roles to access policies.
 */
public final class RoleAccessPolicy {

    static final String READ_ACTION = "R";
    static final String WRITE_ACTION = "W";

    private final String resource;
    private final String action;

    private RoleAccessPolicy(final String resource, final String action) {
        this.resource = resource;
        this.action = action;
    }

    public String getResource() {
        return resource;
    }

    public String getAction() {
        return action;
    }

    public static Map<Role,Set<RoleAccessPolicy>> getMappings(final String rootGroupId) {
        final Map<Role,Set<RoleAccessPolicy>> roleAccessPolicies = new HashMap<>();

        final Set<RoleAccessPolicy> monitorPolicies = new HashSet<>();
        monitorPolicies.add(new RoleAccessPolicy(ResourceType.Flow.getValue(), READ_ACTION));
        monitorPolicies.add(new RoleAccessPolicy(ResourceType.Controller.getValue(), READ_ACTION));
        monitorPolicies.add(new RoleAccessPolicy(ResourceType.System.getValue(), READ_ACTION));
        if (rootGroupId != null) {
            monitorPolicies.add(new RoleAccessPolicy(ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, READ_ACTION));
        }
        roleAccessPolicies.put(Role.ROLE_MONITOR, Collections.unmodifiableSet(monitorPolicies));

        final Set<RoleAccessPolicy> provenancePolicies = new HashSet<>();
        provenancePolicies.add(new RoleAccessPolicy(ResourceType.Provenance.getValue(), READ_ACTION));
        if (rootGroupId != null) {
            provenancePolicies.add(new RoleAccessPolicy(ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, READ_ACTION));
        }
        roleAccessPolicies.put(Role.ROLE_PROVENANCE, Collections.unmodifiableSet(provenancePolicies));

        final Set<RoleAccessPolicy> dfmPolicies = new HashSet<>();
        dfmPolicies.add(new RoleAccessPolicy(ResourceType.Flow.getValue(), READ_ACTION));
        dfmPolicies.add(new RoleAccessPolicy(ResourceType.Controller.getValue(), READ_ACTION));
        dfmPolicies.add(new RoleAccessPolicy(ResourceType.Controller.getValue(), WRITE_ACTION));
        dfmPolicies.add(new RoleAccessPolicy(ResourceType.System.getValue(), READ_ACTION));
        dfmPolicies.add(new RoleAccessPolicy(ResourceType.RestrictedComponents.getValue(), WRITE_ACTION));
        if (rootGroupId != null) {
            dfmPolicies.add(new RoleAccessPolicy(ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, READ_ACTION));
            dfmPolicies.add(new RoleAccessPolicy(ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, WRITE_ACTION));
            dfmPolicies.add(new RoleAccessPolicy(ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, READ_ACTION));
            dfmPolicies.add(new RoleAccessPolicy(ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, WRITE_ACTION));
        }
        roleAccessPolicies.put(Role.ROLE_DFM, Collections.unmodifiableSet(dfmPolicies));

        final Set<RoleAccessPolicy> adminPolicies = new HashSet<>();
        adminPolicies.add(new RoleAccessPolicy(ResourceType.Flow.getValue(), READ_ACTION));
        adminPolicies.add(new RoleAccessPolicy(ResourceType.Controller.getValue(), READ_ACTION));
        if (rootGroupId != null) {
            adminPolicies.add(new RoleAccessPolicy(ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, READ_ACTION));
        }
        adminPolicies.add(new RoleAccessPolicy(ResourceType.Tenant.getValue(), READ_ACTION));
        adminPolicies.add(new RoleAccessPolicy(ResourceType.Tenant.getValue(), WRITE_ACTION));
        adminPolicies.add(new RoleAccessPolicy(ResourceType.Policy.getValue(), READ_ACTION));
        adminPolicies.add(new RoleAccessPolicy(ResourceType.Policy.getValue(), WRITE_ACTION));
        roleAccessPolicies.put(Role.ROLE_ADMIN, Collections.unmodifiableSet(adminPolicies));

        final Set<RoleAccessPolicy> proxyPolicies = new HashSet<>();
        proxyPolicies.add(new RoleAccessPolicy(ResourceType.Proxy.getValue(), WRITE_ACTION));
        if (rootGroupId != null) {
            proxyPolicies.add(new RoleAccessPolicy(ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, READ_ACTION));
            proxyPolicies.add(new RoleAccessPolicy(ResourceType.Data.getValue() + ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, WRITE_ACTION));
        }
        roleAccessPolicies.put(Role.ROLE_PROXY, Collections.unmodifiableSet(proxyPolicies));

        final Set<RoleAccessPolicy> nifiPolicies = new HashSet<>();
        nifiPolicies.add(new RoleAccessPolicy(ResourceType.Controller.getValue(), READ_ACTION));
        nifiPolicies.add(new RoleAccessPolicy(ResourceType.SiteToSite.getValue(), READ_ACTION));
        roleAccessPolicies.put(Role.ROLE_NIFI, Collections.unmodifiableSet(nifiPolicies));

        return roleAccessPolicies;
    }

}
