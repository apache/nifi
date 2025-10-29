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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A holder to provide atomic access to data structures.
 */
public class AuthorizationsHolder {
    private final List<AccessPolicy> policies;
    private final Set<AccessPolicy> allPolicies;
    private final Map<String, Set<AccessPolicy>> policiesByResource;
    private final Map<String, AccessPolicy> policiesById;

    /**
     * Creates a new holder and populates all convenience authorizations data structures.
     *
     */
    public AuthorizationsHolder(final List<AccessPolicy> policies) {
        this.policies = policies;

        // load all access policies
        final Set<AccessPolicy> allPolicies = Set.copyOf(policies);

        // create a convenience map from resource id to policies
        final Map<String, Set<AccessPolicy>> policiesByResourceMap = Collections.unmodifiableMap(createResourcePolicyMap(allPolicies));

        // create a convenience map from policy id to policy
        final Map<String, AccessPolicy> policiesByIdMap = Collections.unmodifiableMap(createPoliciesByIdMap(allPolicies));

        // set all the holders
        this.allPolicies = allPolicies;
        this.policiesByResource = policiesByResourceMap;
        this.policiesById = policiesByIdMap;
    }

    /**
     * Creates a map from resource identifier to the set of policies for the given resource.
     *
     * @param allPolicies the set of all policies
     * @return a map from resource identifier to policies
     */
    private Map<String, Set<AccessPolicy>> createResourcePolicyMap(final Set<AccessPolicy> allPolicies) {
        Map<String, Set<AccessPolicy>> resourcePolicies = new HashMap<>();

        for (AccessPolicy policy : allPolicies) {
            final String resource = policy.getResource();
            final Set<AccessPolicy> policies = resourcePolicies.computeIfAbsent(resource, k -> new HashSet<>());
            policies.add(policy);
        }

        return resourcePolicies;
    }

    /**
     * Creates a Map from policy identifier to AccessPolicy.
     *
     * @param policies the set of all access policies
     * @return the Map from policy identifier to AccessPolicy
     */
    private Map<String, AccessPolicy> createPoliciesByIdMap(final Set<AccessPolicy> policies) {
        Map<String, AccessPolicy> policyMap = new HashMap<>();
        for (AccessPolicy policy : policies) {
            policyMap.put(policy.getIdentifier(), policy);
        }
        return policyMap;
    }

    public List<AccessPolicy> getPolicies() {
        return policies;
    }

    public Set<AccessPolicy> getAllPolicies() {
        return allPolicies;
    }

    public Map<String, AccessPolicy> getPoliciesById() {
        return policiesById;
    }

    public AccessPolicy getAccessPolicy(final String resourceIdentifier, final RequestAction action) {
        if (resourceIdentifier == null) {
            throw new IllegalArgumentException("Resource Identifier cannot be null");
        }

        final Set<AccessPolicy> resourcePolicies = policiesByResource.get(resourceIdentifier);
        if (resourcePolicies == null) {
            return null;
        }

        for (AccessPolicy accessPolicy : resourcePolicies) {
            if (accessPolicy.getAction() == action) {
                return accessPolicy;
            }
        }

        return null;
    }

}
