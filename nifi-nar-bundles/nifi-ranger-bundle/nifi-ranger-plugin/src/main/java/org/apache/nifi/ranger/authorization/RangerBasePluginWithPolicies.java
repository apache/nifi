/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.ranger.authorization;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extends the base plugin to add ability to check if a policy exists for a given resource.
 */
public class RangerBasePluginWithPolicies extends RangerBasePlugin {

    private AtomicReference<Set<String>> resources = new AtomicReference<>(new HashSet<>());

    public RangerBasePluginWithPolicies(String serviceType, String appId) {
        super(serviceType, appId);
    }

    @Override
    public void setPolicies(ServicePolicies policies) {
        super.setPolicies(policies);

        final Set<String> newResources = new HashSet<>();

        if (policies.getPolicies() != null) {
            for (RangerPolicy policy : policies.getPolicies()) {
                if (policy.getResources() != null) {
                    for (Map.Entry<String, RangerPolicy.RangerPolicyResource> entry : policy.getResources().entrySet()) {
                        final RangerPolicy.RangerPolicyResource resource = entry.getValue();
                        if (resource != null && resource.getValues() != null) {
                            newResources.addAll(resource.getValues());
                        }
                    }
                }
            }
        }

        this.resources.set(newResources);
    }

    /**
     * Determines if a policy exists for the given resource.
     *
     * @param resourceIdentifier the id of the resource
     *
     * @return true if a policy exists for the given resource, false otherwise
     */
    public boolean doesPolicyExist(String resourceIdentifier) {
        if (resourceIdentifier == null) {
            return false;
        }

        final Set<String> currResources = resources.get();
        if (currResources == null) {
            return false;
        } else {
            return currResources.contains(resourceIdentifier);
        }
    }

}
