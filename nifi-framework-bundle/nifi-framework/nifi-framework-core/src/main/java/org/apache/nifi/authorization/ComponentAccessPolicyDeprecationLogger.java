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
import org.apache.nifi.deprecation.log.DeprecationLogger;
import org.apache.nifi.deprecation.log.DeprecationLoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Log deprecation warnings for Access Policies configured against individual components. Authorization for
 * individual components will be replaced with authorization for the Controller per NIP-39
 */
public final class ComponentAccessPolicyDeprecationLogger {

    private static final DeprecationLogger deprecationLogger = DeprecationLoggerFactory.getLogger(ComponentAccessPolicyDeprecationLogger.class);

    /** Resource Types that identify single component instances */
    private static final Set<ResourceType> COMPONENT_RESOURCE_TYPES = Set.of(
            ResourceType.ProcessGroup,
            ResourceType.Processor,
            ResourceType.ControllerService,
            ResourceType.InputPort,
            ResourceType.OutputPort,
            ResourceType.Funnel,
            ResourceType.Label,
            ResourceType.RemoteProcessGroup,
            ResourceType.ParameterContext,
            ResourceType.ParameterProvider
    );

    /** Resource Types that can precede other Types */
    private static final Set<ResourceType> RESOURCE_PREFIX_TYPES = Set.of(
            ResourceType.Data,
            ResourceType.DataTransfer,
            ResourceType.Operation,
            ResourceType.Policy,
            ResourceType.ProvenanceData
    );

    private static final String RESOURCE_SEPARATOR = "/";

    /**
     * Log deprecation warnings when the configured Authorizer provides Access Policies for individual components
     *
     * @param authorizer Configured Authorizer
     * @param rootGroupId Identifier of the root Process Group, for which Access Policies are not deprecated
     */
    public static void logComponentPolicies(final Authorizer authorizer, final String rootGroupId) {
        if (authorizer instanceof final ManagedAuthorizer managedAuthorizer) {
            final AccessPolicyProvider accessPolicyProvider = managedAuthorizer.getAccessPolicyProvider();
            final Set<AccessPolicy> configuredAccessPolicies = accessPolicyProvider.getAccessPolicies();
            final Set<AccessPolicy> componentPolicies = findComponentPolicies(configuredAccessPolicies, rootGroupId);

            if (!componentPolicies.isEmpty()) {
                deprecationLogger.warn("Found [{}] component Access Policies deprecated for removal in NIP-39", componentPolicies.size());
            }
        }
    }

    static Set<AccessPolicy> findComponentPolicies(final Set<AccessPolicy> policies, final String rootGroupId) {
        final Set<AccessPolicy> componentPolicies = new LinkedHashSet<>();

        for (final AccessPolicy policy : policies) {
            final String resource = policy.getResource();
            if (isComponentPolicy(resource, rootGroupId)) {
                componentPolicies.add(policy);
            }
        }

        return componentPolicies;
    }

    private static boolean isComponentPolicy(final String resource, final String rootGroupId) {
        boolean componentPolicyFound = false;

        final String componentResource = removeResourcePrefix(resource);

        for (final ResourceType componentResourceType : COMPONENT_RESOURCE_TYPES) {
            final String componentResourcePrefix = componentResourceType.getValue() + RESOURCE_SEPARATOR;

            if (componentResource.startsWith(componentResourcePrefix)) {
                if (ResourceType.ProcessGroup == componentResourceType) {
                    final String groupIdentifier = componentResource.substring(componentResourcePrefix.length());
                    if (groupIdentifier.equals(rootGroupId)) {
                        // Root Group Identifier expected and ignored
                        continue;
                    } else {
                        componentPolicyFound = true;
                    }
                } else {
                    componentPolicyFound = true;
                }
                break;
            }
        }

        return componentPolicyFound;
    }

    private static String removeResourcePrefix(final String resource) {
        String resourceNormalized = resource;

        for (final ResourceType resourcePrefixType : RESOURCE_PREFIX_TYPES) {
            final String resourcePrefixValue = resourcePrefixType.getValue();
            final String resourcePrefixSeparator = resourcePrefixValue + RESOURCE_SEPARATOR;

            if (resource.startsWith(resourcePrefixSeparator)) {
                resourceNormalized = resource.substring(resourcePrefixValue.length());
                break;
            }
        }

        return resourceNormalized;
    }

    private ComponentAccessPolicyDeprecationLogger() { }
}
