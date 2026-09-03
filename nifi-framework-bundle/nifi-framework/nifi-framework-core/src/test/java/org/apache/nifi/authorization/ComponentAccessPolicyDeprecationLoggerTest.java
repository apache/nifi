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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ComponentAccessPolicyDeprecationLoggerTest {

    private static final String RESOURCE_FORMAT = "%s%s/%s";

    private static final String EMPTY_PREFIX = "";

    private static final String ROOT_GROUP_ID = "a1d4c1b0-0000-1000-0000-000000000001";

    private static final String CHILD_GROUP_ID = "a1d4c1b0-0000-1000-0000-000000000002";

    private static final String COMPONENT_ID = "a1d4c1b0-0000-1000-0000-000000000003";

    private static final String ROOT_GROUP_RESOURCE = getComponentResource(ResourceType.ProcessGroup, ROOT_GROUP_ID);

    private static final String CHILD_GROUP_RESOURCE = getComponentResource(ResourceType.ProcessGroup, CHILD_GROUP_ID);

    @Mock
    private Authorizer authorizer;

    @Mock
    private ManagedAuthorizer managedAuthorizer;

    @Mock
    private AccessPolicyProvider accessPolicyProvider;

    @Test
    void testRootProcessGroupAndSharedResourcesNotDeprecated() {
        final Set<AccessPolicy> policies = getPolicies(
                ROOT_GROUP_RESOURCE,
                getComponentResource(ResourceType.Data, ResourceType.ProcessGroup, ROOT_GROUP_ID),
                getComponentResource(ResourceType.Policy, ResourceType.ProcessGroup, ROOT_GROUP_ID),
                getComponentResource(ResourceType.Operation, ResourceType.ProcessGroup, ROOT_GROUP_ID),
                getComponentResource(ResourceType.ProvenanceData, ResourceType.ProcessGroup, ROOT_GROUP_ID),
                ResourceType.Flow.getValue(),
                ResourceType.Controller.getValue(),
                ResourceType.Tenant.getValue(),
                ResourceType.Policy.getValue(),
                ResourceType.Proxy.getValue(),
                ResourceType.System.getValue(),
                ResourceType.Counters.getValue(),
                ResourceType.Provenance.getValue(),
                ResourceType.ParameterContext.getValue()
        );

        assertEquals(Set.of(), getDeprecatedResources(policies, ROOT_GROUP_ID));
    }

    @Test
    void testResourceTypesOutsideComponentScopeNotDeprecated() {
        final Set<AccessPolicy> policies = getPolicies(
                getComponentResource(ResourceType.RegistryClient, COMPONENT_ID),
                getComponentResource(ResourceType.FlowAnalysisRule, COMPONENT_ID),
                getComponentResource(ResourceType.ReportingTask, COMPONENT_ID),
                getComponentResource(ResourceType.Connector, COMPONENT_ID)
        );

        assertEquals(Set.of(), getDeprecatedResources(policies, ROOT_GROUP_ID));
    }

    @Test
    void testProcessGroupOtherThanRootGroupDeprecated() {
        final String childGroupDataResource = getComponentResource(ResourceType.Data, ResourceType.ProcessGroup, CHILD_GROUP_ID);
        final String childGroupPolicyResource = getComponentResource(ResourceType.Policy, ResourceType.ProcessGroup, CHILD_GROUP_ID);

        final Set<AccessPolicy> policies = getPolicies(ROOT_GROUP_RESOURCE, CHILD_GROUP_RESOURCE, childGroupDataResource, childGroupPolicyResource);

        assertEquals(Set.of(CHILD_GROUP_RESOURCE, childGroupDataResource, childGroupPolicyResource), getDeprecatedResources(policies, ROOT_GROUP_ID));
        assertEquals(Set.of(ROOT_GROUP_RESOURCE, CHILD_GROUP_RESOURCE, childGroupDataResource, childGroupPolicyResource), getDeprecatedResources(policies, null));
    }

    @Test
    void testComponentResourceTypesDeprecated() {
        final Set<String> componentResources = Set.of(
                getComponentResource(ResourceType.Processor, COMPONENT_ID),
                getComponentResource(ResourceType.ControllerService, COMPONENT_ID),
                getComponentResource(ResourceType.InputPort, COMPONENT_ID),
                getComponentResource(ResourceType.OutputPort, COMPONENT_ID),
                getComponentResource(ResourceType.Funnel, COMPONENT_ID),
                getComponentResource(ResourceType.Label, COMPONENT_ID),
                getComponentResource(ResourceType.RemoteProcessGroup, COMPONENT_ID),
                getComponentResource(ResourceType.ParameterContext, COMPONENT_ID),
                getComponentResource(ResourceType.ParameterProvider, COMPONENT_ID),
                getComponentResource(ResourceType.Data, ResourceType.Processor, COMPONENT_ID),
                getComponentResource(ResourceType.DataTransfer, ResourceType.InputPort, COMPONENT_ID),
                getComponentResource(ResourceType.Operation, ResourceType.Processor, COMPONENT_ID),
                getComponentResource(ResourceType.ProvenanceData, ResourceType.Processor, COMPONENT_ID)
        );

        final Set<AccessPolicy> policies = getPolicies(componentResources.toArray(new String[0]));

        assertEquals(componentResources, getDeprecatedResources(policies, ROOT_GROUP_ID));
    }

    @Test
    void testLogComponentPoliciesManagedAuthorizer() {
        when(managedAuthorizer.getAccessPolicyProvider()).thenReturn(accessPolicyProvider);
        when(accessPolicyProvider.getAccessPolicies()).thenReturn(getPolicies(getComponentResource(ResourceType.Processor, COMPONENT_ID)));

        ComponentAccessPolicyDeprecationLogger.logComponentPolicies(managedAuthorizer, ROOT_GROUP_ID);

        verify(accessPolicyProvider).getAccessPolicies();
    }

    @Test
    void testLogComponentPoliciesAuthorizerWithoutAccessPolicies() {
        ComponentAccessPolicyDeprecationLogger.logComponentPolicies(authorizer, ROOT_GROUP_ID);

        verifyNoInteractions(authorizer);
    }

    private static String getComponentResource(final ResourceType resourceType, final String identifier) {
        return RESOURCE_FORMAT.formatted(EMPTY_PREFIX, resourceType.getValue(), identifier);
    }

    private static String getComponentResource(final ResourceType prefixResourceType, final ResourceType resourceType, final String identifier) {
        return RESOURCE_FORMAT.formatted(prefixResourceType.getValue(), resourceType.getValue(), identifier);
    }

    private static Set<String> getDeprecatedResources(final Set<AccessPolicy> policies, final String rootGroupId) {
        final Set<String> resources = new LinkedHashSet<>();

        for (final AccessPolicy policy : ComponentAccessPolicyDeprecationLogger.findComponentPolicies(policies, rootGroupId)) {
            resources.add(policy.getResource());
        }

        return resources;
    }

    private static Set<AccessPolicy> getPolicies(final String... resources) {
        final Set<AccessPolicy> policies = new LinkedHashSet<>();

        for (final String resource : resources) {
            final AccessPolicy policy = new AccessPolicy.Builder()
                    .identifierGenerateFromSeed(resource)
                    .resource(resource)
                    .action(RequestAction.READ)
                    .build();

            policies.add(policy);
        }

        return policies;
    }
}
