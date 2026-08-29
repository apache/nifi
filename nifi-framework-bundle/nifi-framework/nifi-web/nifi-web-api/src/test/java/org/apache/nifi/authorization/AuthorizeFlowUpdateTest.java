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

import org.apache.nifi.authorization.AuthorizeFlowUpdate.UnresolvedReferences;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizeFlowUpdateTest {

    private static final String GROUP_ID = "group-id";

    private static final String PARAMETER_CONTEXT_NAME = "Parameter Context";

    private static final String PARAMETER_PROVIDER_ID = "parameter-provider-id";

    private static final String CONTROLLER_SERVICE_ID = "controller-service-id";

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup lookup;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private NiFiUser user;

    @Mock
    private ProcessGroupAuthorizable groupAuthorizable;

    @Mock
    private Authorizable groupResource;

    @Mock
    private ComponentAuthorizable encapsulatedProcessor;

    @Mock
    private Authorizable encapsulatedProcessorResource;

    @Mock
    private Authorizable parameterContexts;

    @Mock
    private Authorizable controller;

    @Test
    void testResolveAndAuthorizeFlowUpdateResolvesReferencesBeforeAuthorizing() {
        final RegisteredFlowSnapshot flowSnapshot = createFlowSnapshot();
        when(lookup.getProcessGroup(eq(GROUP_ID))).thenReturn(groupAuthorizable);
        when(groupAuthorizable.getAuthorizable()).thenReturn(groupResource);

        AuthorizeFlowUpdate.resolveAndAuthorizeFlowUpdate(GROUP_ID, flowSnapshot, serviceFacade, authorizer, lookup, user);

        verify(serviceFacade).discoverCompatibleBundles(eq(flowSnapshot.getFlowContents()));
        verify(serviceFacade).discoverCompatibleBundles(eq(flowSnapshot.getParameterProviders()));
        verify(serviceFacade).resolveInheritedControllerServices(any(FlowSnapshotContainer.class), eq(GROUP_ID), eq(user));
        verify(serviceFacade).resolveParameterProviders(eq(flowSnapshot), eq(user));

        verify(groupResource).authorize(eq(authorizer), eq(RequestAction.READ), any());
        verify(groupResource).authorize(eq(authorizer), eq(RequestAction.WRITE), any());
    }

    @Test
    void testResolveAndAuthorizeFlowUpdateDeniedEncapsulatedProcessor() {
        final RegisteredFlowSnapshot flowSnapshot = createFlowSnapshot();
        when(lookup.getProcessGroup(eq(GROUP_ID))).thenReturn(groupAuthorizable);
        when(groupAuthorizable.getAuthorizable()).thenReturn(groupResource);
        when(groupAuthorizable.getEncapsulatedProcessors()).thenReturn(Set.of(encapsulatedProcessor));
        when(encapsulatedProcessor.getAuthorizable()).thenReturn(encapsulatedProcessorResource);
        doNothing().when(encapsulatedProcessorResource).authorize(eq(authorizer), eq(RequestAction.READ), any());
        doThrow(new AccessDeniedException("denied")).when(encapsulatedProcessorResource).authorize(eq(authorizer), eq(RequestAction.WRITE), any());

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeFlowUpdate.resolveAndAuthorizeFlowUpdate(GROUP_ID, flowSnapshot, serviceFacade, authorizer, lookup, user));
    }

    @Test
    void testAuthorizeFlowUpdateDeniedParameterContextCreation() {
        final RegisteredFlowSnapshot flowSnapshot = createFlowSnapshot();
        flowSnapshot.setParameterContexts(Map.of(PARAMETER_CONTEXT_NAME, createParameterContext()));

        when(lookup.getProcessGroup(eq(GROUP_ID))).thenReturn(groupAuthorizable);
        when(groupAuthorizable.getAuthorizable()).thenReturn(groupResource);
        when(serviceFacade.getParameterContextByName(eq(PARAMETER_CONTEXT_NAME), eq(user))).thenReturn(null);
        when(lookup.getParameterContexts()).thenReturn(parameterContexts);
        doThrow(new AccessDeniedException("denied")).when(parameterContexts).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));

        final UnresolvedReferences unresolvedReferences = new UnresolvedReferences(Set.of(), Set.of());

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeFlowUpdate.authorizeFlowUpdate(GROUP_ID, flowSnapshot, unresolvedReferences, serviceFacade, authorizer, lookup, user));
    }

    @Test
    void testAuthorizeFlowUpdateDeniedUnresolvedParameterProvider() {
        final RegisteredFlowSnapshot flowSnapshot = createFlowSnapshot();
        when(lookup.getProcessGroup(eq(GROUP_ID))).thenReturn(groupAuthorizable);
        when(groupAuthorizable.getAuthorizable()).thenReturn(groupResource);
        when(lookup.getController()).thenReturn(controller);
        doThrow(new AccessDeniedException("denied")).when(controller).authorize(eq(authorizer), eq(RequestAction.WRITE), eq(user));

        final UnresolvedReferences unresolvedReferences = new UnresolvedReferences(Set.of(), Set.of(PARAMETER_PROVIDER_ID));

        assertThrows(AccessDeniedException.class,
                () -> AuthorizeFlowUpdate.authorizeFlowUpdate(GROUP_ID, flowSnapshot, unresolvedReferences, serviceFacade, authorizer, lookup, user));
    }

    @Test
    void testResolveReferencesReturnsUnresolvedIdentifiers() {
        final RegisteredFlowSnapshot flowSnapshot = createFlowSnapshot();
        final FlowSnapshotContainer flowSnapshotContainer = new FlowSnapshotContainer(flowSnapshot);
        when(serviceFacade.resolveInheritedControllerServices(eq(flowSnapshotContainer), eq(GROUP_ID), eq(user))).thenReturn(Set.of(CONTROLLER_SERVICE_ID));
        when(serviceFacade.resolveParameterProviders(eq(flowSnapshot), eq(user))).thenReturn(Set.of(PARAMETER_PROVIDER_ID));

        final UnresolvedReferences unresolvedReferences = AuthorizeFlowUpdate.resolveReferences(GROUP_ID, flowSnapshotContainer, serviceFacade, user);

        assertEquals(Set.of(CONTROLLER_SERVICE_ID), unresolvedReferences.controllerServices());
        assertEquals(Set.of(PARAMETER_PROVIDER_ID), unresolvedReferences.parameterProviders());
    }

    private RegisteredFlowSnapshot createFlowSnapshot() {
        final VersionedProcessGroup flowContents = new VersionedProcessGroup();
        flowContents.setIdentifier(GROUP_ID);

        final RegisteredFlowSnapshot flowSnapshot = new RegisteredFlowSnapshot();
        flowSnapshot.setFlowContents(flowContents);
        flowSnapshot.setParameterContexts(Map.of());
        flowSnapshot.setParameterProviders(Map.of());

        return flowSnapshot;
    }

    private VersionedParameterContext createParameterContext() {
        final VersionedParameter parameter = new VersionedParameter();
        parameter.setName("parameter");
        parameter.setValue("value");

        final VersionedParameterContext parameterContext = new VersionedParameterContext();
        parameterContext.setName(PARAMETER_CONTEXT_NAME);
        parameterContext.setParameters(Set.of(parameter));

        return parameterContext;
    }
}
