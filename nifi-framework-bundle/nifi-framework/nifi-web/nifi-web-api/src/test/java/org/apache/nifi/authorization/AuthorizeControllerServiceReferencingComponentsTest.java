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

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizeControllerServiceReferencingComponentsTest {

    private static final String CONTROLLER_SERVICE_ID = "controller-service-id";

    private static final String REFERENCING_SERVICE_ID = "referencing-service-id";

    private final NiFiUser user = new StandardNiFiUser.Builder().identity("unit-test-user").build();

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup lookup;

    @Mock
    private ComponentAuthorizable controllerServiceAuthorizable;

    @Mock
    private Authorizable controllerService;

    @Mock
    private Authorizable referencingService;

    @Mock
    private Authorizable referencingProcessor;

    @Mock
    private Authorizable referencingReportingTask;

    @Mock
    private Authorizable referencingFlowAnalysisRule;

    @Test
    void testAuthorizeControllerServiceStateAuthorizesServiceAndReferencingServices() {
        stubControllerService();
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, ControllerServiceNode.class))
                .thenReturn(List.of(referencingService));

        AuthorizeControllerServiceReferencingComponents.authorize(
                authorizer, lookup, CONTROLLER_SERVICE_ID, ControllerServiceState.ENABLED, ScheduledState.DISABLED, user);

        verify(controllerService).authorize(authorizer, RequestAction.WRITE, user);
        verify(referencingService).authorize(authorizer, RequestAction.WRITE, user);
        verify(lookup, never()).getControllerServiceReferencingComponents(eq(CONTROLLER_SERVICE_ID), eq(ProcessorNode.class));
        verify(lookup, never()).getControllerServiceReferencingComponents(eq(CONTROLLER_SERVICE_ID), eq(ReportingTaskNode.class));
        verify(lookup, never()).getControllerServiceReferencingComponents(eq(CONTROLLER_SERVICE_ID), eq(FlowAnalysisRuleNode.class));
    }

    @Test
    void testAuthorizeScheduledStateAuthorizesServiceAndReferencingSchedulableComponents() {
        stubControllerService();
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, ProcessorNode.class))
                .thenReturn(List.of(referencingProcessor));
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, ReportingTaskNode.class))
                .thenReturn(List.of(referencingReportingTask));
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, FlowAnalysisRuleNode.class))
                .thenReturn(List.of(referencingFlowAnalysisRule));

        AuthorizeControllerServiceReferencingComponents.authorize(
                authorizer, lookup, CONTROLLER_SERVICE_ID, null, ScheduledState.RUNNING, user);

        verify(controllerService).authorize(authorizer, RequestAction.WRITE, user);
        verify(referencingProcessor).authorize(authorizer, RequestAction.WRITE, user);
        verify(referencingReportingTask).authorize(authorizer, RequestAction.WRITE, user);
        verify(referencingFlowAnalysisRule).authorize(authorizer, RequestAction.WRITE, user);
        verify(lookup, never()).getControllerServiceReferencingComponents(eq(CONTROLLER_SERVICE_ID), eq(ControllerServiceNode.class));
    }

    @Test
    void testAuthorizeEveryReferencingComponentEvaluated() {
        stubControllerService();
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, ProcessorNode.class))
                .thenReturn(List.of(referencingProcessor, referencingFlowAnalysisRule));
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, ReportingTaskNode.class))
                .thenReturn(List.of(referencingReportingTask));
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, FlowAnalysisRuleNode.class))
                .thenReturn(List.of());

        AuthorizeControllerServiceReferencingComponents.authorize(
                authorizer, lookup, CONTROLLER_SERVICE_ID, null, ScheduledState.STOPPED, user);

        verify(referencingProcessor).authorize(authorizer, RequestAction.WRITE, user);
        verify(referencingFlowAnalysisRule).authorize(authorizer, RequestAction.WRITE, user);
        verify(referencingReportingTask).authorize(authorizer, RequestAction.WRITE, user);
    }

    @Test
    void testAuthorizeDeniedControllerService() {
        stubControllerService();
        denyOperation(controllerService, CONTROLLER_SERVICE_ID);

        assertThrows(AccessDeniedException.class, () -> AuthorizeControllerServiceReferencingComponents.authorize(
                authorizer, lookup, CONTROLLER_SERVICE_ID, ControllerServiceState.ENABLED, null, user));

        verify(lookup, never()).getControllerServiceReferencingComponents(anyString(), any());
    }

    @Test
    void testAuthorizeDeniedReferencingComponent() {
        stubControllerService();
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, ControllerServiceNode.class))
                .thenReturn(List.of(referencingService));
        denyOperation(referencingService, REFERENCING_SERVICE_ID);

        assertThrows(AccessDeniedException.class, () -> AuthorizeControllerServiceReferencingComponents.authorize(
                authorizer, lookup, CONTROLLER_SERVICE_ID, ControllerServiceState.DISABLED, null, user));
    }

    @Test
    void testAuthorizeWithoutRequestedState() {
        stubControllerService();

        AuthorizeControllerServiceReferencingComponents.authorize(authorizer, lookup, CONTROLLER_SERVICE_ID, null, null, user);

        verify(controllerService).authorize(authorizer, RequestAction.WRITE, user);
        verify(lookup, never()).getControllerServiceReferencingComponents(anyString(), any());
    }

    @Test
    void testAuthorizeWithoutReferencingComponents() {
        stubControllerService();
        when(lookup.getControllerServiceReferencingComponents(CONTROLLER_SERVICE_ID, ControllerServiceNode.class))
                .thenReturn(List.of());

        AuthorizeControllerServiceReferencingComponents.authorize(
                authorizer, lookup, CONTROLLER_SERVICE_ID, ControllerServiceState.ENABLED, null, user);

        verify(controllerService).authorize(authorizer, RequestAction.WRITE, user);
    }

    private void stubControllerService() {
        when(lookup.getControllerService(CONTROLLER_SERVICE_ID)).thenReturn(controllerServiceAuthorizable);
        when(controllerServiceAuthorizable.getAuthorizable()).thenReturn(controllerService);
    }

    private void denyOperation(final Authorizable authorizable, final String identifier) {
        doThrow(new AccessDeniedException("Access is denied"))
                .when(authorizable).authorize(authorizer, RequestAction.WRITE, user);
        when(authorizable.getResource())
                .thenReturn(ResourceFactory.getComponentResource(ResourceType.ControllerService, identifier, "Controller Service"));
        when(authorizer.authorize(any())).thenReturn(AuthorizationResult.denied("Access is denied"));
    }
}
