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
package org.apache.nifi.web;

import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardNiFiServiceFacadeTest {

    private static final String USER_1 = "user-1";
    private static final String USER_2 = "user-2";

    private static final Integer UNKNOWN_ACTION_ID = 0;

    private static final Integer ACTION_ID_1 = 1;
    private static final String PROCESSOR_ID_1 = "processor-1";

    private static final Integer ACTION_ID_2 = 2;
    private static final String PROCESSOR_ID_2 = "processor-2";

    private StandardNiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    @Before
    public void setUp() throws Exception {
        // audit service
        final AuditService auditService = mock(AuditService.class);
        when(auditService.getAction(anyInt())).then(invocation -> {
            final Integer actionId = invocation.getArgumentAt(0, Integer.class);

            FlowChangeAction action = null;
            if (ACTION_ID_1.equals(actionId)) {
                action = getAction(actionId, PROCESSOR_ID_1);
            } else if (ACTION_ID_2.equals(actionId)) {
                action = getAction(actionId, PROCESSOR_ID_2);
            }

            return action;
        });
        when(auditService.getActions(any(HistoryQuery.class))).then(invocation -> {
            final History history = new History();
            history.setActions(Arrays.asList(getAction(ACTION_ID_1, PROCESSOR_ID_1), getAction(ACTION_ID_2, PROCESSOR_ID_2)));
            return history;
        });


        // authorizable lookup
        final AuthorizableLookup authorizableLookup = mock(AuthorizableLookup.class);
        when(authorizableLookup.getProcessor(Mockito.anyString())).then(getProcessorInvocation -> {
            final String processorId = getProcessorInvocation.getArgumentAt(0, String.class);

            // processor-2 is no longer part of the flow
            if (processorId.equals(PROCESSOR_ID_2)) {
                throw new ResourceNotFoundException("");
            }

            // component authorizable
            final ComponentAuthorizable componentAuthorizable = mock(ComponentAuthorizable.class);
            when(componentAuthorizable.getAuthorizable()).then(getAuthorizableInvocation -> {

                // authorizable
                final Authorizable authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getComponentResource(ResourceType.Processor, processorId, processorId);
                    }
                };

                return authorizable;
            });

            return componentAuthorizable;
        });

        // authorizer
        authorizer = mock(Authorizer.class);
        when(authorizer.authorize(any(AuthorizationRequest.class))).then(invocation -> {
            final AuthorizationRequest request = invocation.getArgumentAt(0, AuthorizationRequest.class);

            AuthorizationResult result = AuthorizationResult.denied();
            if (request.getResource().getIdentifier().endsWith(PROCESSOR_ID_1)) {
                if (USER_1.equals(request.getIdentity())) {
                    result = AuthorizationResult.approved();
                }
            } else if (request.getResource().equals(ResourceFactory.getControllerResource())) {
                if (USER_2.equals(request.getIdentity())) {
                    result = AuthorizationResult.approved();
                }
            }

            return result;
        });

        // flow controller
        final FlowController controller = mock(FlowController.class);
        when(controller.getResource()).thenCallRealMethod();
        when(controller.getParentAuthorizable()).thenCallRealMethod();

        // controller facade
        final ControllerFacade controllerFacade = new ControllerFacade();
        controllerFacade.setFlowController(controller);

        serviceFacade = new StandardNiFiServiceFacade();
        serviceFacade.setAuditService(auditService);
        serviceFacade.setAuthorizableLookup(authorizableLookup);
        serviceFacade.setAuthorizer(authorizer);
        serviceFacade.setEntityFactory(new EntityFactory());
        serviceFacade.setDtoFactory(new DtoFactory());
        serviceFacade.setControllerFacade(controllerFacade);
    }

    private FlowChangeAction getAction(final Integer actionId, final String processorId) {
        final FlowChangeAction action = new FlowChangeAction();
        action.setId(actionId);
        action.setSourceId(processorId);
        action.setSourceType(Component.Processor);
        action.setOperation(Operation.Add);
        return action;
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testGetUnknownAction() throws Exception {
        serviceFacade.getAction(UNKNOWN_ACTION_ID);
    }

    @Test
    public void testGetActionApprovedThroughAction() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // get the action
        final ActionEntity entity = serviceFacade.getAction(ACTION_ID_1);

        // verify
        assertEquals(ACTION_ID_1, entity.getId());
        assertTrue(entity.getCanRead());

        // resource exists and is approved, no need to check the controller
        verify(authorizer, times(1)).authorize(argThat(new ArgumentMatcher<AuthorizationRequest>() {
            @Override
            public boolean matches(Object o) {
                return ((AuthorizationRequest) o).getResource().getIdentifier().endsWith(PROCESSOR_ID_1);
            }
        }));
        verify(authorizer, times(0)).authorize(argThat(new ArgumentMatcher<AuthorizationRequest>() {
            @Override
            public boolean matches(Object o) {
                return ((AuthorizationRequest) o).getResource().equals(ResourceFactory.getControllerResource());
            }
        }));
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetActionDeniedDespiteControllerAccess() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        try {
            // get the action
            serviceFacade.getAction(ACTION_ID_1);
            fail();
        } finally {
            // resource exists, but should trigger access denied and will not check the controller
            verify(authorizer, times(1)).authorize(argThat(new ArgumentMatcher<AuthorizationRequest>() {
                @Override
                public boolean matches(Object o) {
                    return ((AuthorizationRequest) o).getResource().getIdentifier().endsWith(PROCESSOR_ID_1);
                }
            }));
            verify(authorizer, times(0)).authorize(argThat(new ArgumentMatcher<AuthorizationRequest>() {
                @Override
                public boolean matches(Object o) {
                    return ((AuthorizationRequest) o).getResource().equals(ResourceFactory.getControllerResource());
                }
            }));
        }
    }

    @Test
    public void testGetActionApprovedThroughController() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // get the action
        final ActionEntity entity = serviceFacade.getAction(ACTION_ID_2);

        // verify
        assertEquals(ACTION_ID_2, entity.getId());
        assertTrue(entity.getCanRead());

        // component does not exists, so only checks against the controller
        verify(authorizer, times(0)).authorize(argThat(new ArgumentMatcher<AuthorizationRequest>() {
            @Override
            public boolean matches(Object o) {
                return ((AuthorizationRequest) o).getResource().getIdentifier().endsWith(PROCESSOR_ID_2);
            }
        }));
        verify(authorizer, times(1)).authorize(argThat(new ArgumentMatcher<AuthorizationRequest>() {
            @Override
            public boolean matches(Object o) {
                return ((AuthorizationRequest) o).getResource().equals(ResourceFactory.getControllerResource());
            }
        }));
    }

    @Test
    public void testGetActionsForUser1() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        final HistoryDTO dto = serviceFacade.getActions(new HistoryQueryDTO());

        // verify user 1 only has access to actions for processor 1
        dto.getActions().forEach(action -> {
            if (PROCESSOR_ID_1.equals(action.getSourceId())) {
                assertTrue(action.getCanRead());
            } else if (PROCESSOR_ID_2.equals(action.getSourceId())) {
                assertFalse(action.getCanRead());
                assertNull(action.getAction());
            }
        });
    }

    @Test
    public void testGetActionsForUser2() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        final HistoryDTO  dto = serviceFacade.getActions(new HistoryQueryDTO());

        // verify user 2 only has access to actions for processor 2
        dto.getActions().forEach(action -> {
            if (PROCESSOR_ID_1.equals(action.getSourceId())) {
                assertFalse(action.getCanRead());
                assertNull(action.getAction());
            } else if (PROCESSOR_ID_2.equals(action.getSourceId())) {
                assertTrue(action.getCanRead());
            }
        });
    }

}