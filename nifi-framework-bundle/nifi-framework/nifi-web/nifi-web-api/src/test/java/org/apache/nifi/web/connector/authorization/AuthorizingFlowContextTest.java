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
package org.apache.nifi.web.connector.authorization;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AuthorizingFlowContextTest {

    private static final String CONNECTOR_ID = "test-connector-id";
    private static final String USER_IDENTITY = "test-user";

    @Mock
    private FlowContext delegate;

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup authorizableLookup;

    @Mock
    private Authorizable connectorAuthorizable;

    @Mock
    private ProcessGroupFacade processGroupFacade;

    @Mock
    private ParameterContextFacade parameterContextFacade;

    @Mock
    private ConnectorConfigurationContext configurationContext;

    @Mock
    private SecurityContext securityContext;

    @Mock
    private Authentication authentication;

    private AuthorizingFlowContext authorizingFlowContext;

    @BeforeEach
    void setUp() {
        when(authorizableLookup.getConnector(CONNECTOR_ID)).thenReturn(connectorAuthorizable);
        lenient().when(authorizer.authorize(any(AuthorizationRequest.class))).thenReturn(AuthorizationResult.approved());

        final NiFiUser user = new StandardNiFiUser.Builder().identity(USER_IDENTITY).build();
        final NiFiUserDetails userDetails = new NiFiUserDetails(user);
        when(authentication.getPrincipal()).thenReturn(userDetails);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        SecurityContextHolder.setContext(securityContext);

        final ConnectorAuthorizationContext authContext = new ConnectorAuthorizationContext(CONNECTOR_ID, authorizer, authorizableLookup);
        authorizingFlowContext = new AuthorizingFlowContext(delegate, authContext);
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testGetRootGroupAuthorizesReadAndReturnsWrappedFacade() {
        when(delegate.getRootGroup()).thenReturn(processGroupFacade);

        final ProcessGroupFacade result = authorizingFlowContext.getRootGroup();

        assertNotNull(result);
        assertTrue(result instanceof AuthorizingProcessGroupFacade);
        verify(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));
    }

    @Test
    void testGetParameterContextAuthorizesReadAndReturnsWrappedFacade() {
        when(delegate.getParameterContext()).thenReturn(parameterContextFacade);

        final ParameterContextFacade result = authorizingFlowContext.getParameterContext();

        assertNotNull(result);
        assertTrue(result instanceof AuthorizingParameterContextFacade);
        verify(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));
    }

    @Test
    void testGetConfigurationContextAuthorizesRead() {
        when(delegate.getConfigurationContext()).thenReturn(configurationContext);

        final ConnectorConfigurationContext result = authorizingFlowContext.getConfigurationContext();

        assertNotNull(result);
        verify(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));
    }

    @Test
    void testGetTypeAuthorizesRead() {
        when(delegate.getType()).thenReturn(FlowContextType.WORKING);

        authorizingFlowContext.getType();

        verify(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));
    }

    @Test
    void testGetBundleAuthorizesRead() {
        when(delegate.getBundle()).thenReturn(new Bundle("group", "artifact", "version"));

        authorizingFlowContext.getBundle();

        verify(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));
    }

    @Test
    void testAuthorizationFailurePropagates() {
        doThrow(new AccessDeniedException("Access denied"))
                .when(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));

        assertThrows(AccessDeniedException.class, () -> authorizingFlowContext.getRootGroup());
    }
}

