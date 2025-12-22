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

import org.apache.nifi.asset.Asset;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AuthorizingParameterContextFacadeTest {

    private static final String CONNECTOR_ID = "test-connector-id";
    private static final String USER_IDENTITY = "test-user";

    @Mock
    private ParameterContextFacade delegate;

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup authorizableLookup;

    @Mock
    private Authorizable connectorAuthorizable;

    @Mock
    private SecurityContext securityContext;

    @Mock
    private Authentication authentication;

    @Mock
    private Asset asset;

    private AuthorizingParameterContextFacade authorizingFacade;

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
        authorizingFacade = new AuthorizingParameterContextFacade(delegate, authContext);
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testGetValueAuthorizesWithReadAction() {
        when(delegate.getValue("param1")).thenReturn("value1");

        final String result = authorizingFacade.getValue("param1");

        assertEquals("value1", result);
        final ArgumentCaptor<RequestAction> actionCaptor = ArgumentCaptor.forClass(RequestAction.class);
        verify(connectorAuthorizable).authorize(eq(authorizer), actionCaptor.capture(), any(NiFiUser.class));
        assertEquals(RequestAction.READ, actionCaptor.getValue());
    }

    @Test
    void testGetDefinedParameterNamesAuthorizesWithReadAction() {
        when(delegate.getDefinedParameterNames()).thenReturn(Set.of("param1", "param2"));

        final Set<String> result = authorizingFacade.getDefinedParameterNames();

        assertEquals(Set.of("param1", "param2"), result);
        final ArgumentCaptor<RequestAction> actionCaptor = ArgumentCaptor.forClass(RequestAction.class);
        verify(connectorAuthorizable).authorize(eq(authorizer), actionCaptor.capture(), any(NiFiUser.class));
        assertEquals(RequestAction.READ, actionCaptor.getValue());
    }

    @Test
    void testIsSensitiveAuthorizesWithReadAction() {
        when(delegate.isSensitive("param1")).thenReturn(true);

        final boolean result = authorizingFacade.isSensitive("param1");

        assertTrue(result);
        final ArgumentCaptor<RequestAction> actionCaptor = ArgumentCaptor.forClass(RequestAction.class);
        verify(connectorAuthorizable).authorize(eq(authorizer), actionCaptor.capture(), any(NiFiUser.class));
        assertEquals(RequestAction.READ, actionCaptor.getValue());
    }

    @Test
    void testUpdateParametersAuthorizesWithWriteAction() {
        authorizingFacade.updateParameters(Collections.emptyList());

        final ArgumentCaptor<RequestAction> actionCaptor = ArgumentCaptor.forClass(RequestAction.class);
        verify(connectorAuthorizable).authorize(eq(authorizer), actionCaptor.capture(), any(NiFiUser.class));
        assertEquals(RequestAction.WRITE, actionCaptor.getValue());
        verify(delegate).updateParameters(Collections.emptyList());
    }

    @Test
    void testCreateAssetAuthorizesWithWriteAction() throws IOException {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        when(delegate.createAsset(inputStream)).thenReturn(asset);

        final Asset result = authorizingFacade.createAsset(inputStream);

        assertEquals(asset, result);
        final ArgumentCaptor<RequestAction> actionCaptor = ArgumentCaptor.forClass(RequestAction.class);
        verify(connectorAuthorizable).authorize(eq(authorizer), actionCaptor.capture(), any(NiFiUser.class));
        assertEquals(RequestAction.WRITE, actionCaptor.getValue());
    }
}

