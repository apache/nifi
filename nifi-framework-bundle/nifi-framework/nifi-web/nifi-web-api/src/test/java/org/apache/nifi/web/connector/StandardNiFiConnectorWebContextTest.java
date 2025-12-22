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
package org.apache.nifi.web.connector;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.web.ConnectorWebMethod;
import org.apache.nifi.web.ConnectorWebMethod.AccessType;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.web.NiFiConnectorWebContext.ConnectorWebContext;
import org.apache.nifi.web.connector.authorization.AuthorizingFlowContext;
import org.apache.nifi.web.dao.ConnectorDAO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
public class StandardNiFiConnectorWebContextTest {

    private static final String CONNECTOR_ID = "test-connector-id";
    private static final String USER_IDENTITY = "test-user";

    @Mock
    private ConnectorDAO connectorDAO;

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuthorizableLookup authorizableLookup;

    @Mock
    private ConnectorNode connectorNode;

    @Mock
    private FrameworkFlowContext workingFlowContext;

    @Mock
    private FrameworkFlowContext activeFlowContext;

    @Mock
    private Authorizable connectorAuthorizable;

    @Mock
    private SecurityContext securityContext;

    @Mock
    private Authentication authentication;

    private StandardNiFiConnectorWebContext context;

    private TestConnector testConnectorMock;
    private String lastWrittenValue;

    @BeforeEach
    void setUp() {
        context = new StandardNiFiConnectorWebContext();
        context.setConnectorDAO(connectorDAO);
        context.setAuthorizer(authorizer);
        context.setAuthorizableLookup(authorizableLookup);

        testConnectorMock = mock(TestConnector.class, withSettings().extraInterfaces(Connector.class).lenient());
        lenient().when(testConnectorMock.readData()).thenReturn("read-result");
        lenient().when(testConnectorMock.writeData(any())).thenAnswer(invocation -> {
            lastWrittenValue = invocation.getArgument(0);
            return null;
        });

        lenient().when(connectorDAO.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        lenient().when(connectorNode.getConnector()).thenReturn((Connector) testConnectorMock);
        lenient().when(connectorNode.getWorkingFlowContext()).thenReturn(workingFlowContext);
        lenient().when(connectorNode.getActiveFlowContext()).thenReturn(activeFlowContext);
        lenient().when(authorizableLookup.getConnector(CONNECTOR_ID)).thenReturn(connectorAuthorizable);
        lenient().when(authorizer.authorize(any(AuthorizationRequest.class))).thenReturn(AuthorizationResult.approved());

        final NiFiUser user = new StandardNiFiUser.Builder().identity(USER_IDENTITY).build();
        final NiFiUserDetails userDetails = new NiFiUserDetails(user);
        lenient().when(authentication.getPrincipal()).thenReturn(userDetails);
        lenient().when(securityContext.getAuthentication()).thenReturn(authentication);
        SecurityContextHolder.setContext(securityContext);
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testGetConnectorWebContextReturnsProxiedConnector() {
        final ConnectorWebContext<TestConnector> webContext = context.getConnectorWebContext(CONNECTOR_ID);

        assertNotNull(webContext);
        assertNotNull(webContext.connector());
        assertTrue(Proxy.isProxyClass(webContext.connector().getClass()));
        assertNotSame(testConnectorMock, webContext.connector());
    }

    @Test
    void testGetConnectorWebContextReturnsWrappedFlowContexts() {
        final ConnectorWebContext<TestConnector> webContext = context.getConnectorWebContext(CONNECTOR_ID);

        assertNotNull(webContext.workingFlowContext());
        assertNotNull(webContext.activeFlowContext());
        assertTrue(webContext.workingFlowContext() instanceof AuthorizingFlowContext);
        assertTrue(webContext.activeFlowContext() instanceof AuthorizingFlowContext);
    }

    @Test
    void testGetConnectorWebContextThrowsForNonExistentConnector() {
        when(connectorDAO.getConnector("non-existent-id")).thenReturn(null);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> context.getConnectorWebContext("non-existent-id"));
        assertEquals("Unable to find connector with id: non-existent-id", exception.getMessage());
    }

    @Test
    void testProxiedConnectorEnforcesReadAuthorization() {
        final ConnectorWebContext<TestConnector> webContext = context.getConnectorWebContext(CONNECTOR_ID);
        final TestConnector proxy = webContext.connector();

        final String result = proxy.readData();

        assertEquals("read-result", result);
        verify(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));
    }

    @Test
    void testProxiedConnectorEnforcesWriteAuthorization() {
        final ConnectorWebContext<TestConnector> webContext = context.getConnectorWebContext(CONNECTOR_ID);
        final TestConnector proxy = webContext.connector();

        proxy.writeData("test-value");

        assertEquals("test-value", lastWrittenValue);
        verify(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));
    }

    @Test
    void testProxiedConnectorBlocksUnannotatedMethods() {
        final ConnectorWebContext<TestConnector> webContext = context.getConnectorWebContext(CONNECTOR_ID);
        final TestConnector proxy = webContext.connector();

        assertThrows(IllegalStateException.class, proxy::unannotatedMethod);
    }

    @Test
    void testProxiedConnectorPropagatesAuthorizationFailure() {
        doThrow(new AccessDeniedException("Access denied"))
                .when(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));

        final ConnectorWebContext<TestConnector> webContext = context.getConnectorWebContext(CONNECTOR_ID);
        final TestConnector proxy = webContext.connector();

        assertThrows(AccessDeniedException.class, proxy::readData);
    }

    /**
     * Test interface representing a Connector with annotated methods.
     * This interface is used with Mockito's extraInterfaces to create a mock
     * that implements both this interface and Connector.
     */
    public interface TestConnector {

        @ConnectorWebMethod(AccessType.READ)
        String readData();

        @ConnectorWebMethod(AccessType.WRITE)
        Void writeData(String value);

        void unannotatedMethod();
    }
}

