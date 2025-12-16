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
import org.apache.nifi.web.ConnectorWebMethod;
import org.apache.nifi.web.ConnectorWebMethod.AccessType;
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

import java.lang.reflect.Proxy;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class AuthorizingConnectorInvocationHandlerTest {

    private static final String CONNECTOR_ID = "test-connector-id";
    private static final String USER_IDENTITY = "test-user";

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

    private TestConnectorImpl connectorImpl;

    @BeforeEach
    void setUp() {
        connectorImpl = new TestConnectorImpl();
        lenient().when(authorizableLookup.getConnector(CONNECTOR_ID)).thenReturn(connectorAuthorizable);

        final NiFiUser user = new StandardNiFiUser.Builder().identity(USER_IDENTITY).build();
        final NiFiUserDetails userDetails = new NiFiUserDetails(user);
        lenient().when(authentication.getPrincipal()).thenReturn(userDetails);
        lenient().when(securityContext.getAuthentication()).thenReturn(authentication);
        SecurityContextHolder.setContext(securityContext);

        lenient().when(authorizer.authorize(any(AuthorizationRequest.class))).thenReturn(AuthorizationResult.approved());
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testReadMethodAuthorizesWithReadAction() {
        final TestConnector proxy = createProxy();

        final String result = proxy.readData();

        assertEquals("read-result", result);
        final ArgumentCaptor<RequestAction> actionCaptor = ArgumentCaptor.forClass(RequestAction.class);
        verify(connectorAuthorizable).authorize(eq(authorizer), actionCaptor.capture(), any(NiFiUser.class));
        assertEquals(RequestAction.READ, actionCaptor.getValue());
    }

    @Test
    void testWriteMethodAuthorizesWithWriteAction() {
        final TestConnector proxy = createProxy();

        proxy.writeData("test-value");

        assertEquals("test-value", connectorImpl.getLastWrittenValue());
        final ArgumentCaptor<RequestAction> actionCaptor = ArgumentCaptor.forClass(RequestAction.class);
        verify(connectorAuthorizable).authorize(eq(authorizer), actionCaptor.capture(), any(NiFiUser.class));
        assertEquals(RequestAction.WRITE, actionCaptor.getValue());
    }

    @Test
    void testMethodWithoutAnnotationThrowsException() {
        final TestConnector proxy = createProxy();

        final IllegalStateException exception = assertThrows(IllegalStateException.class, proxy::unannotatedMethod);
        assertEquals(String.format("Method [unannotatedMethod] on connector [%s] is not annotated with "
                + "@ConnectorWebMethod and cannot be invoked through the Connector Web Context", CONNECTOR_ID), exception.getMessage());
    }

    @Test
    void testAuthorizationFailurePropagatesAccessDeniedException() {
        doThrow(new AccessDeniedException("Access denied for testing"))
                .when(connectorAuthorizable).authorize(any(Authorizer.class), any(RequestAction.class), any(NiFiUser.class));

        final TestConnector proxy = createProxy();

        assertThrows(AccessDeniedException.class, proxy::readData);
    }

    @Test
    void testMethodArgumentsPassedCorrectly() {
        final TestConnector proxy = createProxy();

        final int result = proxy.processItems(List.of("a", "b", "c"), 10);

        assertEquals(13, result);
    }

    @Test
    void testDelegateExceptionUnwrapped() {
        final TestConnector proxy = createProxy();

        final RuntimeException exception = assertThrows(RuntimeException.class, proxy::throwingMethod);
        assertEquals("Intentional test exception", exception.getMessage());
    }

    @Test
    void testReadMethodWithReturnValue() {
        final TestConnector proxy = createProxy();

        final List<String> result = proxy.getItems();

        assertEquals(List.of("item1", "item2", "item3"), result);
    }

    private TestConnector createProxy() {
        final AuthorizingConnectorInvocationHandler<TestConnector> handler = new AuthorizingConnectorInvocationHandler<>(
                connectorImpl, CONNECTOR_ID, authorizer, authorizableLookup);

        return (TestConnector) Proxy.newProxyInstance(
                TestConnector.class.getClassLoader(),
                new Class<?>[]{TestConnector.class},
                handler);
    }

    /**
     * Test interface representing a Connector with annotated methods.
     */
    public interface TestConnector {

        @ConnectorWebMethod(AccessType.READ)
        String readData();

        @ConnectorWebMethod(AccessType.WRITE)
        void writeData(String value);

        @ConnectorWebMethod(AccessType.READ)
        List<String> getItems();

        @ConnectorWebMethod(AccessType.WRITE)
        int processItems(List<String> items, int multiplier);

        @ConnectorWebMethod(AccessType.READ)
        void throwingMethod();

        void unannotatedMethod();
    }

    /**
     * Test implementation of the TestConnector interface.
     */
    public static class TestConnectorImpl implements TestConnector {

        private String lastWrittenValue;

        @Override
        public String readData() {
            return "read-result";
        }

        @Override
        public void writeData(final String value) {
            this.lastWrittenValue = value;
        }

        @Override
        public List<String> getItems() {
            return List.of("item1", "item2", "item3");
        }

        @Override
        public int processItems(final List<String> items, final int multiplier) {
            return items.size() + multiplier;
        }

        @Override
        public void throwingMethod() {
            throw new RuntimeException("Intentional test exception");
        }

        @Override
        public void unannotatedMethod() {
            // This method is intentionally not annotated
        }

        public String getLastWrittenValue() {
            return lastWrittenValue;
        }
    }
}

