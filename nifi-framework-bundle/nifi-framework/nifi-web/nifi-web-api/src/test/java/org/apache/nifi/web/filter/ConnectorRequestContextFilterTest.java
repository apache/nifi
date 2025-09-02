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
package org.apache.nifi.web.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletResponse;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.components.connector.ConnectorRequestContext;
import org.apache.nifi.components.connector.ConnectorRequestContextHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class ConnectorRequestContextFilterTest {

    private static final String TEST_USER = "test.user@apache.org";
    private static final String TOKEN_HEADER = "API-Key";
    private static final String TOKEN_VALUE = "test-api-key-123";
    private static final String ROLE_HEADER = "API-Role";
    private static final String ROLE_VALUE = "SERVICE_ROLE";

    @Mock
    private FilterChain filterChain;

    @Mock
    private ServletResponse response;

    @AfterEach
    void cleanup() {
        ConnectorRequestContextHolder.clearContext();
        SecurityContextHolder.clearContext();
    }

    @Test
    void testContextIsSetDuringFilterChain() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(TOKEN_HEADER, TOKEN_VALUE);
        request.addHeader(ROLE_HEADER, ROLE_VALUE);
        setUpSecurityContext();

        final ConnectorRequestContext[] capturedContext = new ConnectorRequestContext[1];
        doAnswer(invocation -> {
            capturedContext[0] = ConnectorRequestContextHolder.getContext();
            return null;
        }).when(filterChain).doFilter(any(), any());

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        filter.doFilter(request, response, filterChain);

        assertNotNull(capturedContext[0]);
        assertNotNull(capturedContext[0].getAuthenticatedUser());
        assertEquals(TEST_USER, capturedContext[0].getAuthenticatedUser().getIdentity());

        assertTrue(capturedContext[0].hasRequestHeader(TOKEN_HEADER));
        assertTrue(capturedContext[0].hasRequestHeader(ROLE_HEADER));
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstRequestHeaderValue(TOKEN_HEADER));
        assertEquals(ROLE_VALUE, capturedContext[0].getFirstRequestHeaderValue(ROLE_HEADER));
        assertEquals(List.of(TOKEN_VALUE), capturedContext[0].getRequestHeaderValues(TOKEN_HEADER));
        assertEquals(List.of(ROLE_VALUE), capturedContext[0].getRequestHeaderValues(ROLE_HEADER));
    }

    @Test
    void testContextIsClearedAfterFilterChain() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(TOKEN_HEADER, TOKEN_VALUE);
        setUpSecurityContext();

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        filter.doFilter(request, response, filterChain);

        assertNull(ConnectorRequestContextHolder.getContext());
    }

    @Test
    void testContextIsClearedAfterFilterChainException() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        setUpSecurityContext();

        doAnswer(invocation -> {
            throw new RuntimeException("simulated error");
        }).when(filterChain).doFilter(any(), any());

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        try {
            filter.doFilter(request, response, filterChain);
        } catch (final RuntimeException ignored) {
            // expected
        }

        assertNull(ConnectorRequestContextHolder.getContext());
    }

    @Test
    void testContextWithNoAuthenticatedUser() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(TOKEN_HEADER, TOKEN_VALUE);

        final ConnectorRequestContext[] capturedContext = new ConnectorRequestContext[1];
        doAnswer(invocation -> {
            capturedContext[0] = ConnectorRequestContextHolder.getContext();
            return null;
        }).when(filterChain).doFilter(any(), any());

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        filter.doFilter(request, response, filterChain);

        assertNotNull(capturedContext[0]);
        assertNull(capturedContext[0].getAuthenticatedUser());
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstRequestHeaderValue(TOKEN_HEADER));
    }

    @Test
    void testHeaderMapIsCaseInsensitive() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(TOKEN_HEADER, TOKEN_VALUE);

        final ConnectorRequestContext[] capturedContext = new ConnectorRequestContext[1];
        doAnswer(invocation -> {
            capturedContext[0] = ConnectorRequestContextHolder.getContext();
            return null;
        }).when(filterChain).doFilter(any(), any());

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        filter.doFilter(request, response, filterChain);

        assertNotNull(capturedContext[0]);
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstRequestHeaderValue(TOKEN_HEADER.toLowerCase()));
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstRequestHeaderValue(TOKEN_HEADER.toUpperCase()));
        assertTrue(capturedContext[0].hasRequestHeader(TOKEN_HEADER.toLowerCase()));
        assertTrue(capturedContext[0].hasRequestHeader(TOKEN_HEADER.toUpperCase()));
        assertFalse(capturedContext[0].hasRequestHeader("Non-Existent-Header"));
        assertEquals(List.of(), capturedContext[0].getRequestHeaderValues("Non-Existent-Header"));
        assertNull(capturedContext[0].getFirstRequestHeaderValue("Non-Existent-Header"));
    }

    private void setUpSecurityContext() {
        final NiFiUser user = mock(NiFiUser.class, withSettings().strictness(Strictness.LENIENT));
        when(user.getIdentity()).thenReturn(TEST_USER);
        final NiFiUserDetails userDetails = new NiFiUserDetails(user);
        final TestingAuthenticationToken authentication = new TestingAuthenticationToken(userDetails, "");
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }
}
