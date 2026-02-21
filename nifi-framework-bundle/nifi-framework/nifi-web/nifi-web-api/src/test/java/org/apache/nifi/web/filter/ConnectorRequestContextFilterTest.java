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

@ExtendWith(MockitoExtension.class)
class ConnectorRequestContextFilterTest {

    private static final String TEST_USER = "test.user@snowflake.com";
    private static final String TOKEN_HEADER = "Snowflake-Authorization-Token";
    private static final String TOKEN_VALUE = "eyJhbGciOiJSUzI1NiJ9.test-token";
    private static final String ROLE_HEADER = "Snowflake-Current-Role";
    private static final String ROLE_VALUE = "SYSADMIN";

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
        setUpSecurityContext(TEST_USER);

        final ConnectorRequestContext[] capturedContext = new ConnectorRequestContext[1];
        doAnswer(invocation -> {
            capturedContext[0] = ConnectorRequestContextHolder.getContext();
            return null;
        }).when(filterChain).doFilter(any(), any());

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        filter.doFilter(request, response, filterChain);

        assertNotNull(capturedContext[0]);
        assertNotNull(capturedContext[0].getNiFiUser());
        assertEquals(TEST_USER, capturedContext[0].getNiFiUser().getIdentity());

        assertTrue(capturedContext[0].hasHeader(TOKEN_HEADER));
        assertTrue(capturedContext[0].hasHeader(ROLE_HEADER));
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstHeaderValue(TOKEN_HEADER));
        assertEquals(ROLE_VALUE, capturedContext[0].getFirstHeaderValue(ROLE_HEADER));
        assertEquals(List.of(TOKEN_VALUE), capturedContext[0].getHeaderValues(TOKEN_HEADER));
        assertEquals(List.of(ROLE_VALUE), capturedContext[0].getHeaderValues(ROLE_HEADER));
    }

    @Test
    void testContextIsClearedAfterFilterChain() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(TOKEN_HEADER, TOKEN_VALUE);
        setUpSecurityContext(TEST_USER);

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        filter.doFilter(request, response, filterChain);

        assertNull(ConnectorRequestContextHolder.getContext());
    }

    @Test
    void testContextIsClearedAfterFilterChainException() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        setUpSecurityContext(TEST_USER);

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
        assertNull(capturedContext[0].getNiFiUser());
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstHeaderValue(TOKEN_HEADER));
    }

    @Test
    void testHeaderMapIsCaseInsensitive() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Snowflake-Authorization-Token", TOKEN_VALUE);

        final ConnectorRequestContext[] capturedContext = new ConnectorRequestContext[1];
        doAnswer(invocation -> {
            capturedContext[0] = ConnectorRequestContextHolder.getContext();
            return null;
        }).when(filterChain).doFilter(any(), any());

        final ConnectorRequestContextFilter filter = new ConnectorRequestContextFilter();
        filter.doFilter(request, response, filterChain);

        assertNotNull(capturedContext[0]);
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstHeaderValue("snowflake-authorization-token"));
        assertEquals(TOKEN_VALUE, capturedContext[0].getFirstHeaderValue("SNOWFLAKE-AUTHORIZATION-TOKEN"));
        assertTrue(capturedContext[0].hasHeader("snowflake-authorization-token"));
        assertFalse(capturedContext[0].hasHeader("Non-Existent-Header"));
        assertEquals(List.of(), capturedContext[0].getHeaderValues("Non-Existent-Header"));
        assertNull(capturedContext[0].getFirstHeaderValue("Non-Existent-Header"));
    }

    private void setUpSecurityContext(final String identity) {
        final NiFiUser user = mock(NiFiUser.class, org.mockito.Mockito.withSettings().lenient());
        when(user.getIdentity()).thenReturn(identity);
        final NiFiUserDetails userDetails = new NiFiUserDetails(user);
        final TestingAuthenticationToken authentication = new TestingAuthenticationToken(userDetails, null);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }
}
