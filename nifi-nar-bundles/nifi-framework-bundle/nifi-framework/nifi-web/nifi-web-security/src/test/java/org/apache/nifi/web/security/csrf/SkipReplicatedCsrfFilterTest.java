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
package org.apache.nifi.web.security.csrf;

import org.apache.nifi.web.security.http.SecurityCookieName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpMethod;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.web.csrf.CsrfFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SkipReplicatedCsrfFilterTest {
    private static final String SHOULD_NOT_FILTER = String.format("SHOULD_NOT_FILTER%s", CsrfFilter.class.getName());

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    @Mock
    FilterChain filterChain;

    SkipReplicatedCsrfFilter filter;

    @BeforeEach
    void setHandler() {
        filter = new SkipReplicatedCsrfFilter();
        httpServletRequest = new MockHttpServletRequest();
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testDoFilterInternalNotSkipped() throws ServletException, IOException {
        httpServletRequest.setMethod(HttpMethod.GET.name());

        filter.doFilterInternal(httpServletRequest, httpServletResponse, filterChain);

        assertCsrfFilterNotSkipped();
    }

    @Test
    void testDoFilterInternalBearerCookieNotSkipped() throws ServletException, IOException {
        httpServletRequest.setMethod(HttpMethod.GET.name());
        final Cookie bearerCookie = new Cookie(SecurityCookieName.AUTHORIZATION_BEARER.getName(), UUID.randomUUID().toString());
        httpServletRequest.setCookies(bearerCookie);

        filter.doFilterInternal(httpServletRequest, httpServletResponse, filterChain);

        assertCsrfFilterNotSkipped();
    }

    @Test
    void testDoFilterInternalReplicatedHeaderAndBearerCookieNotSkipped() throws ServletException, IOException {
        httpServletRequest.setMethod(HttpMethod.GET.name());
        httpServletRequest.addHeader(SkipReplicatedCsrfFilter.REPLICATED_REQUEST_HEADER, UUID.randomUUID().toString());
        final Cookie bearerCookie = new Cookie(SecurityCookieName.AUTHORIZATION_BEARER.getName(), UUID.randomUUID().toString());
        httpServletRequest.setCookies(bearerCookie);

        filter.doFilterInternal(httpServletRequest, httpServletResponse, filterChain);

        assertCsrfFilterNotSkipped();
    }

    @Test
    void testDoFilterInternalReplicatedHeaderSkipped() throws ServletException, IOException {
        httpServletRequest.setMethod(HttpMethod.GET.name());
        httpServletRequest.addHeader(SkipReplicatedCsrfFilter.REPLICATED_REQUEST_HEADER, UUID.randomUUID().toString());

        filter.doFilterInternal(httpServletRequest, httpServletResponse, filterChain);

        final Object shouldNotFilter = httpServletRequest.getAttribute(SHOULD_NOT_FILTER);

        assertEquals(Boolean.TRUE, shouldNotFilter);
        verify(filterChain).doFilter(eq(httpServletRequest), eq(httpServletResponse));
    }

    private void assertCsrfFilterNotSkipped() throws ServletException, IOException {
        final Object shouldNotFilter = httpServletRequest.getAttribute(SHOULD_NOT_FILTER);

        assertNotEquals(Boolean.TRUE, shouldNotFilter);
        verify(filterChain).doFilter(eq(httpServletRequest), eq(httpServletResponse));
    }
}
