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

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.nifi.web.security.saml2.SamlUrlPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpMethod;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class FetchSiteRequestFilterTest {
    private static final String REQUEST_URI = "/process-groups/root/processors";

    private static final String CROSS_SITE = "cross-site";

    private static final String SAME_ORIGIN = "same-origin";

    private static final String SAME_SITE = "same-site";

    private static final String NONE = "none";

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    @Mock
    FilterChain filterChain;

    FetchSiteRequestFilter filter;

    @BeforeEach
    void setFilter() {
        filter = new FetchSiteRequestFilter();
        httpServletResponse = new MockHttpServletResponse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"POST", "PUT", "PATCH", "DELETE"})
    void testDoFilterCrossSiteRejected(final String method) throws ServletException, IOException {
        setRequest(method, CROSS_SITE);

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertRejected();
    }

    @Test
    void testDoFilterCrossSiteUpperCaseRejected() throws ServletException, IOException {
        setRequest(HttpMethod.POST.name(), CROSS_SITE.toUpperCase(Locale.ROOT));

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertRejected();
    }

    @Test
    void testDoFilterReplicatedCrossSiteRejected() throws ServletException, IOException {
        setRequest(HttpMethod.POST.name(), CROSS_SITE);
        httpServletRequest.addHeader(SkipReplicatedCsrfFilter.REPLICATED_REQUEST_HEADER, UUID.randomUUID().toString());

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertRejected();
    }

    @Test
    void testDoFilterMultipleHeadersRejected() throws ServletException, IOException {
        setRequest(HttpMethod.POST.name(), SAME_ORIGIN, SAME_ORIGIN);

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertRejected();
    }

    @Test
    void testDoFilterMultipleHeaderValuesRejected() throws ServletException, IOException {
        setRequest(HttpMethod.POST.name(), String.join(", ", SAME_ORIGIN, SAME_SITE));

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertRejected();
    }

    @ParameterizedTest
    @ValueSource(strings = {SAME_ORIGIN, SAME_SITE, NONE})
    void testDoFilterAllowed(final String fetchSite) throws ServletException, IOException {
        setRequest(HttpMethod.POST.name(), fetchSite);

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertAllowed();
    }

    @Test
    void testDoFilterHeaderNotFoundAllowed() throws ServletException, IOException {
        setRequest(HttpMethod.POST.name());

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertAllowed();
    }

    @Test
    void testDoFilterReplicatedSameOriginAllowed() throws ServletException, IOException {
        setRequest(HttpMethod.POST.name(), SAME_ORIGIN);
        httpServletRequest.addHeader(SkipReplicatedCsrfFilter.REPLICATED_REQUEST_HEADER, UUID.randomUUID().toString());

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertAllowed();
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "OPTIONS", "TRACE"})
    void testDoFilterSafeMethodCrossSiteAllowed(final String method) throws ServletException, IOException {
        setRequest(method, CROSS_SITE);

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertAllowed();
    }

    @ParameterizedTest
    @EnumSource(value = SamlUrlPath.class, names = {"LOGIN_RESPONSE", "SINGLE_LOGOUT_RESPONSE"})
    void testDoFilterSamlConsumerCrossSiteAllowed(final SamlUrlPath samlUrlPath) throws ServletException, IOException {
        setRequest(HttpMethod.POST.name(), CROSS_SITE);
        httpServletRequest.setRequestURI(samlUrlPath.getPath());

        filter.doFilter(httpServletRequest, httpServletResponse, filterChain);

        assertAllowed();
    }

    private void setRequest(final String method, final String... fetchSiteValues) {
        httpServletRequest = new MockHttpServletRequest(method, REQUEST_URI);
        for (final String fetchSiteValue : fetchSiteValues) {
            httpServletRequest.addHeader(FetchSiteRequestFilter.FETCH_SITE_HEADER, fetchSiteValue);
        }
    }

    private void assertRejected() {
        assertEquals(HttpServletResponse.SC_FORBIDDEN, httpServletResponse.getStatus());
        verifyNoInteractions(filterChain);
    }

    private void assertAllowed() throws ServletException, IOException {
        verify(filterChain).doFilter(eq(httpServletRequest), eq(httpServletResponse));
    }
}
