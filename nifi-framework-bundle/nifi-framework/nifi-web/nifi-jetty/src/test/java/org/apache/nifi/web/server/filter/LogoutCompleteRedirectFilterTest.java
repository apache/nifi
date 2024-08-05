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
package org.apache.nifi.web.server.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LogoutCompleteRedirectFilterTest {
    private static final String LOGOUT_COMPLETE_URI = "/nifi/logout-complete";

    private static final String ROOT_URI = "/nifi/";

    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER = "allowedContextPaths";

    private static final String FORWARDED_PATH = "/forwarded";

    private static final String LOGOUT_COMPLETE_EXPECTED = "/forwarded/nifi/#/logout-complete";

    private static final String CONTEXT_PATH_HEADER = "X-ProxyContextPath";

    @Mock
    private ServletContext servletContext;

    @Mock
    private FilterConfig filterConfig;

    @Mock
    private FilterChain filterChain;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Captor
    private ArgumentCaptor<String> redirectCaptor;

    private LogoutCompleteRedirectFilter filter;

    @BeforeEach
    public void setFilter() throws ServletException {
        filter = new LogoutCompleteRedirectFilter();
        filter.init(filterConfig);
    }

    @Test
    public void testDoFilter() throws ServletException, IOException {
        when(request.getRequestURI()).thenReturn(ROOT_URI);

        filter.doFilter(request, response, filterChain);

        verify(response, never()).sendRedirect(anyString());
    }

    @Test
    public void testDoFilterLogoutComplete() throws ServletException, IOException {
        when(request.getRequestURI()).thenReturn(LOGOUT_COMPLETE_URI);
        when(request.getServletContext()).thenReturn(servletContext);
        when(servletContext.getInitParameter(eq(ALLOWED_CONTEXT_PATHS_PARAMETER))).thenReturn(FORWARDED_PATH);
        when(request.getHeader(eq(CONTEXT_PATH_HEADER))).thenReturn(FORWARDED_PATH);

        filter.doFilter(request, response, filterChain);

        verify(response).sendRedirect(redirectCaptor.capture());

        final String redirect = redirectCaptor.getValue();
        assertEquals(LOGOUT_COMPLETE_EXPECTED, redirect);
    }
}
