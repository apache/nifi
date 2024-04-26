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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CatchAllFilterTest {
    @Mock
    private ServletContext servletContext;

    @Mock
    private FilterConfig filterConfig;

    @BeforeEach
    public void setUp() {
        when(filterConfig.getServletContext()).thenReturn(servletContext);
    }

    @Test
    public void testInitShouldCallSuper() throws ServletException {
        String expectedAllowedContextPaths = getExpectedAllowedContextPaths();
        final Map<String, String> parameters = Collections.singletonMap("allowedContextPaths", getExpectedAllowedContextPaths());
        when(servletContext.getInitParameter(anyString())).thenAnswer(invocation -> getValue(invocation.getArgument(0), parameters));
        when(filterConfig.getInitParameter(anyString())).thenAnswer(invocation -> getValue(invocation.getArgument(0), parameters));

        CatchAllFilter catchAllFilter = new CatchAllFilter();
        catchAllFilter.init(filterConfig);

        assertEquals(expectedAllowedContextPaths, catchAllFilter.getAllowedContextPaths());
    }

    @Test
    public void testShouldDoFilter(@Mock HttpServletRequest request, @Mock RequestDispatcher requestDispatcher,
                            @Mock HttpServletResponse response, @Mock FilterChain filterChain ) throws ServletException, IOException {
        final String expectedAllowedContextPaths = getExpectedAllowedContextPaths();
        final String expectedForwardPath = "index.jsp";
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("allowedContextPaths", expectedAllowedContextPaths);
        parameters.put("forwardPath", expectedForwardPath);
        final Map<String, Object> requestAttributes = new HashMap<>();
        final String[] forwardedRequestTo = new String[1];
        final Map<String, String> headers = new HashMap<>();
        headers.put("X-ProxyContextPath", "");
        headers.put("X-Forwarded-Context", "");
        headers.put("X-Forwarded-Prefix", "");

        when(servletContext.getInitParameter(anyString())).thenAnswer(invocation -> getValue(invocation.getArgument(0), parameters));
        when(filterConfig.getInitParameter(anyString())).thenAnswer(invocation -> getValue(invocation.getArgument(0), parameters));
        when(request.getHeader(anyString())).thenAnswer(invocation -> getValue(invocation.getArgument(0), headers));
        doAnswer(invocation -> requestAttributes.put(invocation.getArgument(0), invocation.getArgument(1))).when(request).setAttribute(anyString(), any());
        when(request.getRequestDispatcher(anyString())).thenAnswer(outerInvocation -> {
            doAnswer(innerInvocation -> forwardedRequestTo[0] = outerInvocation.getArgument(0)).when(requestDispatcher).forward(any(), any());
            return requestDispatcher;});

        CatchAllFilter catchAllFilter = new CatchAllFilter();
        catchAllFilter.init(filterConfig);
        catchAllFilter.doFilter(request, response, filterChain);

        assertEquals(expectedForwardPath, forwardedRequestTo[0]);
    }

    private String getExpectedAllowedContextPaths() {
        return String.join(",", "/path1", "/path2");
    }

    private static String getValue(String parameterName, Map<String, String> params) {
        return params.getOrDefault(parameterName, "");
    }
}
