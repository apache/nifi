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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SanitizeContextPathFilterTest {
    @Test
    public void testInitShouldExtractAllowedContextPaths(@Mock ServletContext servletContext, @Mock FilterConfig filterConfig) throws ServletException {
        final String expectedAllowedContextPaths = String.join(",", new String[]{"/path1", "/path2"});
        final Map<String, String> parameters = Collections.singletonMap("allowedContextPaths", expectedAllowedContextPaths);
        when(servletContext.getInitParameter(anyString())).thenAnswer(
                (Answer<String>) invocation -> getValue(invocation.getArgument(0, String.class), parameters));
        when(filterConfig.getServletContext()).thenReturn(servletContext);
        SanitizeContextPathFilter sanitizeContextPathFilter = new SanitizeContextPathFilter();
        sanitizeContextPathFilter.init(filterConfig);

        assertEquals(expectedAllowedContextPaths, sanitizeContextPathFilter.getAllowedContextPaths());
    }

    @Test
    public void testInitShouldHandleBlankAllowedContextPaths(@Mock ServletContext servletContext, @Mock FilterConfig filterConfig) throws ServletException {
        when(servletContext.getInitParameter(anyString())).thenReturn("");
        when(filterConfig.getServletContext()).thenReturn(servletContext);
        SanitizeContextPathFilter sanitizeContextPathFilter = new SanitizeContextPathFilter();
        sanitizeContextPathFilter.init(filterConfig);

        assertEquals("", sanitizeContextPathFilter.getAllowedContextPaths());
    }

    @Test
    public void testShouldInjectContextPathAttribute(@Mock ServletContext servletContext, @Mock FilterConfig filterConfig,
                                                     @Mock HttpServletRequest httpServletRequest) throws ServletException {
        final String expectedAllowedContextPaths = String.join(",", new String[]{"/path1", "/path2"});
        final String expectedForwardPath = "index.jsp";
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("allowedContextPaths", expectedAllowedContextPaths);
        parameters.put("forwardPath", expectedForwardPath);
        final String expectedContextPath = "/path1";
        final Map<String, String> headers = new HashMap<>();
        headers.put("X-ProxyContextPath", "path1");
        headers.put("X-Forwarded-Context", "");
        headers.put("X-Forwarded-Prefix", "");
        final Map<String, Object> requestAttributes = new HashMap<>();

        when(servletContext.getInitParameter(anyString())).thenAnswer(
                (Answer<String>) invocation -> getValue(invocation.getArgument(0, String.class), parameters));
        when(filterConfig.getServletContext()).thenReturn(servletContext);
        when(httpServletRequest.getHeader(anyString())).thenAnswer(
                (Answer<String>) invocation -> getValue(invocation.getArgument(0, String.class), headers));
        doAnswer((Answer<Void>) invocation -> {
            requestAttributes.put(invocation.getArgument(0, String.class), invocation.getArgument(1, Object.class));
            return null;
        }).when(httpServletRequest).setAttribute(anyString(), any());

        SanitizeContextPathFilter sanitizeContextPathFilter = new SanitizeContextPathFilter();
        sanitizeContextPathFilter.init(filterConfig);
        sanitizeContextPathFilter.injectContextPathAttribute(httpServletRequest);

        assertEquals(expectedContextPath, requestAttributes.get("contextPath"));
    }

    private static String getValue(String parameterName, Map<String, String> params) {
        return params != null && params.containsKey(parameterName) ? params.get(parameterName) : "";
    }
}
