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
package org.apache.nifi.web.security;

import org.eclipse.jetty.servlet.FilterHolder;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ContentSecurityPolicyFilterTest {

    @Test
    public void testCSPHeaderApplied() throws ServletException, IOException {
        // Arrange

        FilterHolder originFilter = new FilterHolder(new ContentSecurityPolicyFilter());

        // Set up request
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        originFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("frame-ancestors 'self'", mockResponse.getHeader("Content-Security-Policy"));
    }

    @Test
    public void testCSPHeaderAppliedOnlyOnce() throws ServletException, IOException {
        // Arrange

        FilterHolder originFilter = new FilterHolder(new ContentSecurityPolicyFilter());

        // Set up request
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        originFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);
        originFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("frame-ancestors 'self'", mockResponse.getHeader("Content-Security-Policy"));
    }

}