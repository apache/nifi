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
package org.apache.nifi.web.security.headers;

import org.apache.nifi.web.security.headers.ContentSecurityPolicyFilter;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HTTPHeaderFiltersTest {

    @Test
    public void testCSPHeaderApplied() throws ServletException, IOException, Exception {
        // Arrange

        FilterHolder cspFilter = new FilterHolder(new ContentSecurityPolicyFilter());

        // Set up request
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        cspFilter.setServletHandler(new ServletHandler());
        cspFilter.start();
        cspFilter.initialize();
        cspFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("frame-ancestors 'self'", mockResponse.getHeader("Content-Security-Policy"));
    }

    @Test
    public void testCSPHeaderAppliedOnlyOnce() throws ServletException, IOException, Exception {
        // Arrange

        FilterHolder cspFilter = new FilterHolder(new ContentSecurityPolicyFilter());

        // Set up request
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        cspFilter.setServletHandler(new ServletHandler());
        cspFilter.start();
        cspFilter.initialize();
        cspFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);
        cspFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("frame-ancestors 'self'", mockResponse.getHeader("Content-Security-Policy"));
    }


    @Test
    public void testXFrameOptionsHeaderApplied() throws ServletException, IOException, Exception {
        // Arrange

        FilterHolder xfoFilter = new FilterHolder(new XFrameOptionsFilter());

        // Set up request
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        xfoFilter.setServletHandler(new ServletHandler());
        xfoFilter.start();
        xfoFilter.initialize();
        xfoFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("SAMEORIGIN", mockResponse.getHeader("X-Frame-Options"));
    }

    @Test
    public void testHSTSHeaderApplied() throws ServletException, IOException, Exception {
        // Arrange

        FilterHolder hstsFilter = new FilterHolder(new StrictTransportSecurityFilter());

        // Set up request
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        hstsFilter.setServletHandler(new ServletHandler());
        hstsFilter.start();
        hstsFilter.initialize();
        hstsFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("max-age=31540000", mockResponse.getHeader("Strict-Transport-Security"));
    }

    @Test
    public void testXSSProtectionHeaderApplied() throws ServletException, IOException, Exception {
        // Arrange

        FilterHolder xssFilter = new FilterHolder(new XSSProtectionFilter());

        // Set up request
        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        xssFilter.setServletHandler(new ServletHandler());
        xssFilter.start();
        xssFilter.initialize();
        xssFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("1; mode=block", mockResponse.getHeader("X-XSS-Protection"));
    }

    @Test
    public void testXContentTypeOptionsHeaderApplied() throws Exception {
        // Arrange
        FilterHolder xContentTypeFilter = new FilterHolder(new XContentTypeOptionsFilter());

        HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
        MockHttpServletResponse mockResponse = new MockHttpServletResponse();
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);

        // Action
        xContentTypeFilter.setServletHandler(new ServletHandler());
        xContentTypeFilter.start();
        xContentTypeFilter.initialize();
        xContentTypeFilter.getFilter().doFilter(mockRequest, mockResponse, mockFilterChain);

        // Verify
        assertEquals("nosniff", mockResponse.getHeader("X-Content-Type-Options"));
    }
}