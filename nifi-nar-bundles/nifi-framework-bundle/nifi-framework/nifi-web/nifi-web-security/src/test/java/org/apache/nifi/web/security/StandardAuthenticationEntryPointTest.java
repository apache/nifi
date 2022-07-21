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

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardAuthenticationEntryPointTest {
    static final String FAILED = "Authentication Failed";

    static final String BEARER_TOKEN = "Bearer Token";

    MockHttpServletRequest request;

    MockHttpServletResponse response;

    StandardAuthenticationEntryPoint authenticationEntryPoint;

    @BeforeEach
    void setAuthenticationEntryPoint() {
        final BearerTokenAuthenticationEntryPoint bearerTokenAuthenticationEntryPoint = new BearerTokenAuthenticationEntryPoint();
        authenticationEntryPoint = new StandardAuthenticationEntryPoint(bearerTokenAuthenticationEntryPoint);

        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
    }

    @Test
    void testCommenceAuthenticationServiceException() throws ServletException, IOException {
        final AuthenticationException exception = new AuthenticationServiceException(FAILED);

        authenticationEntryPoint.commence(request, response, exception);

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
        final String authenticateHeader = response.getHeader(StandardAuthenticationEntryPoint.AUTHENTICATE_HEADER);
        assertNull(authenticateHeader);

        final Cookie cookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNull(cookie);

        final String content = response.getContentAsString();
        assertEquals(StandardAuthenticationEntryPoint.UNAUTHORIZED, content);
    }

    @Test
    void testCommenceOAuth2AuthenticationException() throws ServletException, IOException {
        final OAuth2AuthenticationException exception = new OAuth2AuthenticationException(FAILED);

        authenticationEntryPoint.commence(request, response, exception);

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
        final String authenticateHeader = response.getHeader(StandardAuthenticationEntryPoint.AUTHENTICATE_HEADER);
        assertNotNull(authenticateHeader);
        assertTrue(authenticateHeader.startsWith(StandardAuthenticationEntryPoint.BEARER_HEADER), "Bearer header not found");
        assertTrue(authenticateHeader.contains(FAILED), "Header error message not found");

        final Cookie cookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNull(cookie);

        final String content = response.getContentAsString();
        assertTrue(content.startsWith(StandardAuthenticationEntryPoint.UNAUTHORIZED), "Unauthorized message not found");
        assertTrue(content.contains(FAILED), "Response error message not found");
    }

    @Test
    void testCommenceRemoveCookie() throws ServletException, IOException {
        final AuthenticationException exception = new AuthenticationServiceException(FAILED);

        final Cookie cookie = new Cookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName(), BEARER_TOKEN);
        request.setCookies(cookie);
        authenticationEntryPoint.commence(request, response, exception);

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());

        final Cookie responseCookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNotNull(responseCookie);

        final String content = response.getContentAsString();
        assertEquals(StandardAuthenticationEntryPoint.UNAUTHORIZED, content);
    }
}
