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
import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.server.resource.InvalidBearerTokenException;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardAuthenticationEntryPointTest {
    static final String FAILED = "Authentication Failed";

    static final String BEARER_TOKEN = "Bearer Token";

    static final String ROOT_PATH = "/";

    static final String FORWARDED_PATH = "/forwarded";

    static final String FORWARDED_COOKIE_PATH = String.format("%s/", FORWARDED_PATH);

    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER = "allowedContextPaths";

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
    void testCommenceAuthenticationServiceException() throws IOException {
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
    void testCommenceOAuth2AuthenticationException() throws IOException {
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
    void testCommenceInvalidBearerTokenExceptionExpired() throws IOException {
        final InvalidBearerTokenException exception = new InvalidBearerTokenException(StandardAuthenticationEntryPoint.EXPIRED_JWT);

        authenticationEntryPoint.commence(request, response, exception);

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());
        final String authenticateHeader = response.getHeader(StandardAuthenticationEntryPoint.AUTHENTICATE_HEADER);
        assertNotNull(authenticateHeader);
        assertTrue(authenticateHeader.startsWith(StandardAuthenticationEntryPoint.BEARER_HEADER), "Bearer header not found");
        assertTrue(authenticateHeader.contains(StandardAuthenticationEntryPoint.EXPIRED_JWT), "Header error message not found");

        final Cookie cookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNull(cookie);

        final String content = response.getContentAsString();
        assertEquals(StandardAuthenticationEntryPoint.SESSION_EXPIRED, content);
    }

    @Test
    void testCommenceRemoveCookie() throws IOException {
        final AuthenticationException exception = new AuthenticationServiceException(FAILED);

        final Cookie cookie = new Cookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName(), BEARER_TOKEN);
        request.setCookies(cookie);
        authenticationEntryPoint.commence(request, response, exception);

        assertResponseStatusUnauthorized();
        assertBearerCookieRemoved(ROOT_PATH);
    }

    @Test
    void testCommenceRemoveCookieForwardedPath() throws IOException {
        final AuthenticationException exception = new AuthenticationServiceException(FAILED);

        final ServletContext servletContext = request.getServletContext();
        servletContext.setInitParameter(ALLOWED_CONTEXT_PATHS_PARAMETER, FORWARDED_PATH);

        request.addHeader(ProxyHeader.FORWARDED_PREFIX.getHeader(), FORWARDED_PATH);

        final Cookie cookie = new Cookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName(), BEARER_TOKEN);
        request.setCookies(cookie);
        authenticationEntryPoint.commence(request, response, exception);

        assertResponseStatusUnauthorized();
        assertBearerCookieRemoved(FORWARDED_COOKIE_PATH);
    }

    void assertResponseStatusUnauthorized() throws UnsupportedEncodingException {
        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());

        final String content = response.getContentAsString();
        assertEquals(StandardAuthenticationEntryPoint.UNAUTHORIZED, content);
    }

    void assertBearerCookieRemoved(final String expectedCookiePath) {
        final Cookie responseCookie = response.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());

        assertNotNull(responseCookie);
        assertEquals(expectedCookiePath, responseCookie.getPath());
    }
}
