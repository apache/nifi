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
import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.springframework.security.web.csrf.CsrfToken;

import jakarta.servlet.http.Cookie;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardCookieCsrfTokenRepositoryTest {
    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER = "allowedContextPaths";

    private static final int MAX_AGE_SESSION = -1;

    private static final int MAX_AGE_EXPIRED = 0;

    private static final String ROOT_PATH = "/";

    private static final String CONTEXT_PATH = "/context-path";

    private static final String HTTPS = "https";

    private static final String HOST = "localhost";

    private static final String PORT = "443";

    private static final String EMPTY = "";

    private static final String SET_COOKIE_HEADER = "Set-Cookie";

    private static final String SAME_SITE = "SameSite";

    private MockHttpServletRequest request;

    private MockHttpServletResponse response;

    private StandardCookieCsrfTokenRepository repository;

    @BeforeEach
    public void setRepository() {
        this.repository = new StandardCookieCsrfTokenRepository();
        this.request = new MockHttpServletRequest();
        this.response = new MockHttpServletResponse();
    }

    @Test
    public void testGenerateToken() {
        final CsrfToken csrfToken = repository.generateToken(request);
        assertNotNull(csrfToken);
        assertNotNull(csrfToken.getToken());
    }

    @Test
    public void testGenerateTokenCookieFound() {
        final String token = UUID.randomUUID().toString();
        final Cookie cookie = new Cookie(SecurityCookieName.REQUEST_TOKEN.getName(), token);
        request.setCookies(cookie);

        final CsrfToken csrfToken = repository.generateToken(request);
        assertNotNull(csrfToken);
        assertEquals(token, csrfToken.getToken());
    }

    @Test
    public void testLoadToken() {
        final String token = UUID.randomUUID().toString();
        final Cookie cookie = new Cookie(SecurityCookieName.REQUEST_TOKEN.getName(), token);
        request.setCookies(cookie);

        final CsrfToken csrfToken = repository.loadToken(request);
        assertNotNull(csrfToken);
        assertEquals(token, csrfToken.getToken());
    }

    @Test
    public void testSaveToken() {
        final CsrfToken csrfToken = repository.generateToken(request);
        repository.saveToken(csrfToken, request, response);

        final Cookie cookie = assertCookieFound();
        final String setCookieHeader = response.getHeader(SET_COOKIE_HEADER);
        assertCookieEquals(csrfToken.getToken(), MAX_AGE_SESSION, cookie, setCookieHeader);
        assertEquals(ROOT_PATH, cookie.getPath());
    }

    @Test
    public void testSaveTokenNullCsrfToken() {
        repository.saveToken(null, request, response);

        final Cookie cookie = assertCookieFound();
        final String setCookieHeader = response.getHeader(SET_COOKIE_HEADER);
        assertCookieEquals(EMPTY, MAX_AGE_EXPIRED, cookie, setCookieHeader);
    }

    @Test
    public void testSaveTokenProxyContextPath() {
        this.repository = new StandardCookieCsrfTokenRepository();

        final CsrfToken csrfToken = repository.generateToken(request);

        request.addHeader(ProxyHeader.PROXY_SCHEME.getHeader(), HTTPS);
        request.addHeader(ProxyHeader.PROXY_HOST.getHeader(), HOST);
        request.addHeader(ProxyHeader.PROXY_PORT.getHeader(), PORT);
        request.addHeader(ProxyHeader.PROXY_PORT.getHeader(), PORT);
        request.addHeader(ProxyHeader.PROXY_CONTEXT_PATH.getHeader(), CONTEXT_PATH);

        final MockServletContext servletContext = (MockServletContext) request.getServletContext();
        servletContext.setInitParameter(ALLOWED_CONTEXT_PATHS_PARAMETER, CONTEXT_PATH);

        repository.saveToken(csrfToken, request, response);

        final Cookie cookie = assertCookieFound();
        final String setCookieHeader = response.getHeader(SET_COOKIE_HEADER);
        assertCookieEquals(csrfToken.getToken(), MAX_AGE_SESSION, cookie, setCookieHeader);
        assertEquals(CONTEXT_PATH, cookie.getPath());
    }

    private Cookie assertCookieFound() {
        final Cookie cookie = response.getCookie(SecurityCookieName.REQUEST_TOKEN.getName());
        assertNotNull(cookie);
        return cookie;
    }

    private void assertCookieEquals(final String token, final int maxAge, final Cookie cookie, final String setCookieHeader) {
        assertNotNull(setCookieHeader);
        assertEquals(token, cookie.getValue());
        assertEquals(maxAge, cookie.getMaxAge());
        assertTrue(cookie.getSecure());
        assertFalse(cookie.isHttpOnly());
        assertEquals(HOST, cookie.getDomain());
        assertTrue(setCookieHeader.contains(SAME_SITE), "SameSite not found");
    }
}
