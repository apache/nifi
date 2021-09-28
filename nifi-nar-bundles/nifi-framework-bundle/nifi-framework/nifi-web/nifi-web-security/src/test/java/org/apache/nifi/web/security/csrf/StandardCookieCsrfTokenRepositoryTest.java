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
import org.apache.nifi.web.util.WebUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.web.csrf.CsrfToken;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardCookieCsrfTokenRepositoryTest {
    private static final int MAX_AGE_SESSION = -1;

    private static final int MAX_AGE_EXPIRED = 0;

    private static final String ROOT_PATH = "/";

    private static final String CONTEXT_PATH = "/context-path";

    private static final String COOKIE_CONTEXT_PATH = CONTEXT_PATH + ROOT_PATH;

    private static final String HTTPS = "https";

    private static final String HOST = "localhost";

    private static final String PORT = "443";

    private static final String EMPTY = "";

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Captor
    private ArgumentCaptor<Cookie> cookieArgumentCaptor;

    private StandardCookieCsrfTokenRepository repository;

    @BeforeEach
    public void setRepository() {
        this.repository = new StandardCookieCsrfTokenRepository(Collections.emptyList());
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
        when(request.getCookies()).thenReturn(new Cookie[]{cookie});

        final CsrfToken csrfToken = repository.generateToken(request);
        assertNotNull(csrfToken);
        assertEquals(token, csrfToken.getToken());
    }

    @Test
    public void testLoadToken() {
        final String token = UUID.randomUUID().toString();
        final Cookie cookie = new Cookie(SecurityCookieName.REQUEST_TOKEN.getName(), token);
        when(request.getCookies()).thenReturn(new Cookie[]{cookie});

        final CsrfToken csrfToken = repository.loadToken(request);
        assertNotNull(csrfToken);
        assertEquals(token, csrfToken.getToken());
    }

    @Test
    public void testSaveToken() {
        final CsrfToken csrfToken = repository.generateToken(request);
        repository.saveToken(csrfToken, request, response);

        verify(response).addCookie(cookieArgumentCaptor.capture());
        final Cookie cookie = cookieArgumentCaptor.getValue();
        assertCookieEquals(csrfToken, cookie);
        assertEquals(ROOT_PATH, cookie.getPath());
    }

    @Test
    public void testSaveTokenNullCsrfToken() {
        repository.saveToken(null, request, response);

        verify(response).addCookie(cookieArgumentCaptor.capture());
        final Cookie cookie = cookieArgumentCaptor.getValue();
        assertEquals(ROOT_PATH, cookie.getPath());
        assertEquals(EMPTY, cookie.getValue());
        assertEquals(MAX_AGE_EXPIRED, cookie.getMaxAge());
        assertTrue(cookie.getSecure());
        assertFalse(cookie.isHttpOnly());
        assertNull(cookie.getDomain());
    }

    @Test
    public void testSaveTokenProxyContextPath() {
        this.repository = new StandardCookieCsrfTokenRepository(Collections.singletonList(CONTEXT_PATH));

        final CsrfToken csrfToken = repository.generateToken(request);
        when(request.getHeader(eq(WebUtils.PROXY_SCHEME_HTTP_HEADER))).thenReturn(HTTPS);
        when(request.getHeader(eq(WebUtils.PROXY_HOST_HTTP_HEADER))).thenReturn(HOST);
        when(request.getHeader(eq(WebUtils.PROXY_PORT_HTTP_HEADER))).thenReturn(PORT);
        when(request.getHeader(eq(WebUtils.PROXY_CONTEXT_PATH_HTTP_HEADER))).thenReturn(CONTEXT_PATH);
        repository.saveToken(csrfToken, request, response);

        verify(response).addCookie(cookieArgumentCaptor.capture());
        final Cookie cookie = cookieArgumentCaptor.getValue();
        assertCookieEquals(csrfToken, cookie);
        assertEquals(COOKIE_CONTEXT_PATH, cookie.getPath());
    }

    private void assertCookieEquals(final CsrfToken csrfToken, final Cookie cookie) {
        assertEquals(csrfToken.getToken(), cookie.getValue());
        assertEquals(MAX_AGE_SESSION, cookie.getMaxAge());
        assertTrue(cookie.getSecure());
        assertFalse(cookie.isHttpOnly());
        assertNull(cookie.getDomain());
    }
}
