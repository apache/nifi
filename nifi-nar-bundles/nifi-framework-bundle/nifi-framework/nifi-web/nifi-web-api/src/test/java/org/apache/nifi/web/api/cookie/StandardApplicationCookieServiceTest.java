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
package org.apache.nifi.web.api.cookie;

import org.apache.nifi.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.mock.web.MockCookie;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StandardApplicationCookieServiceTest {
    private static final String DOMAIN = "localhost.localdomain";

    private static final String RESOURCE_URI = String.format("https://%s", DOMAIN);

    private static final String ROOT_PATH = "/";

    private static final String CONTEXT_PATH = "/context";

    private static final String CONTEXT_RESOURCE_URI = String.format("https://%s%s", DOMAIN, CONTEXT_PATH);

    private static final int EXPECTED_MAX_AGE = 60;

    private static final int SESSION_MAX_AGE = -1;

    private static final int REMOVE_MAX_AGE = 0;

    private static final String SAME_SITE_STRICT = "SameSite=Strict";

    private static final String COOKIE_VALUE = UUID.randomUUID().toString();

    private static final ApplicationCookieName COOKIE_NAME = ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER;

    private URI resourceUri;

    private URI contextResourceUri;

    private StandardApplicationCookieService service;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Captor
    private ArgumentCaptor<String> cookieArgumentCaptor;

    @Before
    public void setService() {
        service = new StandardApplicationCookieService();
        resourceUri = URI.create(RESOURCE_URI);
        contextResourceUri = URI.create(CONTEXT_RESOURCE_URI);
    }

    @Test
    public void testAddCookie() {
        service.addCookie(resourceUri, response, COOKIE_NAME, COOKIE_VALUE);

        verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieArgumentCaptor.capture());
        final String setCookieHeader = cookieArgumentCaptor.getValue();
        assertAddCookieMatches(setCookieHeader, ROOT_PATH, EXPECTED_MAX_AGE);
    }

    @Test
    public void testAddCookieContextPath() {
        service.addCookie(contextResourceUri, response, COOKIE_NAME, COOKIE_VALUE);

        verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieArgumentCaptor.capture());
        final String setCookieHeader = cookieArgumentCaptor.getValue();
        assertAddCookieMatches(setCookieHeader, CONTEXT_PATH, EXPECTED_MAX_AGE);
    }

    @Test
    public void testAddSessionCookie() {
        service.addSessionCookie(resourceUri, response, COOKIE_NAME, COOKIE_VALUE);

        verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieArgumentCaptor.capture());

        final String setCookieHeader = cookieArgumentCaptor.getValue();
        assertAddCookieMatches(setCookieHeader, ROOT_PATH, SESSION_MAX_AGE);
        assertTrue("SameSite not found", setCookieHeader.endsWith(SAME_SITE_STRICT));
    }

    @Test
    public void testAddSessionCookieContextPath() {
        service.addSessionCookie(contextResourceUri, response, COOKIE_NAME, COOKIE_VALUE);

        verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieArgumentCaptor.capture());

        final String setCookieHeader = cookieArgumentCaptor.getValue();
        assertAddCookieMatches(setCookieHeader, CONTEXT_PATH, SESSION_MAX_AGE);
        assertTrue("SameSite not found", setCookieHeader.endsWith(SAME_SITE_STRICT));
    }

    @Test
    public void testGetCookieValue() {
        final Cookie cookie = new Cookie(COOKIE_NAME.getCookieName(), COOKIE_VALUE);
        when(request.getCookies()).thenReturn(new Cookie[]{cookie});
        final Optional<String> cookieValue = service.getCookieValue(request, COOKIE_NAME);
        assertTrue(cookieValue.isPresent());
        assertEquals(COOKIE_VALUE, cookieValue.get());
    }

    @Test
    public void testGetCookieValueEmpty() {
        final Optional<String> cookieValue = service.getCookieValue(request, COOKIE_NAME);
        assertFalse(cookieValue.isPresent());
    }

    @Test
    public void testRemoveCookie() {
        service.removeCookie(resourceUri, response, COOKIE_NAME);

        verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieArgumentCaptor.capture());
        final String setCookieHeader = cookieArgumentCaptor.getValue();
        assertRemoveCookieMatches(setCookieHeader, ROOT_PATH);
    }

    @Test
    public void testRemoveCookieContextPath() {
        service.removeCookie(contextResourceUri, response, COOKIE_NAME);

        verify(response).addHeader(eq(HttpHeaders.SET_COOKIE), cookieArgumentCaptor.capture());
        final String setCookieHeader = cookieArgumentCaptor.getValue();
        assertRemoveCookieMatches(setCookieHeader, CONTEXT_PATH);
    }

    private void assertAddCookieMatches(final String setCookieHeader, final String path, final long maxAge) {
        final Cookie cookie = MockCookie.parse(setCookieHeader);
        assertCookieMatches(setCookieHeader, cookie, path);
        assertEquals(COOKIE_VALUE, cookie.getValue());
        assertEquals(maxAge, cookie.getMaxAge());
    }

    private void assertRemoveCookieMatches(final String setCookieHeader, final String path) {
        final Cookie cookie = MockCookie.parse(setCookieHeader);
        assertCookieMatches(setCookieHeader, cookie, path);
        assertEquals(StringUtils.EMPTY, cookie.getValue());
        assertEquals(REMOVE_MAX_AGE, cookie.getMaxAge());
    }

    private void assertCookieMatches(final String setCookieHeader, final Cookie cookie, final String path) {
        assertEquals("Cookie Name not matched", COOKIE_NAME.getCookieName(), cookie.getName());
        assertEquals("Path not matched", path, cookie.getPath());
        assertEquals("Domain not matched", DOMAIN, cookie.getDomain());
        assertTrue("HTTP Only not matched", cookie.isHttpOnly());
        assertTrue("Secure not matched", cookie.getSecure());
    }
}
