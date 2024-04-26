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
package org.apache.nifi.web.security.oidc.client.web;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.Cache;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;

import jakarta.servlet.http.Cookie;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardAuthorizationRequestRepositoryTest {
    private static final String AUTHORIZATION_REQUEST_URI = "http://localhost/authorize";

    private static final String CLIENT_ID = "client-id";

    private static final int MAX_AGE_EXPIRED = 0;

    @Captor
    ArgumentCaptor<String> identifierCaptor;

    @Mock
    Cache cache;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    StandardAuthorizationRequestRepository repository;

    @BeforeEach
    void setRepository() {
        repository = new StandardAuthorizationRequestRepository(cache);
        httpServletRequest = new MockHttpServletRequest();
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testLoadAuthorizationRequestNotFound() {
        final OAuth2AuthorizationRequest authorizationRequest = repository.loadAuthorizationRequest(httpServletRequest);

        assertNull(authorizationRequest);
    }

    @Test
    void testLoadAuthorizationRequestFound() {
        final String identifier = UUID.randomUUID().toString();
        httpServletRequest.setCookies(new Cookie(ApplicationCookieName.OIDC_REQUEST_IDENTIFIER.getCookieName(), identifier));

        final OAuth2AuthorizationRequest authorizationRequest = getAuthorizationRequest();
        when(cache.get(eq(identifier), eq(OAuth2AuthorizationRequest.class))).thenReturn(authorizationRequest);

        final OAuth2AuthorizationRequest loadedAuthorizationRequest = repository.loadAuthorizationRequest(httpServletRequest);

        assertEquals(authorizationRequest, loadedAuthorizationRequest);
    }

    @Test
    void testRemoveAuthorizationRequestNotFound() {
        final OAuth2AuthorizationRequest authorizationRequest = repository.removeAuthorizationRequest(httpServletRequest, httpServletResponse);

        assertNull(authorizationRequest);
    }

    @Test
    void testRemoveAuthorizationRequestFound() {
        final String identifier = UUID.randomUUID().toString();
        httpServletRequest.setCookies(new Cookie(ApplicationCookieName.OIDC_REQUEST_IDENTIFIER.getCookieName(), identifier));

        final OAuth2AuthorizationRequest authorizationRequest = getAuthorizationRequest();
        when(cache.get(eq(identifier), eq(OAuth2AuthorizationRequest.class))).thenReturn(authorizationRequest);

        final OAuth2AuthorizationRequest removedAuthorizationRequest = repository.removeAuthorizationRequest(httpServletRequest, httpServletResponse);

        assertEquals(authorizationRequest, removedAuthorizationRequest);

        verify(cache).evict(identifierCaptor.capture());
        final String evictedIdentifier = identifierCaptor.getValue();
        assertEquals(identifier, evictedIdentifier);

        final Cookie cookie = httpServletResponse.getCookie(ApplicationCookieName.OIDC_REQUEST_IDENTIFIER.getCookieName());
        assertNotNull(cookie);
        assertEquals(MAX_AGE_EXPIRED, cookie.getMaxAge());
    }

    @Test
    void testSaveAuthorizationRequest() {
        final OAuth2AuthorizationRequest authorizationRequest = getAuthorizationRequest();

        repository.saveAuthorizationRequest(authorizationRequest, httpServletRequest, httpServletResponse);

        verify(cache).put(identifierCaptor.capture(), eq(authorizationRequest));
        final String identifier = identifierCaptor.getValue();
        assertCookieFound(identifier);
    }

    private void assertCookieFound(final String identifier) {
        final Cookie cookie = httpServletResponse.getCookie(ApplicationCookieName.OIDC_REQUEST_IDENTIFIER.getCookieName());
        assertNotNull(cookie);

        final String cookieValue = cookie.getValue();
        assertEquals(identifier, cookieValue);
    }

    private OAuth2AuthorizationRequest getAuthorizationRequest() {
        return OAuth2AuthorizationRequest.authorizationCode()
                .authorizationRequestUri(AUTHORIZATION_REQUEST_URI)
                .authorizationUri(AUTHORIZATION_REQUEST_URI)
                .clientId(CLIENT_ID)
                .build();
    }
}
