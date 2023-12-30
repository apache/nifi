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
package org.apache.nifi.web.security.jwt.resolver;

import org.apache.nifi.web.security.http.SecurityCookieName;
import org.apache.nifi.web.security.http.SecurityHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardBearerTokenResolverTest {
    private static final String BEARER_TOKEN = "TOKEN";

    private StandardBearerTokenResolver resolver;

    @Mock
    private HttpServletRequest request;

    @BeforeEach
    public void setResolver() {
        resolver = new StandardBearerTokenResolver();
    }

    @Test
    public void testResolveAuthorizationHeaderFound() {
        setHeader(String.format("Bearer %s", BEARER_TOKEN));
        assertEquals(BEARER_TOKEN, resolver.resolve(request));
    }

    @Test
    public void testResolveAuthorizationHeaderMissingPrefix() {
        setHeader(BEARER_TOKEN);
        assertNull(resolver.resolve(request));
    }

    @Test
    public void testResolveAuthorizationHeaderIncorrectPrefix() {
        setHeader(String.format("Basic %s", BEARER_TOKEN));
        assertNull(resolver.resolve(request));
    }

    @Test
    public void testResolveCookieFound() {
        final Cookie cookie = new Cookie(SecurityCookieName.AUTHORIZATION_BEARER.getName(), BEARER_TOKEN);
        when(request.getCookies()).thenReturn(new Cookie[]{cookie});
        assertEquals(BEARER_TOKEN, resolver.resolve(request));
    }

    private void setHeader(final String header) {
        when(request.getHeader(eq(SecurityHeader.AUTHORIZATION.getHeader()))).thenReturn(header);
    }
}