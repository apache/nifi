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
package org.apache.nifi.web.security.saml2.web.authentication;

import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.idp.IdpType;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import javax.servlet.http.Cookie;
import java.time.Duration;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class Saml2AuthenticationSuccessHandlerTest {
    private static final String ISSUER = Saml2AuthenticationSuccessHandlerTest.class.getSimpleName();

    private static final Duration EXPIRATION = Duration.ofMinutes(1);

    private static final String IDENTITY = Authentication.class.getSimpleName();

    private static final String IDENTITY_UPPER = IDENTITY.toUpperCase();

    private static final String AUTHORITY = GrantedAuthority.class.getSimpleName();

    private static final String AUTHORITY_LOWER = AUTHORITY.toLowerCase();

    private static final String REQUEST_URI = "/nifi-api";

    private static final int SERVER_PORT = 8080;

    private static final String TARGET_URL = "http://localhost:8080/nifi/";

    private static final String FIRST_GROUP = "$1";

    private static final Pattern MATCH_PATTERN = Pattern.compile("(.*)");

    private static final IdentityMapping UPPER_IDENTITY_MAPPING = new IdentityMapping(
            IdentityMapping.Transform.UPPER.toString(),
            MATCH_PATTERN,
            FIRST_GROUP,
            IdentityMapping.Transform.UPPER
    );

    private static final IdentityMapping LOWER_IDENTITY_MAPPING = new IdentityMapping(
            IdentityMapping.Transform.LOWER.toString(),
            MATCH_PATTERN,
            FIRST_GROUP,
            IdentityMapping.Transform.LOWER
    );

    @Mock
    BearerTokenProvider bearerTokenProvider;

    @Mock
    IdpUserGroupService idpUserGroupService;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    Saml2AuthenticationSuccessHandler handler;

    @BeforeEach
    void setHandler() {
        handler = new Saml2AuthenticationSuccessHandler(
                bearerTokenProvider,
                idpUserGroupService,
                Collections.singletonList(UPPER_IDENTITY_MAPPING),
                Collections.singletonList(LOWER_IDENTITY_MAPPING),
                EXPIRATION,
                ISSUER
        );
        httpServletRequest = new MockHttpServletRequest();
        httpServletRequest.setServerPort(SERVER_PORT);
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testDetermineTargetUrl() {
        httpServletRequest.setRequestURI(REQUEST_URI);

        final Authentication authentication = new TestingAuthenticationToken(IDENTITY, IDENTITY, AUTHORITY);

        final String targetUrl = handler.determineTargetUrl(httpServletRequest, httpServletResponse, authentication);

        assertEquals(TARGET_URL, targetUrl);

        verify(idpUserGroupService).replaceUserGroups(eq(IDENTITY_UPPER), eq(IdpType.SAML), eq(Collections.singleton(AUTHORITY_LOWER)));

        final Cookie bearerCookie = httpServletResponse.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());
        assertNotNull(bearerCookie);
    }
}
