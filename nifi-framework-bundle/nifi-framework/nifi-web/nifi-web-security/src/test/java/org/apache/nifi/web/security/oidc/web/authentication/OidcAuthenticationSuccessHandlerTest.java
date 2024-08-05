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
package org.apache.nifi.web.security.oidc.web.authentication;

import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.apache.nifi.web.security.oidc.client.web.converter.StandardOAuth2AuthenticationToken;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.Cookie;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OidcAuthenticationSuccessHandlerTest {

    @Mock
    OidcUser oidcUser;

    @Mock
    BearerTokenProvider bearerTokenProvider;

    @Captor
    ArgumentCaptor<LoginAuthenticationToken> authenticationTokenCaptor;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    OidcAuthenticationSuccessHandler handler;

    private static final String REQUEST_URI = "/nifi-api";

    private static final String UI_PATH = "/nifi/";

    private static final String ROOT_PATH = "/";

    private static final int SERVER_PORT = 8080;

    private static final String LOCALHOST_URL = "http://localhost:8080";

    private static final String TARGET_URL = String.format("%s%s", LOCALHOST_URL, UI_PATH);

    private static final String USER_NAME_CLAIM = "email";

    private static final String GROUPS_CLAIM = "groups";

    private static final String IDENTITY = Authentication.class.getSimpleName();

    private static final String AUTHORITY = GrantedAuthority.class.getSimpleName();

    private static final String ACCESS_TOKEN = "access-token";

    private static final Duration TOKEN_EXPIRATION = Duration.ofHours(1);

    private static final Instant ACCESS_TOKEN_ISSUED = Instant.ofEpochSecond(0);

    private static final Instant ACCESS_TOKEN_EXPIRES = ACCESS_TOKEN_ISSUED.plus(TOKEN_EXPIRATION);

    private static final String FIRST_GROUP = "$1";

    private static final Pattern MATCH_PATTERN = Pattern.compile("(.*)");

    static final String FORWARDED_PATH = "/forwarded";

    static final String FORWARDED_COOKIE_PATH = String.format("%s/", FORWARDED_PATH);

    private static final String FORWARDED_TARGET_URL = String.format("%s%s%s", LOCALHOST_URL, FORWARDED_PATH, UI_PATH);

    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER = "allowedContextPaths";

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

    @BeforeEach
    void setHandler() {
        handler = new OidcAuthenticationSuccessHandler(
                bearerTokenProvider,
                Collections.singletonList(UPPER_IDENTITY_MAPPING),
                Collections.singletonList(LOWER_IDENTITY_MAPPING),
                Collections.singletonList(USER_NAME_CLAIM),
                GROUPS_CLAIM
        );
        httpServletRequest = new MockHttpServletRequest();
        httpServletRequest.setServerPort(SERVER_PORT);
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testDetermineTargetUrl() {
        httpServletRequest.setRequestURI(REQUEST_URI);

        assertTargetUrlEquals(TARGET_URL);
        assertBearerCookieAdded(ROOT_PATH);
    }

    @Test
    void testDetermineTargetUrlForwardedPath() {
        final ServletContext servletContext = httpServletRequest.getServletContext();
        servletContext.setInitParameter(ALLOWED_CONTEXT_PATHS_PARAMETER, FORWARDED_PATH);
        httpServletRequest.addHeader(ProxyHeader.FORWARDED_PREFIX.getHeader(), FORWARDED_PATH);

        httpServletRequest.setRequestURI(REQUEST_URI);

        assertTargetUrlEquals(FORWARDED_TARGET_URL);
        assertBearerCookieAdded(FORWARDED_COOKIE_PATH);
    }

    void assertTargetUrlEquals(final String expectedTargetUrl) {
        setOidcUser();

        final StandardOAuth2AuthenticationToken authentication = getAuthenticationToken();

        final String targetUrl = handler.determineTargetUrl(httpServletRequest, httpServletResponse, authentication);

        assertEquals(expectedTargetUrl, targetUrl);
    }

    void assertBearerCookieAdded(final String expectedCookiePath) {
        final Cookie responseCookie = httpServletResponse.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());

        assertNotNull(responseCookie);
        assertEquals(expectedCookiePath, responseCookie.getPath());

        verify(bearerTokenProvider).getBearerToken(authenticationTokenCaptor.capture());

        final LoginAuthenticationToken authenticationToken = authenticationTokenCaptor.getValue();
        final Instant expiration = authenticationToken.getExpiration();

        final ChronoUnit truncation = ChronoUnit.MINUTES;
        final Instant expirationTruncated = expiration.truncatedTo(truncation);
        assertEquals(ACCESS_TOKEN_EXPIRES, expirationTruncated);
    }

    void setOidcUser() {
        when(oidcUser.getClaimAsString(eq(USER_NAME_CLAIM))).thenReturn(IDENTITY);
        when(oidcUser.getClaimAsStringList(eq(GROUPS_CLAIM))).thenReturn(Collections.singletonList(AUTHORITY));
    }

    StandardOAuth2AuthenticationToken getAuthenticationToken() {
        final OAuth2AccessToken accessToken = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, ACCESS_TOKEN, ACCESS_TOKEN_ISSUED, ACCESS_TOKEN_EXPIRES);
        final Collection<? extends GrantedAuthority> authorities = Collections.singletonList(new SimpleGrantedAuthority(AUTHORITY));
        return new StandardOAuth2AuthenticationToken(oidcUser, authorities, OidcRegistrationProperty.REGISTRATION_ID.getProperty(), accessToken);
    }
}
