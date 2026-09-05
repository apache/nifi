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

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.Cookie;
import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.saml2.web.authentication.identity.AttributeNameIdentityConverter;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.saml2.provider.service.authentication.Saml2AssertionAuthentication;
import org.springframework.security.saml2.provider.service.authentication.Saml2ResponseAssertion;
import org.springframework.security.saml2.provider.service.authentication.Saml2ResponseAssertionAccessor;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class Saml2AuthenticationSuccessHandlerTest {
    private static final Duration EXPIRATION = Duration.ofMinutes(1);

    private static final String IDENTITY = Authentication.class.getSimpleName();

    private static final String AUTHORITY = GrantedAuthority.class.getSimpleName();

    private static final String REQUEST_URI = "/nifi-api";

    private static final String UI_PATH = "/nifi/";

    private static final int SERVER_PORT = 8080;

    private static final String LOCALHOST_URL = "http://localhost:8080";

    private static final String TARGET_URL = String.format("%s%s", LOCALHOST_URL, UI_PATH);

    static final String FORWARDED_PATH = "/forwarded";

    static final String FORWARDED_COOKIE_PATH = String.format("%s/", FORWARDED_PATH);

    private static final String FORWARDED_TARGET_URL = String.format("%s%s%s", LOCALHOST_URL, FORWARDED_PATH, UI_PATH);

    private static final String FIRST_GROUP = "$1";

    private static final Pattern MATCH_PATTERN = Pattern.compile("(.*)");

    static final String ROOT_PATH = "/";

    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER = "allowedContextPaths";

    private static final String IDENTITY_ATTRIBUTE_NAME = "urn:oid:0.9.2342.19200300.100.1.3";

    private static final String IDENTITY_ATTRIBUTE_VALUE = "user@localhost.localdomain";

    private static final String MAPPED_IDENTITY_ATTRIBUTE_VALUE = "USER@LOCALHOST.LOCALDOMAIN";

    private static final String NAME_IDENTIFIER = "name-identifier";

    private static final String MAPPED_NAME_IDENTIFIER = "NAME-IDENTIFIER";

    private static final String RESPONSE_VALUE = "<saml2p:Response/>";

    private static final String REGISTRATION_ID = "consumer";

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

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    Saml2AuthenticationSuccessHandler handler;

    @BeforeEach
    void setHandler() {
        handler = new Saml2AuthenticationSuccessHandler(
                bearerTokenProvider,
                Collections.singletonList(UPPER_IDENTITY_MAPPING),
                Collections.singletonList(LOWER_IDENTITY_MAPPING),
                EXPIRATION
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

    @Test
    void testDetermineTargetUrlAssertionAuthenticationNameIdentifier() {
        httpServletRequest.setRequestURI(REQUEST_URI);

        final Authentication authentication = getAssertionAuthentication(Map.of());
        final String targetUrl = handler.determineTargetUrl(httpServletRequest, httpServletResponse, authentication);

        assertEquals(TARGET_URL, targetUrl);
        assertBearerTokenIdentityEquals(MAPPED_NAME_IDENTIFIER);
    }

    @Test
    void testDetermineTargetUrlAssertionAuthenticationIdentityConverter() {
        handler.setIdentityConverter(new AttributeNameIdentityConverter(IDENTITY_ATTRIBUTE_NAME));
        httpServletRequest.setRequestURI(REQUEST_URI);

        final Authentication authentication = getAssertionAuthentication(Map.of(IDENTITY_ATTRIBUTE_NAME, List.of(IDENTITY_ATTRIBUTE_VALUE)));
        final String targetUrl = handler.determineTargetUrl(httpServletRequest, httpServletResponse, authentication);

        assertEquals(TARGET_URL, targetUrl);
        assertBearerTokenIdentityEquals(MAPPED_IDENTITY_ATTRIBUTE_VALUE);
    }

    void assertTargetUrlEquals(final String expectedTargetUrl) {
        final Authentication authentication = new TestingAuthenticationToken(IDENTITY, IDENTITY, AUTHORITY);

        final String targetUrl = handler.determineTargetUrl(httpServletRequest, httpServletResponse, authentication);

        assertEquals(expectedTargetUrl, targetUrl);
    }

    void assertBearerTokenIdentityEquals(final String expectedIdentity) {
        final ArgumentCaptor<LoginAuthenticationToken> tokenCaptor = ArgumentCaptor.forClass(LoginAuthenticationToken.class);
        verify(bearerTokenProvider).getBearerToken(tokenCaptor.capture());

        assertEquals(expectedIdentity, tokenCaptor.getValue().getName());
    }

    private Authentication getAssertionAuthentication(final Map<String, List<Object>> attributes) {
        final Saml2ResponseAssertionAccessor assertion = Saml2ResponseAssertion.withResponseValue(RESPONSE_VALUE)
                .nameId(NAME_IDENTIFIER)
                .attributes(attributes)
                .build();

        return new Saml2AssertionAuthentication(NAME_IDENTIFIER, assertion, Collections.emptyList(), REGISTRATION_ID);
    }

    void assertBearerCookieAdded(final String expectedCookiePath) {
        final Cookie responseCookie = httpServletResponse.getCookie(ApplicationCookieName.AUTHORIZATION_BEARER.getCookieName());

        assertNotNull(responseCookie);
        assertEquals(expectedCookiePath, responseCookie.getPath());
    }
}
