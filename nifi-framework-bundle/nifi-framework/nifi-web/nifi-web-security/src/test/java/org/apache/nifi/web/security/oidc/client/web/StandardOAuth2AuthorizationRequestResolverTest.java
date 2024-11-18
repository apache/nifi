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

import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;

import jakarta.servlet.ServletContext;
import org.springframework.security.oauth2.core.endpoint.PkceParameterNames;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardOAuth2AuthorizationRequestResolverTest {
    private static final String REDIRECT_URI = "https://localhost:8443/nifi-api/callback";

    private static final int FORWARDED_HTTPS_PORT = 443;

    private static final String FORWARDED_PATH = "/forwarded";

    private static final String FORWARDED_REDIRECT_URI = String.format("https://localhost.localdomain:%d%s/nifi-api/callback", FORWARDED_HTTPS_PORT, FORWARDED_PATH);

    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER = "allowedContextPaths";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String CLIENT_ID = "client-id";

    private static final String REGISTRATION_ID = OidcRegistrationProperty.REGISTRATION_ID.getProperty();

    private static final int STANDARD_SERVER_PORT = 8443;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    @Mock
    ClientRegistrationRepository clientRegistrationRepository;

    StandardOAuth2AuthorizationRequestResolver resolver;

    @BeforeEach
    void setResolver() {
        resolver = new StandardOAuth2AuthorizationRequestResolver(clientRegistrationRepository);
        httpServletRequest = new MockHttpServletRequest();
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testResolveNotFound() {
        final OAuth2AuthorizationRequest authorizationRequest = resolver.resolve(httpServletRequest);

        assertNull(authorizationRequest);
    }

    @Test
    void testResolveClientRegistrationIdNotFound() {
        final OAuth2AuthorizationRequest authorizationRequest = resolver.resolve(httpServletRequest, null);

        assertNull(authorizationRequest);
    }

    @Test
    void testResolveFound() {
        final URI redirectUri = URI.create(REDIRECT_URI);
        httpServletRequest.setScheme(redirectUri.getScheme());
        httpServletRequest.setServerPort(redirectUri.getPort());

        final ClientRegistration clientRegistration = getClientRegistration();
        when(clientRegistrationRepository.findByRegistrationId(eq(REGISTRATION_ID))).thenReturn(clientRegistration);

        final OAuth2AuthorizationRequest authorizationRequest = resolver.resolve(httpServletRequest, REGISTRATION_ID);

        assertNotNull(authorizationRequest);
        assertEquals(REDIRECT_URI, authorizationRequest.getRedirectUri());
        assertPkceParametersFound(authorizationRequest);
    }

    @Test
    void testResolveFoundRedirectUriProxyHeaders() {
        final ClientRegistration clientRegistration = getClientRegistration();
        when(clientRegistrationRepository.findByRegistrationId(eq(REGISTRATION_ID))).thenReturn(clientRegistration);

        final ServletContext servletContext = httpServletRequest.getServletContext();
        servletContext.setInitParameter(ALLOWED_CONTEXT_PATHS_PARAMETER, FORWARDED_PATH);

        final URI forwardedRedirectUri = URI.create(FORWARDED_REDIRECT_URI);
        httpServletRequest.setServerPort(STANDARD_SERVER_PORT);
        httpServletRequest.addHeader(ProxyHeader.PROXY_SCHEME.getHeader(), forwardedRedirectUri.getScheme());
        httpServletRequest.addHeader(ProxyHeader.PROXY_HOST.getHeader(), forwardedRedirectUri.getHost());
        httpServletRequest.addHeader(ProxyHeader.PROXY_PORT.getHeader(), FORWARDED_HTTPS_PORT);
        httpServletRequest.addHeader(ProxyHeader.PROXY_CONTEXT_PATH.getHeader(), FORWARDED_PATH);

        final OAuth2AuthorizationRequest authorizationRequest = resolver.resolve(httpServletRequest, REGISTRATION_ID);

        assertNotNull(authorizationRequest);
        assertEquals(FORWARDED_REDIRECT_URI, authorizationRequest.getRedirectUri());
        assertPkceParametersFound(authorizationRequest);
    }

    private void assertPkceParametersFound(final OAuth2AuthorizationRequest authorizationRequest) {
        assertNotNull(authorizationRequest.getAttribute(PkceParameterNames.CODE_VERIFIER));

        final Map<String, Object> additionalParameters = authorizationRequest.getAdditionalParameters();
        assertTrue(additionalParameters.containsKey(PkceParameterNames.CODE_CHALLENGE));
        assertTrue(additionalParameters.containsKey(PkceParameterNames.CODE_CHALLENGE_METHOD));
    }

    ClientRegistration getClientRegistration() {
        return ClientRegistration.withRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty())
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .clientId(CLIENT_ID)
                .redirectUri(REDIRECT_URI)
                .authorizationUri(AUTHORIZATION_URI)
                .tokenUri(TOKEN_URI)
                .build();
    }
}
