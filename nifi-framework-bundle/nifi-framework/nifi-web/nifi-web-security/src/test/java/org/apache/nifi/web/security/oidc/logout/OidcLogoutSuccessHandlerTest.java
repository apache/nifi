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
package org.apache.nifi.web.security.oidc.logout;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.oidc.client.web.OidcAuthorizedClient;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationRequest;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationResponse;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationResponseClient;
import org.apache.nifi.web.security.oidc.revocation.TokenTypeHint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;

import jakarta.servlet.http.Cookie;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OidcLogoutSuccessHandlerTest {
    private static final String REQUEST_IDENTIFIER = UUID.randomUUID().toString();

    private static final String CLIENT_ID = "client-id";

    private static final String REDIRECT_URI = "http://localhost:8080";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String END_SESSION_URI = "http://localhost/end_session";

    private static final String USER_IDENTITY = LogoutRequest.class.getSimpleName();

    private static final String REQUEST_URI = "/nifi-api";

    private static final int SERVER_PORT = 8080;

    private static final String REDIRECTED_URL = "http://localhost:8080/nifi/logout-complete";

    private static final String ACCESS_TOKEN = "access-token";

    private static final String REFRESH_TOKEN = "refresh-token";

    private static final String ID_TOKEN = "oidc-id-token";

    private static final String END_SESSION_REDIRECT_URL = String.format("%s?id_token_hint=%s&post_logout_redirect_uri=%s", END_SESSION_URI, ID_TOKEN, REDIRECTED_URL);

    @Mock
    ClientRegistrationRepository clientRegistrationRepository;

    @Mock
    OAuth2AuthorizedClientRepository authorizedClientRepository;

    @Mock
    Authentication authentication;

    @Mock
    OAuth2AccessToken accessToken;

    @Mock
    OAuth2RefreshToken refreshToken;

    @Mock
    OidcIdToken idToken;

    @Mock
    TokenRevocationResponseClient tokenRevocationResponseClient;

    @Captor
    ArgumentCaptor<TokenRevocationRequest> revocationRequestCaptor;

    MockHttpServletRequest httpServletRequest;

    MockHttpServletResponse httpServletResponse;

    LogoutRequestManager logoutRequestManager;

    OidcLogoutSuccessHandler handler;

    @BeforeEach
    void setHandler() {
        logoutRequestManager = new LogoutRequestManager();
        handler = new OidcLogoutSuccessHandler(
                logoutRequestManager,
                clientRegistrationRepository,
                authorizedClientRepository,
                tokenRevocationResponseClient
        );
        httpServletRequest = new MockHttpServletRequest();
        httpServletRequest.setServerPort(SERVER_PORT);
        httpServletResponse = new MockHttpServletResponse();
    }

    @Test
    void testOnLogoutSuccessRequestNotFound() throws IOException {
        setRequestCookie();

        handler.onLogoutSuccess(httpServletRequest, httpServletResponse, authentication);

        final String redirectedUrl = httpServletResponse.getRedirectedUrl();

        assertEquals(REDIRECTED_URL, redirectedUrl);
    }

    @Test
    void testOnLogoutSuccessRequestFoundEndSessionNotSupported() throws IOException {
        setRequestCookie();
        startLogoutRequest();

        final ClientRegistration clientRegistration = getClientRegistrationBuilder().build();
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);

        handler.onLogoutSuccess(httpServletRequest, httpServletResponse, authentication);

        final String redirectedUrl = httpServletResponse.getRedirectedUrl();

        assertEquals(REDIRECTED_URL, redirectedUrl);
    }

    @Test
    void testOnLogoutSuccessRequestFoundEndSessionSupportedTokenNotFound() throws IOException {
        setRequestCookie();
        startLogoutRequest();

        final Map<String, Object> configurationMetadata = Collections.singletonMap(OidcLogoutSuccessHandler.END_SESSION_ENDPOINT, END_SESSION_URI);
        final ClientRegistration clientRegistration = getClientRegistrationBuilder().providerConfigurationMetadata(configurationMetadata).build();
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);

        handler.onLogoutSuccess(httpServletRequest, httpServletResponse, authentication);

        final String redirectedUrl = httpServletResponse.getRedirectedUrl();

        assertEquals(REDIRECTED_URL, redirectedUrl);
    }

    @Test
    void testOnLogoutSuccessRequestFoundEndSessionSupportedTokenFound() throws IOException {
        setRequestCookie();
        startLogoutRequest();

        final Map<String, Object> configurationMetadata = Collections.singletonMap(OidcLogoutSuccessHandler.END_SESSION_ENDPOINT, END_SESSION_URI);
        final ClientRegistration clientRegistration = getClientRegistrationBuilder().providerConfigurationMetadata(configurationMetadata).build();
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);

        when(idToken.getTokenValue()).thenReturn(ID_TOKEN);
        final OidcAuthorizedClient oidcAuthorizedClient = new OidcAuthorizedClient(
                clientRegistration,
                USER_IDENTITY,
                accessToken,
                refreshToken,
                idToken
        );
        when(authorizedClientRepository.loadAuthorizedClient(
                eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()),
                isA(Authentication.class),
                eq(httpServletRequest))
        ).thenReturn(oidcAuthorizedClient);

        final TokenRevocationResponse revocationResponse = new TokenRevocationResponse(true, HttpStatus.OK.value());
        when(tokenRevocationResponseClient.getRevocationResponse(any())).thenReturn(revocationResponse);
        when(accessToken.getTokenValue()).thenReturn(ACCESS_TOKEN);
        when(refreshToken.getTokenValue()).thenReturn(REFRESH_TOKEN);

        handler.onLogoutSuccess(httpServletRequest, httpServletResponse, authentication);

        final String redirectedUrl = httpServletResponse.getRedirectedUrl();

        assertEquals(END_SESSION_REDIRECT_URL, redirectedUrl);
        verify(authorizedClientRepository).removeAuthorizedClient(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()), any(), eq(httpServletRequest), eq(httpServletResponse));
        verify(tokenRevocationResponseClient, times(2)).getRevocationResponse(revocationRequestCaptor.capture());

        final Iterator<TokenRevocationRequest> revocationRequests = revocationRequestCaptor.getAllValues().iterator();

        final TokenRevocationRequest firstRevocationRequest = revocationRequests.next();
        assertEquals(TokenTypeHint.REFRESH_TOKEN.getHint(), firstRevocationRequest.getTokenTypeHint());
        assertEquals(REFRESH_TOKEN, firstRevocationRequest.getToken());

        final TokenRevocationRequest secondRevocationRequest = revocationRequests.next();
        assertEquals(TokenTypeHint.ACCESS_TOKEN.getHint(), secondRevocationRequest.getTokenTypeHint());
        assertEquals(ACCESS_TOKEN, secondRevocationRequest.getToken());
    }

    void setRequestCookie() {
        final Cookie cookie = new Cookie(ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName(), REQUEST_IDENTIFIER);
        httpServletRequest.setCookies(cookie);
        httpServletRequest.setRequestURI(REQUEST_URI);
    }

    void startLogoutRequest() {
        final LogoutRequest logoutRequest = new LogoutRequest(REQUEST_IDENTIFIER, USER_IDENTITY);
        logoutRequestManager.start(logoutRequest);
    }

    ClientRegistration.Builder getClientRegistrationBuilder() {
        return ClientRegistration.withRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty())
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .clientId(CLIENT_ID)
                .redirectUri(REDIRECT_URI)
                .authorizationUri(AUTHORIZATION_URI)
                .tokenUri(TOKEN_URI);
    }
}
