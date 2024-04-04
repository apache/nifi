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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.web.security.oidc.client.web.converter.AuthorizedClientConverter;
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
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardOidcAuthorizedClientRepositoryTest {
    private static final String REGISTRATION_ID = OidcRegistrationProperty.REGISTRATION_ID.getProperty();

    private static final String IDENTITY = "user-identity";

    private static final String ENCODED_CLIENT = "encoded-client";

    private static final String CLIENT_ID = "client-id";

    private static final String REDIRECT_URI = "http://localhost:8080";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String TOKEN = "token";

    private static final int EXPIRES_OFFSET = 60;

    private static final Scope SCOPE = Scope.LOCAL;

    @Mock
    StateManager stateManager;

    @Mock
    StateMap stateMap;

    @Mock
    AuthorizedClientConverter authorizedClientConverter;

    @Mock
    OidcAuthorizedClient authorizedClient;

    @Captor
    ArgumentCaptor<Map<String, String>> stateMapCaptor;

    MockHttpServletRequest request;

    MockHttpServletResponse response;

    StandardOidcAuthorizedClientRepository repository;

    @BeforeEach
    void setRepository() {
        repository = new StandardOidcAuthorizedClientRepository(stateManager, authorizedClientConverter);
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
    }

    @Test
    void testLoadAuthorizedClientNotFound() throws IOException {
        final Authentication principal = mock(Authentication.class);
        when(principal.getName()).thenReturn(IDENTITY);

        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);

        final OidcAuthorizedClient authorizedClient = repository.loadAuthorizedClient(REGISTRATION_ID, principal, request);

        assertNull(authorizedClient);
    }

    @Test
    void testLoadAuthorizedClientFound() throws IOException {
        final Authentication principal = mock(Authentication.class);
        when(principal.getName()).thenReturn(IDENTITY);

        when(stateMap.get(eq(IDENTITY))).thenReturn(ENCODED_CLIENT);
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(authorizedClientConverter.getDecoded(eq(ENCODED_CLIENT))).thenReturn(authorizedClient);

        final OidcAuthorizedClient authorizedClientFound = repository.loadAuthorizedClient(REGISTRATION_ID, principal, request);

        assertEquals(authorizedClient, authorizedClientFound);
    }

    @Test
    void testSaveAuthorizedClient() throws IOException {
        final OAuth2AuthenticationToken principal = mock(OAuth2AuthenticationToken.class);
        final OidcUser oidcUser = mock(OidcUser.class);
        final OidcIdToken idToken = mock(OidcIdToken.class);
        final OAuth2AccessToken accessToken = mock(OAuth2AccessToken.class);

        when(principal.getName()).thenReturn(IDENTITY);
        when(principal.getPrincipal()).thenReturn(oidcUser);
        when(oidcUser.getIdToken()).thenReturn(idToken);
        when(authorizedClient.getClientRegistration()).thenReturn(getClientRegistration());
        when(authorizedClient.getPrincipalName()).thenReturn(IDENTITY);
        when(authorizedClient.getAccessToken()).thenReturn(accessToken);
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(authorizedClientConverter.getEncoded(isA(OidcAuthorizedClient.class))).thenReturn(ENCODED_CLIENT);

        repository.saveAuthorizedClient(authorizedClient, principal, request, response);

        verify(authorizedClientConverter).getEncoded(isA(OidcAuthorizedClient.class));
        verify(stateManager).replace(eq(stateMap), stateMapCaptor.capture(), eq(SCOPE));

        final Map<String, String> updatedStateMap = stateMapCaptor.getValue();
        final String encodedClient = updatedStateMap.get(IDENTITY);
        assertEquals(ENCODED_CLIENT, encodedClient);
    }

    @Test
    void testRemoveAuthorizedClient() throws IOException {
        final Authentication principal = mock(Authentication.class);
        when(principal.getName()).thenReturn(IDENTITY);

        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);

        repository.removeAuthorizedClient(REGISTRATION_ID, principal, request, response);

        verify(stateManager).replace(eq(stateMap), stateMapCaptor.capture(), eq(SCOPE));
        final Map<String, String> updatedStateMap = stateMapCaptor.getValue();
        assertTrue(updatedStateMap.isEmpty());
    }

    @Test
    void testRemoveAuthorizedClientStateManagerException() throws IOException {
        final Authentication principal = mock(Authentication.class);
        when(principal.getName()).thenReturn(IDENTITY);

        when(stateManager.getState(eq(SCOPE))).thenThrow(new IOException());

        assertDoesNotThrow(() -> repository.removeAuthorizedClient(REGISTRATION_ID, principal, request, response));
    }

    @Test
    void testDeleteExpiredEmpty() throws IOException {
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);

        final List<OidcAuthorizedClient> deletedAuthorizedClients = repository.deleteExpired();

        assertTrue(deletedAuthorizedClients.isEmpty());
    }

    @Test
    void testDeleteExpired() throws IOException {
        final Map<String, String> currentStateMap = new LinkedHashMap<>();
        currentStateMap.put(IDENTITY, ENCODED_CLIENT);

        when(stateMap.toMap()).thenReturn(currentStateMap);
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);

        final Instant issuedAt = Instant.MIN;
        final Instant expiresAt = Instant.now().minusSeconds(EXPIRES_OFFSET);
        final OAuth2AccessToken accessToken = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, TOKEN, issuedAt, expiresAt);
        when(authorizedClient.getAccessToken()).thenReturn(accessToken);
        when(authorizedClientConverter.getDecoded(eq(ENCODED_CLIENT))).thenReturn(authorizedClient);

        final List<OidcAuthorizedClient> deletedAuthorizedClients = repository.deleteExpired();

        assertFalse(deletedAuthorizedClients.isEmpty());
        final OidcAuthorizedClient deletedAuthorizedClient = deletedAuthorizedClients.iterator().next();
        assertEquals(authorizedClient, deletedAuthorizedClient);
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
