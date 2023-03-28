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
package org.apache.nifi.web.security.oidc.revocation;

import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.web.client.RestOperations;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardTokenRevocationResponseClientTest {
    private static final String CLIENT_ID = "client-id";

    private static final String CLIENT_SECRET = "client-secret";

    private static final String REDIRECT_URI = "http://localhost:8080";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String REVOCATION_ENDPOINT_URI = "http://localhost/revoke";

    private static final String TOKEN = "token";

    private static final String TOKEN_TYPE_HINT = "refresh_token";

    @Mock
    RestOperations restOperations;

    @Mock
    ClientRegistrationRepository clientRegistrationRepository;

    StandardTokenRevocationResponseClient client;

    @BeforeEach
    void setClient() {
        client = new StandardTokenRevocationResponseClient(restOperations, clientRegistrationRepository);
    }

    @Test
    void testGetRevocationResponseEndpointNotFound() {
        final TokenRevocationRequest revocationRequest = new TokenRevocationRequest(TOKEN, TOKEN_TYPE_HINT);

        final ClientRegistration clientRegistration = getClientRegistration(null);
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);

        final TokenRevocationResponse revocationResponse = client.getRevocationResponse(revocationRequest);

        assertNotNull(revocationResponse);
        assertTrue(revocationResponse.isSuccess());
    }

    @Test
    void testGetRevocationResponseException() {
        final TokenRevocationRequest revocationRequest = new TokenRevocationRequest(TOKEN, TOKEN_TYPE_HINT);

        final ClientRegistration clientRegistration = getClientRegistration(REVOCATION_ENDPOINT_URI);
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);

        when(restOperations.exchange(any(), eq(String.class))).thenThrow(new RuntimeException());

        final TokenRevocationResponse revocationResponse = client.getRevocationResponse(revocationRequest);

        assertNotNull(revocationResponse);
        assertFalse(revocationResponse.isSuccess());
    }

    @Test
    void testGetRevocationResponse() {
        final TokenRevocationRequest revocationRequest = new TokenRevocationRequest(TOKEN, TOKEN_TYPE_HINT);

        final ClientRegistration clientRegistration = getClientRegistration(REVOCATION_ENDPOINT_URI);
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);

        final ResponseEntity<String> responseEntity = ResponseEntity.ok().build();
        when(restOperations.exchange(any(), eq(String.class))).thenReturn(responseEntity);

        final TokenRevocationResponse revocationResponse = client.getRevocationResponse(revocationRequest);

        assertNotNull(revocationResponse);
        assertTrue(revocationResponse.isSuccess());
    }

    ClientRegistration getClientRegistration(final String revocationEndpoint) {
        final Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put(StandardTokenRevocationResponseClient.REVOCATION_ENDPOINT, revocationEndpoint);
        return ClientRegistration.withRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty())
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .clientId(CLIENT_ID)
                .clientSecret(CLIENT_SECRET)
                .redirectUri(REDIRECT_URI)
                .authorizationUri(AUTHORIZATION_URI)
                .tokenUri(TOKEN_URI)
                .providerConfigurationMetadata(metadata)
                .build();
    }
}
