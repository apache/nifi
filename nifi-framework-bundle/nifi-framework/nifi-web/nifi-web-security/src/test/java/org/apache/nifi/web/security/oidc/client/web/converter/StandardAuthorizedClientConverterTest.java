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
package org.apache.nifi.web.security.oidc.client.web.converter;

import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.oidc.client.web.OidcAuthorizedClient;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardAuthorizedClientConverterTest {
    private static final String CLIENT_ID = "client-id";

    private static final String REDIRECT_URI = "http://localhost:8080";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String USER_IDENTITY = LogoutRequest.class.getSimpleName();

    private static final String ACCESS_TOKEN = "access";

    private static final String REFRESH_TOKEN = "refresh";

    private static final String ID_TOKEN = "id";

    @Mock
    ClientRegistrationRepository clientRegistrationRepository;

    StandardAuthorizedClientConverter converter;

    @BeforeEach
    void setConverter() {
        converter = new StandardAuthorizedClientConverter(new StringPropertyEncryptor(), clientRegistrationRepository);
    }

    @Test
    void testGetEncoded() {
        final OidcAuthorizedClient oidcAuthorizedClient = getOidcAuthorizedClient(new OAuth2RefreshToken(REFRESH_TOKEN, Instant.now()));
        final String encoded = converter.getEncoded(oidcAuthorizedClient);

        assertNotNull(encoded);
    }

    @Test
    void testGetDecodedInvalid() {
        final OidcAuthorizedClient oidcAuthorizedClient = converter.getDecoded(String.class.getName());

        assertNull(oidcAuthorizedClient);
    }

    @Test
    void testGetEncodedDecoded() {
        final OidcAuthorizedClient oidcAuthorizedClient = getOidcAuthorizedClient(new OAuth2RefreshToken(REFRESH_TOKEN, Instant.now()));
        final String encoded = converter.getEncoded(oidcAuthorizedClient);

        assertNotNull(encoded);

        final ClientRegistration clientRegistration = getClientRegistration();
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);
        final OidcAuthorizedClient decoded = converter.getDecoded(encoded);

        assertEquals(decoded.getClientRegistration().getRedirectUri(), clientRegistration.getRedirectUri());
        assertAuthorizedClientEquals(oidcAuthorizedClient, decoded);
    }

    @Test
    void testGetEncodedDecodedNullRefreshToken() {
        final OidcAuthorizedClient oidcAuthorizedClient = getOidcAuthorizedClient(null);
        final String encoded = converter.getEncoded(oidcAuthorizedClient);

        assertNotNull(encoded);

        final ClientRegistration clientRegistration = getClientRegistration();
        when(clientRegistrationRepository.findByRegistrationId(eq(OidcRegistrationProperty.REGISTRATION_ID.getProperty()))).thenReturn(clientRegistration);
        final OidcAuthorizedClient decoded = converter.getDecoded(encoded);

        assertEquals(decoded.getClientRegistration().getRedirectUri(), clientRegistration.getRedirectUri());
        assertAuthorizedClientEquals(oidcAuthorizedClient, decoded);
    }

    void assertAuthorizedClientEquals(final OidcAuthorizedClient expected, final OidcAuthorizedClient actual) {
        assertNotNull(actual);
        assertEquals(expected.getPrincipalName(), actual.getPrincipalName());

        assertEquals(expected.getAccessToken().getTokenValue(), actual.getAccessToken().getTokenValue());
        assertEquals(expected.getAccessToken().getExpiresAt(), actual.getAccessToken().getExpiresAt());

        final OidcIdToken idToken = actual.getIdToken();
        assertEquals(expected.getIdToken().getTokenValue(), idToken.getTokenValue());
        assertEquals(expected.getIdToken().getExpiresAt(), idToken.getExpiresAt());
        assertEquals(USER_IDENTITY, idToken.getSubject());

        final OAuth2RefreshToken expectedRefreshToken = expected.getRefreshToken();
        if (expectedRefreshToken == null) {
            assertNull(actual.getRefreshToken());
        } else {
            final OAuth2RefreshToken actualRefreshToken = actual.getRefreshToken();
            assertNotNull(actualRefreshToken);
            assertEquals(expectedRefreshToken.getTokenValue(), actualRefreshToken.getTokenValue());
            assertEquals(expectedRefreshToken.getExpiresAt(), actualRefreshToken.getExpiresAt());
        }
    }

    OidcAuthorizedClient getOidcAuthorizedClient(final OAuth2RefreshToken refreshToken) {
        final Instant issuedAt = Instant.now();
        final Instant expiresAt = Instant.MAX;

        final ClientRegistration clientRegistration = getClientRegistration();
        final OAuth2AccessToken accessToken = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, ACCESS_TOKEN, issuedAt, expiresAt);
        final Map<String, Object> claims = new LinkedHashMap<>();
        claims.put(SupportedClaim.ISSUED_AT.getClaim(), issuedAt);
        claims.put(SupportedClaim.EXPIRATION.getClaim(), expiresAt);
        final OidcIdToken idToken = new OidcIdToken(ID_TOKEN, issuedAt, expiresAt, claims);
        return new OidcAuthorizedClient(
                clientRegistration,
                USER_IDENTITY,
                accessToken,
                refreshToken,
                idToken
        );
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

    private static class StringPropertyEncryptor implements PropertyEncryptor {

        @Override
        public String encrypt(String property) {
            return property;
        }

        @Override
        public String decrypt(String encryptedProperty) {
            return encryptedProperty;
        }
    }
}
