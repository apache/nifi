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
package org.apache.nifi.web.security.oidc.authentication;

import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.client.RestOperations;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class StandardOidcIdTokenDecoderFactoryTest {
    private static final String REDIRECT_URI = "https://localhost:8443/nifi-api/callback";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String JWK_SET_URI = "http://localhost/token";

    private static final String CLIENT_ID = "client-id";

    private static final String CLIENT_SECRET = "client-secret";

    private static final String NULL_JWS_ALGORITHM = null;

    private static final SignatureAlgorithm DEFAULT_JWS_ALGORITHM = SignatureAlgorithm.RS256;

    @Mock
    RestOperations restOperations;

    StandardOidcIdTokenDecoderFactory factory;

    @Test
    void testNullJwsAlgorithmJwkSetUriRequired() {
        factory = new StandardOidcIdTokenDecoderFactory(NULL_JWS_ALGORITHM, restOperations);

        final ClientRegistration clientRegistration = getClientRegistrationBuilder().jwkSetUri(JWK_SET_URI).build();
        final JwtDecoder decoder = factory.createDecoder(clientRegistration);

        assertNotNull(decoder);
    }

    @Test
    void testNullJwsAlgorithmJwkSetUriNotFound() {
        factory = new StandardOidcIdTokenDecoderFactory(NULL_JWS_ALGORITHM, restOperations);

        final ClientRegistration clientRegistration = getClientRegistrationBuilder().build();
        final OAuth2AuthenticationException exception = assertThrows(OAuth2AuthenticationException.class, () -> factory.createDecoder(clientRegistration));

        final OAuth2Error error = exception.getError();
        final String description = error.getDescription();
        assertTrue(description.contains(DEFAULT_JWS_ALGORITHM.name()));
    }

    @Test
    void testMacJwsAlgorithmClientSecretRequired() {
        factory = new StandardOidcIdTokenDecoderFactory(MacAlgorithm.HS256.getName(), restOperations);

        final ClientRegistration clientRegistration = getClientRegistrationBuilder().clientSecret(CLIENT_SECRET).build();
        final JwtDecoder decoder = factory.createDecoder(clientRegistration);

        assertNotNull(decoder);
    }

    @Test
    void testMacJwsAlgorithmClientSecretNotFound() {
        final MacAlgorithm macAlgorithm = MacAlgorithm.HS256;
        factory = new StandardOidcIdTokenDecoderFactory(macAlgorithm.getName(), restOperations);

        final ClientRegistration clientRegistration = getClientRegistrationBuilder().build();
        final OAuth2AuthenticationException exception = assertThrows(OAuth2AuthenticationException.class, () -> factory.createDecoder(clientRegistration));

        final OAuth2Error error = exception.getError();
        final String description = error.getDescription();
        assertTrue(description.contains(macAlgorithm.name()));
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
