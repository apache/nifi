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
package org.apache.nifi.web.security.oidc.userinfo;

import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.oidc.OidcConfigurationException;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserRequest;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandardOidcUserServiceTest {
    private static final String REDIRECT_URI = "https://localhost:8443/nifi-api/callback";

    private static final String AUTHORIZATION_URI = "http://localhost/authorize";

    private static final String TOKEN_URI = "http://localhost/token";

    private static final String CLIENT_ID = "client-id";

    private static final String ACCESS_TOKEN = "access";

    private static final String ID_TOKEN = "id";

    private static final String USER_NAME_CLAIM = "email";

    private static final String FALLBACK_CLAIM = "preferred_username";

    private static final String MISSING_CLAIM = "missing";

    private static final String SUBJECT = String.class.getSimpleName();

    private static final String IDENTITY = Authentication.class.getSimpleName();

    private static final String FIRST_GROUP = "$1";

    private static final Pattern MATCH_PATTERN = Pattern.compile("(.*)");

    private static final IdentityMapping UPPER_IDENTITY_MAPPING = new IdentityMapping(
            IdentityMapping.Transform.UPPER.toString(),
            MATCH_PATTERN,
            FIRST_GROUP,
            IdentityMapping.Transform.UPPER
    );

    private StandardOidcUserService service;

    @BeforeEach
    void setService() {
        service = new StandardOidcUserService(
                Arrays.asList(USER_NAME_CLAIM, FALLBACK_CLAIM),
                Collections.singletonList(UPPER_IDENTITY_MAPPING)
        );
    }

    @Test
    void testLoadUser() {
        final OidcUserRequest userRequest = getUserRequest(USER_NAME_CLAIM);
        final OidcUser oidcUser = service.loadUser(userRequest);

        assertNotNull(oidcUser);
        assertEquals(IDENTITY.toUpperCase(), oidcUser.getName());
    }

    @Test
    void testLoadUserFallbackClaim() {
        final OidcUserRequest userRequest = getUserRequest(FALLBACK_CLAIM);
        final OidcUser oidcUser = service.loadUser(userRequest);

        assertNotNull(oidcUser);
        assertEquals(IDENTITY.toUpperCase(), oidcUser.getName());
    }

    @Test
    void testLoadUserClaimNotFound() {
        final OidcUserRequest userRequest = getUserRequest(MISSING_CLAIM);

        assertThrows(OidcConfigurationException.class, () -> service.loadUser(userRequest));
    }

    OidcUserRequest getUserRequest(final String userNameClaim) {
        final ClientRegistration clientRegistration = getClientRegistrationBuilder().build();

        final Instant issuedAt = Instant.now();
        final Instant expiresAt = Instant.MAX;
        final OAuth2AccessToken accessToken = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, ACCESS_TOKEN, issuedAt, expiresAt);

        final Map<String, Object> claims = getClaims(userNameClaim);

        final OidcIdToken idToken = new OidcIdToken(ID_TOKEN, issuedAt, expiresAt, claims);
        return new OidcUserRequest(clientRegistration, accessToken, idToken);
    }

    Map<String, Object> getClaims(final String userNameClaim) {
        final Map<String, Object> claims = new LinkedHashMap<>();
        claims.put(SupportedClaim.SUBJECT.getClaim(), SUBJECT);
        claims.put(SupportedClaim.ISSUED_AT.getClaim(), Instant.now());
        claims.put(SupportedClaim.EXPIRATION.getClaim(), Instant.MAX);
        claims.put(userNameClaim, IDENTITY);
        return claims;
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
