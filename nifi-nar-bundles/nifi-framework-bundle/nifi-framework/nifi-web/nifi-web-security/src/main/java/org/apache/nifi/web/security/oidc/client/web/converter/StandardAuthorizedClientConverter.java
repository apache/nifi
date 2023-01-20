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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.oidc.OidcConfigurationException;
import org.apache.nifi.web.security.oidc.client.web.OidcAuthorizedClient;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Standard Authorized Client Converter with JSON serialization and encryption
 */
public class StandardAuthorizedClientConverter implements AuthorizedClientConverter {
    private static final Logger logger = LoggerFactory.getLogger(StandardAuthorizedClientConverter.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModules(new JavaTimeModule());

    private final PropertyEncryptor propertyEncryptor;

    private final ClientRegistrationRepository clientRegistrationRepository;

    public StandardAuthorizedClientConverter(
            final PropertyEncryptor propertyEncryptor,
            final ClientRegistrationRepository clientRegistrationRepository
            ) {
        this.propertyEncryptor = Objects.requireNonNull(propertyEncryptor, "Property Encryptor required");
        this.clientRegistrationRepository = Objects.requireNonNull(clientRegistrationRepository, "Client Registry Repository required");
    }

    /**
     * Get encoded representation serializes as JSON and returns an encrypted string
     *
     * @param oidcAuthorizedClient OpenID Connect Authorized Client required
     * @return JSON serialized encrypted string
     */
    @Override
    public String getEncoded(final OidcAuthorizedClient oidcAuthorizedClient) {
        Objects.requireNonNull(oidcAuthorizedClient, "Authorized Client required");

        try {
            final AuthorizedClient authorizedClient = writeAuthorizedClient(oidcAuthorizedClient);
            final String serialized = OBJECT_MAPPER.writeValueAsString(authorizedClient);
            return propertyEncryptor.encrypt(serialized);
        } catch (final Exception e) {
            throw new OidcConfigurationException("OIDC Authorized Client serialization failed", e);
        }
    }

    /**
     * Get decoded Authorized Client from encrypted string containing JSON
     *
     * @param encoded Encoded representation required
     * @return OpenID Connect Authorized Client or null on decoding failures
     */
    @Override
    public OidcAuthorizedClient getDecoded(final String encoded) {
        Objects.requireNonNull(encoded, "Encoded representation required");

        try {
            final String decrypted = propertyEncryptor.decrypt(encoded);
            final AuthorizedClient authorizedClient = OBJECT_MAPPER.readValue(decrypted, AuthorizedClient.class);
            return readAuthorizedClient(authorizedClient);
        } catch (final Exception e) {
            logger.warn("OIDC Authorized Client decoding failed", e);
            return null;
        }
    }

    private AuthorizedClient writeAuthorizedClient(final OidcAuthorizedClient oidcAuthorizedClient) {
        final OAuth2AccessToken oidcAccessToken = oidcAuthorizedClient.getAccessToken();
        final AuthorizedToken accessToken = new AuthorizedToken(
                oidcAccessToken.getTokenValue(),
                oidcAccessToken.getIssuedAt(),
                oidcAccessToken.getExpiresAt()
        );

        final AuthorizedToken refreshToken;
        final OAuth2RefreshToken oidcRefreshToken = oidcAuthorizedClient.getRefreshToken();
        if (oidcRefreshToken == null) {
            refreshToken = null;
        } else {
            refreshToken = new AuthorizedToken(
                    oidcRefreshToken.getTokenValue(),
                    oidcRefreshToken.getIssuedAt(),
                    oidcRefreshToken.getExpiresAt()
            );
        }

        final OidcIdToken oidcIdToken = oidcAuthorizedClient.getIdToken();
        final AuthorizedToken idToken = new AuthorizedToken(
                oidcIdToken.getTokenValue(),
                oidcIdToken.getIssuedAt(),
                oidcIdToken.getExpiresAt()
        );

        final String principalName = oidcAuthorizedClient.getPrincipalName();
        return new AuthorizedClient(principalName, accessToken, refreshToken, idToken);
    }

    private OidcAuthorizedClient readAuthorizedClient(final AuthorizedClient authorizedClient) {
        final ClientRegistration clientRegistration = clientRegistrationRepository.findByRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty());

        final String principalName = authorizedClient.getPrincipalName();
        final OAuth2AccessToken accessToken = getAccessToken(authorizedClient.getAccessToken());
        final OAuth2RefreshToken refreshToken = getRefreshToken(authorizedClient.getRefreshToken());
        final OidcIdToken idToken = getIdToken(authorizedClient);

        return new OidcAuthorizedClient(clientRegistration, principalName, accessToken, refreshToken, idToken);
    }

    private OAuth2AccessToken getAccessToken(final AuthorizedToken authorizedToken) {
        return new OAuth2AccessToken(
                OAuth2AccessToken.TokenType.BEARER,
                authorizedToken.getTokenValue(),
                authorizedToken.getIssuedAt(),
                authorizedToken.getExpiresAt()
        );
    }

    private OAuth2RefreshToken getRefreshToken(final AuthorizedToken authorizedToken) {
        return authorizedToken == null ? null : new OAuth2RefreshToken(
                authorizedToken.getTokenValue(),
                authorizedToken.getIssuedAt(),
                authorizedToken.getExpiresAt()
        );
    }

    private OidcIdToken getIdToken(final AuthorizedClient authorizedClient) {
        final AuthorizedToken authorizedToken = authorizedClient.getIdToken();

        final Map<String, Object> claims = new LinkedHashMap<>();
        claims.put(SupportedClaim.SUBJECT.getClaim(), authorizedClient.getPrincipalName());
        claims.put(SupportedClaim.ISSUED_AT.getClaim(), authorizedToken.getIssuedAt());
        claims.put(SupportedClaim.EXPIRATION.getClaim(), authorizedToken.getExpiresAt());
        return new OidcIdToken(
                authorizedToken.getTokenValue(),
                authorizedToken.getIssuedAt(),
                authorizedToken.getExpiresAt(),
                claims
        );
    }
}
