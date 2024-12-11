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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.core.user.OAuth2User;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * OpenID Connect implementation of Authorized Client Repository for storing OAuth2 Tokens
 */
public class StandardOidcAuthorizedClientRepository implements OAuth2AuthorizedClientRepository, TrackedAuthorizedClientRepository {
    private static final Logger logger = LoggerFactory.getLogger(StandardOidcAuthorizedClientRepository.class);

    private static final Scope SCOPE = Scope.LOCAL;

    private final StateManager stateManager;

    private final AuthorizedClientConverter authorizedClientConverter;

    /**
     * Standard OpenID Connect Authorized Client Repository Constructor
     *
     * @param stateManager State Manager for storing authorized clients
     * @param authorizedClientConverter Authorized Client Converter
     */
    public StandardOidcAuthorizedClientRepository(
            final StateManager stateManager,
            final AuthorizedClientConverter authorizedClientConverter
    ) {
        this.stateManager = Objects.requireNonNull(stateManager, "State Manager required");
        this.authorizedClientConverter = Objects.requireNonNull(authorizedClientConverter, "Authorized Client Converter required");
    }

    /**
     * Load Authorized Client from State Manager
     *
     * @param clientRegistrationId the identifier for the client's registration
     * @param principal Principal Resource Owner to be loaded
     * @param request HTTP Servlet Request not used
     * @return Authorized Client or null when not found
     * @param <T> Authorized Client Type
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T extends OAuth2AuthorizedClient> T loadAuthorizedClient(
            final String clientRegistrationId,
            final Authentication principal,
            final HttpServletRequest request
    ) {
        final OidcAuthorizedClient authorizedClient;

        final String encoded = findEncoded(principal);
        final String principalId = getPrincipalId(principal);
        if (encoded == null) {
            logger.debug("Identity [{}] OIDC Authorized Client not found", principalId);
            authorizedClient = null;
        } else {
            authorizedClient = authorizedClientConverter.getDecoded(encoded);
            if (authorizedClient == null) {
                logger.warn("Identity [{}] Removing OIDC Authorized Client after decoding failed", principalId);
                removeAuthorizedClient(principal);
            }
        }

        return (T) authorizedClient;
    }

    /**
     * Save Authorized Client in State Manager
     *
     * @param authorizedClient Authorized Client to be saved
     * @param principal Principal Resource Owner to be saved
     * @param request HTTP Servlet Request not used
     * @param response HTTP Servlet Response not used
     */
    @Override
    public void saveAuthorizedClient(
            final OAuth2AuthorizedClient authorizedClient,
            final Authentication principal,
            final HttpServletRequest request,
            final HttpServletResponse response
    ) {
        final OidcAuthorizedClient oidcAuthorizedClient = getOidcAuthorizedClient(authorizedClient, principal);
        final String encoded = authorizedClientConverter.getEncoded(oidcAuthorizedClient);
        final String principalId = getPrincipalId(principal);
        updateState(principalId, stateMap -> stateMap.put(principalId, encoded));
    }

    /**
     * Remove Authorized Client from State Manager
     *
     * @param clientRegistrationId Client Registration Identifier not used
     * @param principal Principal Resource Owner to be removed
     * @param request HTTP Servlet Request not used
     * @param response HTTP Servlet Response not used
     */
    @Override
    public void removeAuthorizedClient(
            final String clientRegistrationId,
            final Authentication principal,
            final HttpServletRequest request,
            final HttpServletResponse response
    ) {
        removeAuthorizedClient(principal);
    }

    /**
     * Delete expired Authorized Clients from the configured State Manager
     *
     * @return Deleted OIDC Authorized Clients
     */
    @Override
    public synchronized List<OidcAuthorizedClient> deleteExpired() {
        final StateMap stateMap = getStateMap();
        final Map<String, String> currentStateMap = stateMap.toMap();
        final Map<String, String> updatedStateMap = new LinkedHashMap<>();

        final List<OidcAuthorizedClient> deletedAuthorizedClients = new ArrayList<>();

        for (final Map.Entry<String, String> encodedEntry : currentStateMap.entrySet()) {
            final String identity = encodedEntry.getKey();
            final String encoded = encodedEntry.getValue();
            try {
                final OidcAuthorizedClient authorizedClient = authorizedClientConverter.getDecoded(encoded);
                if (isExpired(authorizedClient)) {
                    deletedAuthorizedClients.add(authorizedClient);
                } else {
                    updatedStateMap.put(identity, encoded);
                }
            } catch (final Exception e) {
                logger.warn("Decoding OIDC Authorized Client [{}] failed", identity, e);
            }
        }

        setStateMap(updatedStateMap);
        if (deletedAuthorizedClients.isEmpty()) {
            logger.debug("Expired Authorized Clients not found");
        } else {
            logger.debug("Deleted Expired Authorized Clients: State before contained [{}] and after [{}]", currentStateMap.size(), updatedStateMap.size());
        }

        return deletedAuthorizedClients;
    }

    private boolean isExpired(final OidcAuthorizedClient authorizedClient) {
        final OAuth2AccessToken accessToken = authorizedClient.getAccessToken();
        final Instant expiration = accessToken.getExpiresAt();
        return expiration == null || Instant.now().isAfter(expiration);
    }

    private void removeAuthorizedClient(final Authentication principal) {
        final String principalId = getPrincipalId(principal);
        updateState(principalId, stateMap -> stateMap.remove(principalId));
    }

    private OidcAuthorizedClient getOidcAuthorizedClient(final OAuth2AuthorizedClient authorizedClient, final Authentication authentication) {
        final OidcIdToken oidcIdToken = getOidcIdToken(authentication);
        return new OidcAuthorizedClient(
                authorizedClient.getClientRegistration(),
                authorizedClient.getPrincipalName(),
                authorizedClient.getAccessToken(),
                authorizedClient.getRefreshToken(),
                oidcIdToken
        );
    }

    private OidcIdToken getOidcIdToken(final Authentication authentication) {
        final OidcIdToken oidcIdToken;

        if (authentication instanceof OAuth2AuthenticationToken authenticationToken) {
            final OAuth2User oAuth2User = authenticationToken.getPrincipal();
            if (oAuth2User instanceof OidcUser oidcUser) {
                oidcIdToken = oidcUser.getIdToken();
            } else {
                final String message = String.format("OpenID Connect User not found [%s]", oAuth2User.getClass());
                throw new IllegalArgumentException(message);
            }
        } else {
            final String message = String.format("OpenID Connect Authentication Token not found [%s]", authentication.getClass());
            throw new IllegalArgumentException(message);
        }

        return oidcIdToken;
    }

    private String findEncoded(final Authentication authentication) {
        final String principalId = getPrincipalId(authentication);
        final StateMap stateMap = getStateMap();
        return stateMap.get(principalId);
    }

    private String getPrincipalId(final Authentication authentication) {
        return authentication.getName();
    }

    private synchronized void updateState(final String principalId, final Consumer<Map<String, String>> stateConsumer) {
        try {
            final StateMap stateMap = getStateMap();
            final Map<String, String> updated = new LinkedHashMap<>(stateMap.toMap());
            stateConsumer.accept(updated);

            final boolean completed = stateManager.replace(stateMap, updated, SCOPE);

            if (completed) {
                logger.info("Identity [{}] OIDC Authorized Client update completed", principalId);
            } else {
                logger.info("Identity [{}] OIDC Authorized Client update failed", principalId);
            }
        } catch (final Exception e) {
            logger.warn("Identity [{}] OIDC Authorized Client update processing failed", principalId, e);
        }
    }

    private void setStateMap(final Map<String, String> stateMap) {
        try {
            stateManager.setState(stateMap, SCOPE);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private StateMap getStateMap() {
        try {
            return stateManager.getState(SCOPE);
        } catch (final IOException e) {
            throw new UncheckedIOException("Get State for OIDC Authorized Clients failed", e);
        }
    }
}
