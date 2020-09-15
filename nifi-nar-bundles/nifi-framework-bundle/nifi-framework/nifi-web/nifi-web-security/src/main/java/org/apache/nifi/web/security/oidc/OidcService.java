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
package org.apache.nifi.web.security.oidc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.id.State;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.util.CacheKey;
import org.apache.nifi.web.security.util.IdentityProviderUtils;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.web.security.oidc.StandardOidcIdentityProvider.OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED;

/**
 * OidcService is a service for managing the OpenId Connect Authorization flow.
 */
public class OidcService {

    private OidcIdentityProvider identityProvider;
    private Cache<CacheKey, State> stateLookupForPendingRequests; // identifier from cookie -> state value
    private Cache<CacheKey, String> jwtLookupForCompletedRequests; // identifier from cookie -> jwt or identity (and generate jwt on retrieval)

    /**
     * Creates a new OtpService with an expiration of 1 minute.
     *
     * @param identityProvider          The identity provider
     */
    public OidcService(final OidcIdentityProvider identityProvider) {
        this(identityProvider, 60, TimeUnit.SECONDS);
    }

    /**
     * Creates a new OtpService.
     *
     * @param identityProvider          The identity provider
     * @param duration                  The expiration duration
     * @param units                     The expiration units
     * @throws NullPointerException     If units is null
     * @throws IllegalArgumentException If duration is negative
     */
    public OidcService(final OidcIdentityProvider identityProvider, final int duration, final TimeUnit units) {
        if (identityProvider == null) {
            throw new RuntimeException("The OidcIdentityProvider must be specified.");
        }

        identityProvider.initializeProvider();
        this.identityProvider = identityProvider;
        this.stateLookupForPendingRequests = CacheBuilder.newBuilder().expireAfterWrite(duration, units).build();
        this.jwtLookupForCompletedRequests = CacheBuilder.newBuilder().expireAfterWrite(duration, units).build();
    }

    /**
     * Returns whether OpenId Connect is enabled.
     *
     * @return whether OpenId Connect is enabled
     */
    public boolean isOidcEnabled() {
        return identityProvider.isOidcEnabled();
    }

    /**
     * Returns the OpenId Connect authorization endpoint.
     *
     * @return the authorization endpoint
     */
    public URI getAuthorizationEndpoint() {
        return identityProvider.getAuthorizationEndpoint();
    }

    /**
     * Returns the OpenId Connect end session endpoint.
     *
     * @return the end session endpoint
     */
    public URI getEndSessionEndpoint() {
        return identityProvider.getEndSessionEndpoint();
    }

    /**
     * Returns the OpenId Connect revocation endpoint.
     *
     * @return the revocation endpoint
     */
    public URI getRevocationEndpoint() {
        return identityProvider.getRevocationEndpoint();
    }

    /**
     * Returns the OpenId Connect scope.
     *
     * @return scope
     */
    public Scope getScope() {
        return identityProvider.getScope();
    }

    /**
     * Returns the OpenId Connect client id.
     *
     * @return client id
     */
    public String getClientId() {
        return identityProvider.getClientId().getValue();
    }

    /**
     * Initiates an OpenId Connection authorization code flow using the specified request identifier to maintain state.
     *
     * @param oidcRequestIdentifier request identifier
     * @return state
     */
    public State createState(final String oidcRequestIdentifier) {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        final CacheKey oidcRequestIdentifierKey = new CacheKey(oidcRequestIdentifier);
        final State state = new State(IdentityProviderUtils.generateStateValue());

        try {
            synchronized (stateLookupForPendingRequests) {
                final State cachedState = stateLookupForPendingRequests.get(oidcRequestIdentifierKey, () -> state);
                if (!IdentityProviderUtils.timeConstantEqualityCheck(state.getValue(), cachedState.getValue())) {
                    throw new IllegalStateException("An existing login request is already in progress.");
                }
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unable to store the login request state.");
        }

        return state;
    }

    /**
     * Validates the proposed state with the given request identifier. Will return false if the
     * state does not match or if entry for this request identifier has expired.
     *
     * @param oidcRequestIdentifier request identifier
     * @param proposedState proposed state
     * @return whether the state is valid or not
     */
    public boolean isStateValid(final String oidcRequestIdentifier, final State proposedState) {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        if (proposedState == null) {
            throw new IllegalArgumentException("Proposed state must be specified.");
        }

        final CacheKey oidcRequestIdentifierKey = new CacheKey(oidcRequestIdentifier);

        synchronized (stateLookupForPendingRequests) {
            final State state = stateLookupForPendingRequests.getIfPresent(oidcRequestIdentifierKey);
            if (state != null) {
                stateLookupForPendingRequests.invalidate(oidcRequestIdentifierKey);
            }

            return state != null && IdentityProviderUtils.timeConstantEqualityCheck(state.getValue(), proposedState.getValue());
        }
    }

    /**
     * Exchanges the specified authorization grant for an ID token.
     *
     * @param authorizationGrant authorization grant
     * @return a Login Authentication Token
     * @throws IOException exceptional case for communication error with the OpenId Connect provider
     */
    public LoginAuthenticationToken exchangeAuthorizationCodeForLoginAuthenticationToken(final AuthorizationGrant authorizationGrant) throws IOException {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        // Retrieve Login Authentication Token
        return identityProvider.exchangeAuthorizationCodeforLoginAuthenticationToken(authorizationGrant);
    }

    /**
     * Exchanges the specified authorization grant for an access token.
     *
     * @param authorizationGrant authorization grant
     * @return an Access Token string
     * @throws IOException exceptional case for communication error with the OpenId Connect provider
     */
    public String exchangeAuthorizationCodeForAccessToken(final AuthorizationGrant authorizationGrant) throws Exception {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        // Retrieve access token
        return identityProvider.exchangeAuthorizationCodeForAccessToken(authorizationGrant);
    }

    /**
     * Exchanges the specified authorization grant for an ID Token.
     *
     * @param authorizationGrant authorization grant
     * @return an ID Token string
     * @throws IOException exceptional case for communication error with the OpenId Connect provider
     */
    public String exchangeAuthorizationCodeForIdToken(final AuthorizationGrant authorizationGrant) throws IOException {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        // Retrieve ID token
        return identityProvider.exchangeAuthorizationCodeForIdToken(authorizationGrant);
    }

    /**
     * Stores the NiFi Jwt.
     *
     * @param oidcRequestIdentifier request identifier
     * @param jwt NiFi JWT
     */
    public void storeJwt(final String oidcRequestIdentifier, final String jwt) {
        final CacheKey oidcRequestIdentifierKey = new CacheKey(oidcRequestIdentifier);
        try {
            // Cache the jwt for later retrieval
            synchronized (jwtLookupForCompletedRequests) {
                final String cachedJwt = jwtLookupForCompletedRequests.get(oidcRequestIdentifierKey, () -> jwt);
                if (!IdentityProviderUtils.timeConstantEqualityCheck(jwt, cachedJwt)) {
                    throw new IllegalStateException("An existing login request is already in progress.");
                }
            }
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Unable to store the login authentication token.");
        }
    }

    /**
     * Returns the resulting JWT for the given request identifier. Will return null if the request
     * identifier is not associated with a JWT or if the login sequence was not completed before
     * this request identifier expired.
     *
     * @param oidcRequestIdentifier request identifier
     * @return jwt token
     */
    public String getJwt(final String oidcRequestIdentifier) {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        final CacheKey oidcRequestIdentifierKey = new CacheKey(oidcRequestIdentifier);

        synchronized (jwtLookupForCompletedRequests) {
            final String jwt = jwtLookupForCompletedRequests.getIfPresent(oidcRequestIdentifierKey);
            if (jwt != null) {
                jwtLookupForCompletedRequests.invalidate(oidcRequestIdentifierKey);
            }

            return jwt;
        }
    }

}
