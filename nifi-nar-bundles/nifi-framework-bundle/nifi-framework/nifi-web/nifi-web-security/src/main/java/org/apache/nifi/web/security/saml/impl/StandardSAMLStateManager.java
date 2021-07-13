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
package org.apache.nifi.web.security.saml.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.saml.SAMLStateManager;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.util.CacheKey;
import org.apache.nifi.web.security.util.IdentityProviderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class StandardSAMLStateManager implements SAMLStateManager {

    private static Logger LOGGER = LoggerFactory.getLogger(StandardSAMLStateManager.class);

    private JwtService jwtService;

    // identifier from cookie -> state value
    private final Cache<CacheKey, String> stateLookupForPendingRequests;

    // identifier from cookie -> jwt or identity (and generate jwt on retrieval)
    private final Cache<CacheKey, String> jwtLookupForCompletedRequests;

    public StandardSAMLStateManager(final JwtService jwtService) {
        this(jwtService, 60, TimeUnit.SECONDS);
    }

    public StandardSAMLStateManager(final JwtService jwtService, final int cacheExpiration, final TimeUnit units) {
        this.jwtService = jwtService;
        this.stateLookupForPendingRequests = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration, units).build();
        this.jwtLookupForCompletedRequests = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration, units).build();
    }

    @Override
    public String createState(final String requestIdentifier) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalArgumentException("Request identifier is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);
        final String state = IdentityProviderUtils.generateStateValue();

        try {
            synchronized (stateLookupForPendingRequests) {
                final String cachedState = stateLookupForPendingRequests.get(requestIdentifierKey, () -> state);
                if (!IdentityProviderUtils.timeConstantEqualityCheck(state, cachedState)) {
                    throw new IllegalStateException("An existing login request is already in progress.");
                }
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unable to store the login request state.");
        }

        return state;
    }

    @Override
    public boolean isStateValid(final String requestIdentifier, final String proposedState) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalArgumentException("Request identifier is required");
        }

        if (StringUtils.isBlank(proposedState)) {
            throw new IllegalArgumentException("Proposed state must be specified.");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);

        synchronized (stateLookupForPendingRequests) {
            final String state = stateLookupForPendingRequests.getIfPresent(requestIdentifierKey);
            if (state != null) {
                stateLookupForPendingRequests.invalidate(requestIdentifierKey);
            }

            return state != null && IdentityProviderUtils.timeConstantEqualityCheck(state, proposedState);
        }
    }

    @Override
    public void createJwt(final String requestIdentifier, final LoginAuthenticationToken token) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalStateException("Request identifier is required");
        }

        if (token == null) {
            throw new IllegalArgumentException("Token is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);
        final String nifiJwt = jwtService.generateSignedToken(token);
        try {
            // cache the jwt for later retrieval
            synchronized (jwtLookupForCompletedRequests) {
                final String cachedJwt = jwtLookupForCompletedRequests.get(requestIdentifierKey, () -> nifiJwt);
                if (!IdentityProviderUtils.timeConstantEqualityCheck(nifiJwt, cachedJwt)) {
                    throw new IllegalStateException("An existing login request is already in progress.");
                }
            }
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Unable to store the login authentication token.");
        }
    }

    @Override
    public String getJwt(final String requestIdentifier) {
        if (StringUtils.isBlank(requestIdentifier)) {
            throw new IllegalStateException("Request identifier is required");
        }

        final CacheKey requestIdentifierKey = new CacheKey(requestIdentifier);

        synchronized (jwtLookupForCompletedRequests) {
            final String jwt = jwtLookupForCompletedRequests.getIfPresent(requestIdentifierKey);
            if (jwt != null) {
                jwtLookupForCompletedRequests.invalidate(requestIdentifierKey);
            }

            return jwt;
        }
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }
}
