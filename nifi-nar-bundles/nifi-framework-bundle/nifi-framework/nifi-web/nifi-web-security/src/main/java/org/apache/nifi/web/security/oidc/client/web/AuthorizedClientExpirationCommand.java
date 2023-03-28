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

import org.apache.nifi.web.security.oidc.revocation.TokenRevocationRequest;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationResponse;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationResponseClient;
import org.apache.nifi.web.security.oidc.revocation.TokenTypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Runnable command to delete expired OpenID Authorized Clients
 */
public class AuthorizedClientExpirationCommand implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AuthorizedClientExpirationCommand.class);

    private final TrackedAuthorizedClientRepository trackedAuthorizedClientRepository;

    private final TokenRevocationResponseClient tokenRevocationResponseClient;

    public AuthorizedClientExpirationCommand(
            final TrackedAuthorizedClientRepository trackedAuthorizedClientRepository,
            final TokenRevocationResponseClient tokenRevocationResponseClient
    ) {
        this.trackedAuthorizedClientRepository = Objects.requireNonNull(trackedAuthorizedClientRepository, "Repository required");
        this.tokenRevocationResponseClient = Objects.requireNonNull(tokenRevocationResponseClient, "Response Client required");
    }

    /**
     * Run expiration command and send Token Revocation Requests for Refresh Tokens of expired Authorized Clients
     */
    public void run() {
        logger.debug("Delete Expired Authorized Clients started");
        final List<OidcAuthorizedClient> deletedAuthorizedClients = deleteExpired();

        for (final OidcAuthorizedClient authorizedClient : deletedAuthorizedClients) {
            final String identity = authorizedClient.getPrincipalName();
            final OAuth2RefreshToken refreshToken = authorizedClient.getRefreshToken();
            if (refreshToken == null) {
                logger.debug("Identity [{}] OIDC Refresh Token not found", identity);
            } else {
                final TokenRevocationRequest revocationRequest = new TokenRevocationRequest(refreshToken.getTokenValue(), TokenTypeHint.REFRESH_TOKEN.getHint());
                final TokenRevocationResponse revocationResponse = tokenRevocationResponseClient.getRevocationResponse(revocationRequest);
                logger.debug("Identity [{}] OIDC Refresh Token revocation response status [{}]", identity, revocationResponse.getStatusCode());
            }
        }

        logger.debug("Delete Expired Authorized Clients completed");
    }

    private List<OidcAuthorizedClient> deleteExpired() {
        try {
            return trackedAuthorizedClientRepository.deleteExpired();
        } catch (final Exception e) {
            logger.warn("Delete Expired Authorized Clients failed", e);
            return Collections.emptyList();
        }
    }
}
