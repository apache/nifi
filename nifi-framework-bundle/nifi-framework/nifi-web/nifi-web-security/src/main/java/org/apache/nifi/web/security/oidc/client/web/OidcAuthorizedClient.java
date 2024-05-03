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

import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;

import java.util.Objects;

/**
 * OpenID Connect Authorized Client with ID Token
 */
public class OidcAuthorizedClient extends OAuth2AuthorizedClient {
    private final OidcIdToken idToken;

    public OidcAuthorizedClient(
            final ClientRegistration clientRegistration,
            final String principalName,
            final OAuth2AccessToken accessToken,
            final OAuth2RefreshToken refreshToken,
            final OidcIdToken idToken
    ) {
        super(clientRegistration, principalName, accessToken, refreshToken);
        this.idToken = Objects.requireNonNull(idToken, "OpenID Connect ID Token required");
    }

    /**
     * Get OpenID Connect ID Token
     *
     * @return OpenID Connect ID Token
     */
    public OidcIdToken getIdToken() {
        return idToken;
    }
}
