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

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.user.OAuth2User;

import java.util.Collection;
import java.util.Objects;

/**
 * OAuth2 Authentication Token extended to include the OAuth2 Access Token
 */
public class StandardOAuth2AuthenticationToken extends OAuth2AuthenticationToken {
    private final OAuth2AccessToken accessToken;

    public StandardOAuth2AuthenticationToken(
            final OAuth2User principal,
            final Collection<? extends GrantedAuthority> authorities,
            final String authorizedClientRegistrationId,
            final OAuth2AccessToken accessToken
    ) {
        super(principal, authorities, authorizedClientRegistrationId);
        this.accessToken = Objects.requireNonNull(accessToken, "Access Token required");
    }

    /**
     * Get Credentials returns the OAuth2 Access Token
     *
     * @return OAuth2 Access Token
     */
    @Override
    public Object getCredentials() {
        return accessToken;
    }
}
