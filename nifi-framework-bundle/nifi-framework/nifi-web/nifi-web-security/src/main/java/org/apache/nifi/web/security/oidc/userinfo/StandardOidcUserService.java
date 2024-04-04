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
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.web.security.oidc.OidcConfigurationException;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserRequest;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserService;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard extension of Spring Security OIDC User Service supporting customized identity claim mapping
 */
public class StandardOidcUserService extends OidcUserService {
    private final List<String> userClaimNames;

    private final List<IdentityMapping> userIdentityMappings;

    /**
     * Standard OIDC User Service constructor with arguments derived from application properties for mapping usernames
     *
     * @param userClaimNames Ordered list of Token Claim names from which to determine the user identity
     * @param userIdentityMappings List of Identity Mapping rules for optional transformation of a user identity
     */
    public StandardOidcUserService(final List<String> userClaimNames, final List<IdentityMapping> userIdentityMappings) {
        this.userClaimNames = Objects.requireNonNull(userClaimNames, "User Claim Names required");
        this.userIdentityMappings = Objects.requireNonNull(userIdentityMappings, "User Identity Mappings required");
    }

    /**
     * Load User with user identity based on first available Token Claim found
     *
     * @param userRequest OIDC User Request information
     * @return Standard OIDC User
     * @throws OAuth2AuthenticationException Thrown on failures loading user information from Identity Provider
     */
    @Override
    public OidcUser loadUser(final OidcUserRequest userRequest) throws OAuth2AuthenticationException {
        final OidcUser oidcUser = super.loadUser(userRequest);
        final String userClaimName = getUserClaimName(oidcUser);
        final String claim = oidcUser.getClaimAsString(userClaimName);
        final String name = IdentityMappingUtil.mapIdentity(claim, userIdentityMappings);
        return new StandardOidcUser(oidcUser.getAuthorities(), oidcUser.getIdToken(), oidcUser.getUserInfo(), userClaimName, name);
    }

    private String getUserClaimName(final OidcUser oidcUser) {
        final Optional<String> userClaimNameFound = userClaimNames.stream()
                .filter(oidcUser::hasClaim)
                .findFirst();
        return userClaimNameFound.orElseThrow(() -> {
            final String message = String.format("User Claim Name not found in configured Token Claims %s", userClaimNames);
            return new OidcConfigurationException(message);
        });
    }
}
