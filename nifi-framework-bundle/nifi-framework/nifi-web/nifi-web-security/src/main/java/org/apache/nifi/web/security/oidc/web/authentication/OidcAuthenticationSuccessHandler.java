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
package org.apache.nifi.web.security.oidc.web.authentication;

import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.oidc.OidcConfigurationException;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2Token;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * OpenID Connect Authentication Success Handler redirects to the user interface and sets a Session Cookie with a Bearer Token
 */
public class OidcAuthenticationSuccessHandler extends SimpleUrlAuthenticationSuccessHandler {
    private static final String UI_PATH = "/nifi/";

    private static final String ROOT_PATH = "/";

    private final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final BearerTokenProvider bearerTokenProvider;

    private final List<IdentityMapping> userIdentityMappings;

    private final List<IdentityMapping> groupIdentityMappings;

    private final List<String> userClaimNames;

    private final String groupsClaimName;

    /**
     * OpenID Connect Authentication Success Handler requires Bearer Token Provider and expiration for generated tokens
     *
     * @param bearerTokenProvider Bearer Token Provider
     * @param userIdentityMappings User Identity Mappings
     * @param groupIdentityMappings Group Identity Mappings
     * @param userClaimNames Claim Names for User Identity
     * @param groupsClaimName Claim Name for Groups contained in ID Token or null when not configured
     */
    public OidcAuthenticationSuccessHandler(
            final BearerTokenProvider bearerTokenProvider,
            final List<IdentityMapping> userIdentityMappings,
            final List<IdentityMapping> groupIdentityMappings,
            final List<String> userClaimNames,
            final String groupsClaimName
    ) {
        this.bearerTokenProvider = Objects.requireNonNull(bearerTokenProvider, "Bearer Token Provider required");
        this.userIdentityMappings = Objects.requireNonNull(userIdentityMappings, "User Identity Mappings required");
        this.groupIdentityMappings = Objects.requireNonNull(groupIdentityMappings, "Group Identity Mappings required");
        this.userClaimNames = Objects.requireNonNull(userClaimNames, "User Claim Names required");
        this.groupsClaimName = groupsClaimName;
    }

    /**
     * Determine Redirect Target URL based on Request URL and add Session Cookie containing a Bearer Token
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param authentication OpenID Connect Authentication
     * @return Redirect Target URL
     */
    @Override
    public String determineTargetUrl(final HttpServletRequest request, final HttpServletResponse response, final Authentication authentication) {
        final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).path(ROOT_PATH).build();
        processAuthentication(response, authentication, resourceUri);

        final URI targetUri = RequestUriBuilder.fromHttpServletRequest(request).path(UI_PATH).build();
        return targetUri.toString();
    }

    private void processAuthentication(final HttpServletResponse response, final Authentication authentication, final URI resourceUri) {
        final OAuth2AuthenticationToken authenticationToken = getAuthenticationToken(authentication);
        final OidcUser oidcUser = getOidcUser(authenticationToken);
        final String identity = getIdentity(oidcUser);
        final Set<String> groups = getGroups(oidcUser);

        final OAuth2AccessToken accessToken = getAccessToken(authenticationToken);
        final String bearerToken = getBearerToken(identity, accessToken, groups);
        applicationCookieService.addSessionCookie(resourceUri, response, ApplicationCookieName.AUTHORIZATION_BEARER, bearerToken);
    }

    private String getBearerToken(final String identity, final OAuth2AccessToken accessToken, final Set<String> groups) {
        final Instant sessionExpiration = getSessionExpiration(accessToken);
        final Set<? extends GrantedAuthority> authorities = groups.stream().map(SimpleGrantedAuthority::new).collect(Collectors.toSet());
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(identity, sessionExpiration, authorities);
        return bearerTokenProvider.getBearerToken(loginAuthenticationToken);
    }

    private Instant getSessionExpiration(final OAuth2Token token) {
        final Instant tokenExpiration = token.getExpiresAt();
        if (tokenExpiration == null) {
            throw new IllegalArgumentException("Token expiration claim not found");
        }
        return tokenExpiration;
    }

    private OAuth2AuthenticationToken getAuthenticationToken(final Authentication authentication) {
        if (authentication instanceof OAuth2AuthenticationToken) {
            return (OAuth2AuthenticationToken) authentication;
        } else {
            final String message = String.format("OAuth2AuthenticationToken not found [%s]", authentication.getClass());
            throw new IllegalArgumentException(message);
        }
    }

    private OAuth2AccessToken getAccessToken(final OAuth2AuthenticationToken authenticationToken) {
        final Object credentials = authenticationToken.getCredentials();
        if (credentials instanceof OAuth2AccessToken) {
            return (OAuth2AccessToken) credentials;
        } else {
            final String message = String.format("OAuth2AccessToken not found in credentials [%s]", credentials.getClass());
            throw new IllegalArgumentException(message);
        }
    }

    private OidcUser getOidcUser(final OAuth2AuthenticationToken authenticationToken) {
        final OAuth2User principalUser = authenticationToken.getPrincipal();
        if (principalUser instanceof OidcUser) {
            return (OidcUser) principalUser;
        } else {
            final String message = String.format("OpenID Connect User not found [%s]", principalUser.getClass());
            throw new IllegalArgumentException(message);
        }
    }

    private String getIdentity(final OidcUser oidcUser) {
        final Optional<String> userNameFound = userClaimNames.stream()
                .map(oidcUser::getClaimAsString)
                .filter(Objects::nonNull)
                .findFirst();
        final String identity = userNameFound.orElseThrow(() -> {
            final String message = String.format("User Identity not found in Token Claims %s", userClaimNames);
            return new OidcConfigurationException(message);
        });
        return IdentityMappingUtil.mapIdentity(identity, userIdentityMappings);
    }

    private Set<String> getGroups(final OidcUser oidcUser) {
        final Set<String> groups;
        if (groupsClaimName == null || groupsClaimName.isEmpty()) {
            groups = Collections.emptySet();
        } else {
            final List<String> groupsFound = oidcUser.getClaimAsStringList(groupsClaimName);
            final List<String> claimGroups = groupsFound == null ? Collections.emptyList() : groupsFound;
            groups = claimGroups.stream()
                    .map(group -> IdentityMappingUtil.mapIdentity(group, groupIdentityMappings))
                    .collect(Collectors.toSet());
        }
        return groups;
    }
}
