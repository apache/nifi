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
package org.apache.nifi.web.security.saml2.web.authentication;

import org.apache.nifi.authorization.util.IdentityMapping;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.saml2.provider.service.authentication.Saml2AuthenticatedPrincipal;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SAML 2 Authentication Success Handler redirects to the user interface and sets a Session Cookie with a Bearer Token
 */
public class Saml2AuthenticationSuccessHandler extends SimpleUrlAuthenticationSuccessHandler {
    private static final String UI_PATH = "/nifi/";

    private static final String ROOT_PATH = "/";

    private final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final BearerTokenProvider bearerTokenProvider;

    private final List<IdentityMapping> userIdentityMappings;

    private final List<IdentityMapping> groupIdentityMappings;

    private final Duration expiration;

    private Converter<Saml2AuthenticatedPrincipal, String> identityConverter = Saml2AuthenticatedPrincipal::getName;

    /**
     * SAML 2 Authentication Success Handler requires Bearer Token Provider and expiration for generated tokens
     *
     * @param bearerTokenProvider Bearer Token Provider
     * @param userIdentityMappings User Identity Mappings
     * @param groupIdentityMappings Group Identity Mappings
     * @param expiration Expiration for generated tokens
     */
    public Saml2AuthenticationSuccessHandler(
            final BearerTokenProvider bearerTokenProvider,
            final List<IdentityMapping> userIdentityMappings,
            final List<IdentityMapping> groupIdentityMappings,
            final Duration expiration
    ) {
        this.bearerTokenProvider = Objects.requireNonNull(bearerTokenProvider, "Bearer Token Provider required");
        this.userIdentityMappings = Objects.requireNonNull(userIdentityMappings, "User Identity Mappings required");
        this.groupIdentityMappings = Objects.requireNonNull(groupIdentityMappings, "Group Identity Mappings required");
        this.expiration = Objects.requireNonNull(expiration, "Expiration required");
    }

    /**
     * Set Identity Converter for customized mapping of SAML 2 Authenticated Principal to user identity
     *
     * @param identityConverter Identity Converter required
     */
    public void setIdentityConverter(final Converter<Saml2AuthenticatedPrincipal, String> identityConverter) {
        this.identityConverter = Objects.requireNonNull(identityConverter, "Converter required");
    }

    /**
     * Determine Redirect Target URL based on Request URL and add Session Cookie containing a Bearer Token
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param authentication SAML 2 Authentication
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
        final String identity = getIdentity(authentication);
        final Set<String> groups = getGroups(authentication);

        final String bearerToken = getBearerToken(identity, groups);
        applicationCookieService.addSessionCookie(resourceUri, response, ApplicationCookieName.AUTHORIZATION_BEARER, bearerToken);
    }

    private String getBearerToken(final String identity, final Set<String> groups) {
        final Set<? extends GrantedAuthority> authorities = groups.stream().map(SimpleGrantedAuthority::new).collect(Collectors.toSet());
        final Instant sessionExpiration = Instant.now().plus(expiration);
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(identity, sessionExpiration, authorities);
        return bearerTokenProvider.getBearerToken(loginAuthenticationToken);
    }

    private String getIdentity(final Authentication authentication) {
        final Object principal = authentication.getPrincipal();

        final String identity;
        if (principal instanceof Saml2AuthenticatedPrincipal authenticatedPrincipal) {
            identity = identityConverter.convert(authenticatedPrincipal);
        } else {
            identity = authentication.getName();
        }
        return IdentityMappingUtil.mapIdentity(identity, userIdentityMappings);
    }

    private Set<String> getGroups(final Authentication authentication) {
        return authentication.getAuthorities()
                .stream()
                .map(GrantedAuthority::getAuthority)
                .map(group -> IdentityMappingUtil.mapIdentity(group, groupIdentityMappings))
                .collect(Collectors.toSet());
    }
}
