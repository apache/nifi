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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.endpoint.OAuth2AccessTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2RefreshTokenGrantRequest;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.endpoint.OAuth2AccessTokenResponse;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * OpenID Connect Filter for evaluating the application Bearer Token and returning an updated Bearer Token after successful OAuth2 Refresh Token processing
 */
public class OidcBearerTokenRefreshFilter extends OncePerRequestFilter {
    private static final String ROOT_PATH = "/";

    private static final Logger logger = LoggerFactory.getLogger(OidcBearerTokenRefreshFilter.class);

    private final AntPathRequestMatcher currentUserRequestMatcher = new AntPathRequestMatcher("/flow/current-user");

    private final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final Duration refreshWindow;

    private final ConcurrentMap<String, Instant> refreshRequests = new ConcurrentHashMap<>();

    private final BearerTokenProvider bearerTokenProvider;

    private final BearerTokenResolver bearerTokenResolver;

    private final JwtDecoder jwtDecoder;

    private final OAuth2AuthorizedClientRepository authorizedClientRepository;

    private final OAuth2AccessTokenResponseClient<OAuth2RefreshTokenGrantRequest> refreshTokenResponseClient;

    public OidcBearerTokenRefreshFilter(
            final Duration refreshWindow,
            final BearerTokenProvider bearerTokenProvider,
            final BearerTokenResolver bearerTokenResolver,
            final JwtDecoder jwtDecoder,
            final OAuth2AuthorizedClientRepository authorizedClientRepository,
            final OAuth2AccessTokenResponseClient<OAuth2RefreshTokenGrantRequest> refreshTokenResponseClient
    ) {
        this.refreshWindow = Objects.requireNonNull(refreshWindow, "Refresh Window required");
        this.bearerTokenProvider = Objects.requireNonNull(bearerTokenProvider, "Bearer Token Provider required");
        this.bearerTokenResolver = Objects.requireNonNull(bearerTokenResolver, "Bearer Token Resolver required");
        this.jwtDecoder = Objects.requireNonNull(jwtDecoder, "JWT Decoder required");
        this.authorizedClientRepository = Objects.requireNonNull(authorizedClientRepository, "Authorized Client Repository required");
        this.refreshTokenResponseClient = Objects.requireNonNull(refreshTokenResponseClient, "Refresh Token Response Client required");
    }

    /**
     * Run Bearer Token Refresh check for matched HTTP Requests and avoid processing multiple requests for the same user
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param filterChain Filter Chain
     * @throws ServletException Thrown on filter processing failures
     * @throws IOException Thrown on filter processing failures
     */
    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response, final FilterChain filterChain) throws ServletException, IOException {
        if (currentUserRequestMatcher.matches(request)) {
            final String userIdentity = NiFiUserUtils.getNiFiUserIdentity();
            if (refreshRequests.putIfAbsent(userIdentity, Instant.now()) == null) {
                logger.debug("Identity [{}] Bearer Token refresh processing started", userIdentity);
                try {
                    processRequest(userIdentity, request, response);
                } catch (final Exception e) {
                    logger.error("Identity [{}] Bearer Token refresh processing failed", userIdentity, e);
                } finally {
                    refreshRequests.remove(userIdentity);
                    logger.debug("Identity [{}] Bearer Token refresh processing completed", userIdentity);
                }
            }
        }

        filterChain.doFilter(request, response);
    }

    private void processRequest(final String userIdentity, final HttpServletRequest request, final HttpServletResponse response) {
        if (isRefreshRequired(userIdentity, request)) {
            logger.info("Identity [{}] Bearer Token refresh required", userIdentity);

            final OidcAuthorizedClient authorizedClient = loadAuthorizedClient(request);
            if (authorizedClient == null) {
                logger.warn("Identity [{}] OIDC Authorized Client not found", userIdentity);
            } else {
                final OAuth2AccessTokenResponse tokenResponse = getRefreshTokenResponse(authorizedClient, request, response);
                if (tokenResponse == null) {
                    logger.warn("Identity [{}] OpenID Connect Refresh Token not found", userIdentity);
                } else {
                    final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).path(ROOT_PATH).build();
                    final String bearerToken = getBearerToken(userIdentity, tokenResponse);
                    applicationCookieService.addSessionCookie(resourceUri, response, ApplicationCookieName.AUTHORIZATION_BEARER, bearerToken);
                }
            }
        }
    }

    private boolean isRefreshRequired(final String userIdentity, final HttpServletRequest request) {
        final boolean required;

        final String token = bearerTokenResolver.resolve(request);
        if (token == null) {
            logger.debug("Identity [{}] Bearer Token not found", userIdentity);
            required = false;
        } else {
            final Jwt jwt = jwtDecoder.decode(token);
            final Instant expiresAt = Objects.requireNonNull(jwt.getExpiresAt(), "Bearer Token expiration claim not found");
            final Instant refreshRequired = Instant.now().plus(refreshWindow);
            required = refreshRequired.isAfter(expiresAt);
        }

        return required;
    }

    private OidcAuthorizedClient loadAuthorizedClient(final HttpServletRequest request) {
        final SecurityContext context = SecurityContextHolder.getContext();
        final Authentication principal = context.getAuthentication();
        return authorizedClientRepository.loadAuthorizedClient(OidcRegistrationProperty.REGISTRATION_ID.getProperty(), principal, request);
    }

    private String getBearerToken(final String userIdentity, final OAuth2AccessTokenResponse tokenResponse) {
        final OAuth2AccessToken accessToken = tokenResponse.getAccessToken();
        final Instant sessionExpiration = getSessionExpiration(accessToken);
        final Set<? extends GrantedAuthority> providerAuthorities = getProviderAuthorities();
        final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(userIdentity, sessionExpiration, providerAuthorities);
        return bearerTokenProvider.getBearerToken(loginAuthenticationToken);
    }

    private Instant getSessionExpiration(final OAuth2AccessToken accessToken) {
        final Instant tokenExpiration = accessToken.getExpiresAt();
        if (tokenExpiration == null) {
            throw new IllegalArgumentException("OpenID Connect Access Token expiration claim not found");
        }
        return tokenExpiration;
    }

    private OAuth2AccessTokenResponse getRefreshTokenResponse(final OidcAuthorizedClient authorizedClient, final HttpServletRequest request, final HttpServletResponse response) {
        final OAuth2AccessTokenResponse tokenResponse;

        if (authorizedClient.getRefreshToken() == null) {
            tokenResponse = null;
        } else {
            tokenResponse = getRefreshTokenResponse(authorizedClient);

            final OAuth2RefreshToken responseRefreshToken = tokenResponse.getRefreshToken();
            final OAuth2RefreshToken refreshToken = responseRefreshToken == null ? authorizedClient.getRefreshToken() : responseRefreshToken;

            final OidcAuthorizedClient refreshedAuthorizedClient = new OidcAuthorizedClient(
                    authorizedClient.getClientRegistration(),
                    authorizedClient.getPrincipalName(),
                    tokenResponse.getAccessToken(),
                    refreshToken,
                    authorizedClient.getIdToken()
            );
            final OAuth2AuthenticationToken authenticationToken = getAuthenticationToken(authorizedClient);
            authorizedClientRepository.saveAuthorizedClient(refreshedAuthorizedClient, authenticationToken, request, response);
        }

        return tokenResponse;
    }

    private OAuth2AccessTokenResponse getRefreshTokenResponse(final OidcAuthorizedClient authorizedClient) {
        final ClientRegistration clientRegistration = authorizedClient.getClientRegistration();
        final OAuth2AccessToken accessToken = authorizedClient.getAccessToken();
        final OAuth2RefreshToken refreshToken = Objects.requireNonNull(authorizedClient.getRefreshToken(), "Refresh Token required");
        final OAuth2RefreshTokenGrantRequest grantRequest = new OAuth2RefreshTokenGrantRequest(clientRegistration, accessToken, refreshToken);
        return refreshTokenResponseClient.getTokenResponse(grantRequest);
    }

    private OAuth2AuthenticationToken getAuthenticationToken(final OidcAuthorizedClient authorizedClient) {
        final ClientRegistration clientRegistration = authorizedClient.getClientRegistration();
        final OidcIdToken idToken = authorizedClient.getIdToken();
        final OidcUser oidcUser = new DefaultOidcUser(Collections.emptyList(), idToken, SupportedClaim.SUBJECT.getClaim());
        return new OAuth2AuthenticationToken(oidcUser, Collections.emptyList(), clientRegistration.getRegistrationId());
    }

    private Set<? extends GrantedAuthority> getProviderAuthorities() {
        final Set<? extends GrantedAuthority> authorities;

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Set<String> providerGroups = user.getIdentityProviderGroups();
        if (providerGroups == null) {
            authorities = Collections.emptySet();
        } else {
            authorities = providerGroups.stream().map(SimpleGrantedAuthority::new).collect(Collectors.toSet());
        }

        return authorities;
    }
}
