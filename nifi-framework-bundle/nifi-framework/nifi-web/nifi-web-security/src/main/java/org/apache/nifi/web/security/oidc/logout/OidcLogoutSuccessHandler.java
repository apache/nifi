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
package org.apache.nifi.web.security.oidc.logout;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.apache.nifi.web.security.oidc.client.web.OidcAuthorizedClient;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationRequest;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationResponse;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationResponseClient;
import org.apache.nifi.web.security.oidc.revocation.TokenTypeHint;
import org.apache.nifi.web.security.token.LogoutAuthenticationToken;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.web.util.UriComponentsBuilder;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * OpenID Connect Logout Success Handler supports RP-Initiated Logout and
 * completes Logout Requests with conditional redirect to Authorization Server
 */
public class OidcLogoutSuccessHandler implements LogoutSuccessHandler {
    static final String END_SESSION_ENDPOINT = "end_session_endpoint";

    private static final String LOGOUT_COMPLETE_PATH = "/nifi/logout-complete";

    private static final String ID_TOKEN_HINT_PARAMETER = "id_token_hint";

    private static final String POST_LOGOUT_REDIRECT_URI_PARAMETER = "post_logout_redirect_uri";

    private static final Logger logger = LoggerFactory.getLogger(OidcLogoutSuccessHandler.class);

    private final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final LogoutRequestManager logoutRequestManager;

    private final ClientRegistrationRepository clientRegistrationRepository;

    private final OAuth2AuthorizedClientRepository authorizedClientRepository;

    private final TokenRevocationResponseClient tokenRevocationResponseClient;

    /**
     * OpenID Connect Logout Success Handler with RP-Initiated Logout 1.0 and RFC 7009 Token Revocation
     *
     * @param logoutRequestManager Application Logout Request Manager
     * @param clientRegistrationRepository OIDC Client Registry Repository for configuration information
     * @param authorizedClientRepository OIDC Authorized Client Repository for cached tokens
     * @param tokenRevocationResponseClient OIDC Revocation Response Client for revoking Refresh Tokens
     */
    public OidcLogoutSuccessHandler(
            final LogoutRequestManager logoutRequestManager,
            final ClientRegistrationRepository clientRegistrationRepository,
            final OAuth2AuthorizedClientRepository authorizedClientRepository,
            final TokenRevocationResponseClient tokenRevocationResponseClient
    ) {
        this.logoutRequestManager = Objects.requireNonNull(logoutRequestManager, "Logout Request Manager required");
        this.clientRegistrationRepository = Objects.requireNonNull(clientRegistrationRepository, "Client Registration Repository required");
        this.authorizedClientRepository = Objects.requireNonNull(authorizedClientRepository, "Authorized Client Repository required");
        this.tokenRevocationResponseClient = Objects.requireNonNull(tokenRevocationResponseClient, "Revocation Response Client required");
    }

    /**
     * On Logout Success complete Logout Request based on Logout Request Identifier found in cookies
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param authentication Authentication is not used
     * @throws IOException Thrown on HttpServletResponse.sendRedirect() failures
     */
    @Override
    public void onLogoutSuccess(final HttpServletRequest request, final HttpServletResponse response, final Authentication authentication) throws IOException {
        final Optional<String> logoutRequestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
        if (logoutRequestIdentifier.isPresent()) {
            final String targetUrl;

            final String requestIdentifier = logoutRequestIdentifier.get();
            final LogoutRequest logoutRequest = logoutRequestManager.get(requestIdentifier);
            if (logoutRequest == null) {
                logger.warn("OIDC Logout Request [{}] not found", requestIdentifier);
                targetUrl = getPostLogoutRedirectUri(request);
            } else {
                final String mappedUserIdentity = logoutRequest.getMappedUserIdentity();
                targetUrl = processLogoutRequest(request, response, requestIdentifier, mappedUserIdentity);
            }

            response.sendRedirect(targetUrl);
        }
    }

    private String processLogoutRequest(final HttpServletRequest request, final HttpServletResponse response, final String requestIdentifier, final String mappedUserIdentity) {
        final String targetUrl;

        final ClientRegistration clientRegistration = clientRegistrationRepository.findByRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty());

        final OidcAuthorizedClient oidcAuthorizedClient = getOidcAuthorizedClient(clientRegistration, mappedUserIdentity, request);
        if (oidcAuthorizedClient == null) {
            targetUrl = getPostLogoutRedirectUri(request);
            logger.warn("OIDC Logout Request [{}] Identity [{}] ID Token not found", requestIdentifier, mappedUserIdentity);
        } else {
            final URI endSessionEndpoint = getEndSessionEndpoint(clientRegistration);
            if (endSessionEndpoint == null) {
                targetUrl = getPostLogoutRedirectUri(request);
                logger.debug("OIDC Logout Request [{}] Identity [{}] end_session_endpoint not provided", requestIdentifier, mappedUserIdentity);
            } else {
                final String postLogoutRedirectUri = getPostLogoutRedirectUri(request);

                final OidcIdToken oidcIdToken = oidcAuthorizedClient.getIdToken();
                final String idToken = oidcIdToken.getTokenValue();
                targetUrl = getEndSessionTargetUrl(endSessionEndpoint, idToken, postLogoutRedirectUri);
                logger.info("OIDC Logout Request [{}] Identity [{}] initiated", requestIdentifier, mappedUserIdentity);
            }

            final LogoutAuthenticationToken principal = new LogoutAuthenticationToken(mappedUserIdentity);
            authorizedClientRepository.removeAuthorizedClient(OidcRegistrationProperty.REGISTRATION_ID.getProperty(), principal, request, response);
            processRefreshTokenRevocation(oidcAuthorizedClient, mappedUserIdentity);
            processAccessTokenRevocation(oidcAuthorizedClient, mappedUserIdentity);
        }

        return targetUrl;
    }

    private void processAccessTokenRevocation(final OidcAuthorizedClient oidcAuthorizedClient, final String userIdentity) {
        final OAuth2AccessToken accessToken = oidcAuthorizedClient.getAccessToken();
        final String token = accessToken.getTokenValue();
        final TokenRevocationRequest revocationRequest = new TokenRevocationRequest(token, TokenTypeHint.ACCESS_TOKEN.getHint());
        final TokenRevocationResponse revocationResponse = tokenRevocationResponseClient.getRevocationResponse(revocationRequest);
        logger.info("Identity [{}] OIDC Access Token Revocation completed [HTTP {}]", userIdentity, revocationResponse.getStatusCode());
    }

    private void processRefreshTokenRevocation(final OidcAuthorizedClient oidcAuthorizedClient, final String userIdentity) {
        final OAuth2RefreshToken refreshToken = oidcAuthorizedClient.getRefreshToken();
        if (refreshToken == null) {
            logger.debug("Identity [{}] OIDC Refresh Token not found for revocation", userIdentity);
        } else {
            final String token = refreshToken.getTokenValue();
            final TokenRevocationRequest revocationRequest = new TokenRevocationRequest(token, TokenTypeHint.REFRESH_TOKEN.getHint());
            final TokenRevocationResponse revocationResponse = tokenRevocationResponseClient.getRevocationResponse(revocationRequest);
            logger.info("Identity [{}] OIDC Refresh Token Revocation completed [HTTP {}]", userIdentity, revocationResponse.getStatusCode());
        }
    }

    private OidcAuthorizedClient getOidcAuthorizedClient(final ClientRegistration clientRegistration, final String userIdentity, final HttpServletRequest request) {
        final OidcAuthorizedClient oidcAuthorizedClient;

        final String clientRegistrationId = clientRegistration.getRegistrationId();
        final LogoutAuthenticationToken authenticationToken = new LogoutAuthenticationToken(userIdentity);
        final OAuth2AuthorizedClient authorizedClient = authorizedClientRepository.loadAuthorizedClient(clientRegistrationId, authenticationToken, request);
        if (authorizedClient == null) {
            logger.warn("Identity [{}] OpenID Connect Authorized Client not found", userIdentity);
            oidcAuthorizedClient = null;
        } else if (authorizedClient instanceof OidcAuthorizedClient) {
            oidcAuthorizedClient = (OidcAuthorizedClient) authorizedClient;
        } else {
            logger.error("Identity [{}] OpenID Connect Authorized Client Class not found [{}]", userIdentity, authorizedClient.getClass());
            oidcAuthorizedClient = null;
        }

        return oidcAuthorizedClient;
    }

    private URI getEndSessionEndpoint(final ClientRegistration clientRegistration) {
        final ClientRegistration.ProviderDetails providerDetails = clientRegistration.getProviderDetails();
        final Map<String, Object> configurationMetadata = providerDetails.getConfigurationMetadata();
        final Object endSessionEndpoint = configurationMetadata.get(END_SESSION_ENDPOINT);
        return endSessionEndpoint == null ? null : URI.create(endSessionEndpoint.toString());
    }

    private String getEndSessionTargetUrl(final URI endSessionEndpoint, final String idToken, final String postLogoutRedirectUri) {
        final UriComponentsBuilder builder = UriComponentsBuilder.fromUri(endSessionEndpoint);
        builder.queryParam(ID_TOKEN_HINT_PARAMETER, idToken);
        builder.queryParam(POST_LOGOUT_REDIRECT_URI_PARAMETER, postLogoutRedirectUri);
        return builder.encode(StandardCharsets.UTF_8).build().toUriString();
    }

    private String getPostLogoutRedirectUri(final HttpServletRequest request) {
        return RequestUriBuilder.fromHttpServletRequest(request)
                .path(LOGOUT_COMPLETE_PATH)
                .build()
                .toString();
    }
}
