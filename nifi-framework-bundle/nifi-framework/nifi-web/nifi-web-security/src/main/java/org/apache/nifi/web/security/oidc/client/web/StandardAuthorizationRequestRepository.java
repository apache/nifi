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

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.security.oauth2.client.web.AuthorizationRequestRepository;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * OAuth2 Authorization Request Repository with Spring Cache storage and Request Identifier tracked using HTTP cookies
 */
public class StandardAuthorizationRequestRepository implements AuthorizationRequestRepository<OAuth2AuthorizationRequest> {
    private static final Logger logger = LoggerFactory.getLogger(StandardAuthorizationRequestRepository.class);

    private static final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final Cache cache;

    /**
     * Standard Authorization Request Repository Constructor
     *
     * @param cache Spring Cache for Authorization Requests
     */
    public StandardAuthorizationRequestRepository(final Cache cache) {
        this.cache = Objects.requireNonNull(cache, "Cache required");
    }

    /**
     * Load Authorization Request using Request Identifier Cookie to lookup cached requests
     *
     * @param request HTTP Servlet Request
     * @return Cached OAuth2 Authorization Request or null when not found
     */
    @Override
    public OAuth2AuthorizationRequest loadAuthorizationRequest(final HttpServletRequest request) {
        final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.OIDC_REQUEST_IDENTIFIER);

        final OAuth2AuthorizationRequest authorizationRequest;
        if (requestIdentifier.isPresent()) {
            final String identifier = requestIdentifier.get();
            authorizationRequest = cache.get(identifier, OAuth2AuthorizationRequest.class);
            if (authorizationRequest == null) {
                logger.warn("OIDC Authentication Request [{}] not found", identifier);
            }
        } else {
            logger.warn("OIDC Authorization Request Identifier cookie not found");
            authorizationRequest = null;
        }

        return authorizationRequest;
    }

    /**
     * Save Authorization Request in cache and set HTTP Request Identifier cookie for tracking
     *
     * @param authorizationRequest OAuth2 Authorization Request to be cached
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     */
    @Override
    public void saveAuthorizationRequest(final OAuth2AuthorizationRequest authorizationRequest, final HttpServletRequest request, final HttpServletResponse response) {
        if (authorizationRequest == null) {
            removeAuthorizationRequest(request, response);
        } else {
            final String identifier = UUID.randomUUID().toString();
            cache.put(identifier, authorizationRequest);

            final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).build();
            applicationCookieService.addCookie(resourceUri, response, ApplicationCookieName.OIDC_REQUEST_IDENTIFIER, identifier);
            logger.debug("OIDC Authentication Request [{}] saved", identifier);
        }
    }

    /**
     * Remove Authorization Request from cache and remove HTTP cookie with Request Identifier
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @return OAuth2 Authorization Request removed or null when not found
     */
    @Override
    public OAuth2AuthorizationRequest removeAuthorizationRequest(final HttpServletRequest request, final HttpServletResponse response) {
        final OAuth2AuthorizationRequest authorizationRequest = loadAuthorizationRequest(request);
        if (authorizationRequest == null) {
            logger.warn("OIDC Authentication Request not removed");
        } else {
            if (response == null) {
                logger.warn("HTTP Servlet Response not specified");
            } else {
                final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).build();
                applicationCookieService.removeCookie(resourceUri, response, ApplicationCookieName.OIDC_REQUEST_IDENTIFIER);
            }

            final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.OIDC_REQUEST_IDENTIFIER);
            requestIdentifier.ifPresent(cache::evict);
        }
        return authorizationRequest;
    }
}
