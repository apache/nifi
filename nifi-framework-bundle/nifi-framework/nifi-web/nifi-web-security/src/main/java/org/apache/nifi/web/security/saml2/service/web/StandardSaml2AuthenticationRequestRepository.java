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
package org.apache.nifi.web.security.saml2.service.web;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.security.saml2.provider.service.authentication.AbstractSaml2AuthenticationRequest;
import org.springframework.security.saml2.provider.service.web.Saml2AuthenticationRequestRepository;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Standard implementation of SAML 2 Authentication Request Repository using cookies
 */
public class StandardSaml2AuthenticationRequestRepository implements Saml2AuthenticationRequestRepository<AbstractSaml2AuthenticationRequest> {
    private static final Logger logger = LoggerFactory.getLogger(StandardSaml2AuthenticationRequestRepository.class);

    private static final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final Cache cache;

    /**
     * Standard SAML 2 Authentication Request Repository with Spring Cache abstraction
     *
     * @param cache Spring Cache for Authentication Requests
     */
    public StandardSaml2AuthenticationRequestRepository(final Cache cache) {
        this.cache = Objects.requireNonNull(cache, "Cache required");
    }

    /**
     * Load Authentication Request based on SAML Request Identifier cookies
     *
     * @param request HTTP Servlet Request
     * @return SAML 2 Authentication Request or null when not found
     */
    @Override
    public AbstractSaml2AuthenticationRequest loadAuthenticationRequest(final HttpServletRequest request) {
        final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.SAML_REQUEST_IDENTIFIER);

        final AbstractSaml2AuthenticationRequest authenticationRequest;
        if (requestIdentifier.isPresent()) {
            final String identifier = requestIdentifier.get();
            authenticationRequest = cache.get(identifier, AbstractSaml2AuthenticationRequest.class);
            if (authenticationRequest == null) {
                logger.warn("SAML Authentication Request [{}] not found", identifier);
            }
        } else {
            logger.warn("SAML Authentication Request Identifier cookie not found");
            authenticationRequest = null;
        }
        return authenticationRequest;
    }

    /**
     * Save Authentication Request in cache and add cookies to HTTP responses
     *
     * @param authenticationRequest Authentication Request to be saved
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     */
    @Override
    public void saveAuthenticationRequest(final AbstractSaml2AuthenticationRequest authenticationRequest, final HttpServletRequest request, final HttpServletResponse response) {
        if (authenticationRequest == null) {
            removeAuthenticationRequest(request, response);
        } else {
            final String identifier = UUID.randomUUID().toString();
            cache.put(identifier, authenticationRequest);

            final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).build();
            applicationCookieService.addCookie(resourceUri, response, ApplicationCookieName.SAML_REQUEST_IDENTIFIER, identifier);
            logger.debug("SAML Authentication Request [{}] saved", identifier);
        }
    }

    /**
     * Remove Authentication Request from cache and remove cookies
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @return SAML 2 Authentication Request removed or null when not found
     */
    @Override
    public AbstractSaml2AuthenticationRequest removeAuthenticationRequest(final HttpServletRequest request, final HttpServletResponse response) {
        final AbstractSaml2AuthenticationRequest authenticationRequest = loadAuthenticationRequest(request);
        if (authenticationRequest == null) {
            logger.warn("SAML Authentication Request not found");
        } else {
            final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).build();
            applicationCookieService.removeCookie(resourceUri, response, ApplicationCookieName.SAML_REQUEST_IDENTIFIER);

            final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.SAML_REQUEST_IDENTIFIER);
            requestIdentifier.ifPresent(cache::evict);
        }
        return authenticationRequest;
    }
}
