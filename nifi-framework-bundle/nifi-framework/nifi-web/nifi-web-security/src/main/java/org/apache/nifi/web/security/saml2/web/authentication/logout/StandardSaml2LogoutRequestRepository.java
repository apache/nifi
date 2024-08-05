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
package org.apache.nifi.web.security.saml2.web.authentication.logout;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.security.saml2.provider.service.authentication.logout.Saml2LogoutRequest;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutRequestRepository;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Standard implementation of SAML 2 Logout Request Repository using cookies
 */
public class StandardSaml2LogoutRequestRepository implements Saml2LogoutRequestRepository {
    private static final Logger logger = LoggerFactory.getLogger(StandardSaml2LogoutRequestRepository.class);

    private static final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final Cache cache;

    public StandardSaml2LogoutRequestRepository(final Cache cache) {
        this.cache = Objects.requireNonNull(cache, "Cache required");
    }

    /**
     * Load Logout Request
     *
     * @param request HTTP Servlet Request
     * @return SAML 2 Logout Request or null when not found
     */
    @Override
    public Saml2LogoutRequest loadLogoutRequest(final HttpServletRequest request) {
        Objects.requireNonNull(request, "Request required");

        final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);

        final Saml2LogoutRequest logoutRequest;
        if (requestIdentifier.isPresent()) {
            final String identifier = requestIdentifier.get();
            logoutRequest = cache.get(identifier, Saml2LogoutRequest.class);
            if (logoutRequest == null) {
                logger.warn("SAML Logout Request [{}] not found", identifier);
            }
        } else {
            logger.warn("SAML Logout Request Identifier cookie not found");
            logoutRequest = null;
        }
        return logoutRequest;
    }

    /**
     * Save Logout Request in cache and set cookies
     *
     * @param logoutRequest Logout Request to be saved
     * @param request HTTP Servlet Request required
     * @param response HTTP Servlet Response required
     */
    @Override
    public void saveLogoutRequest(final Saml2LogoutRequest logoutRequest, final HttpServletRequest request, final HttpServletResponse response) {
        Objects.requireNonNull(request, "Request required");
        Objects.requireNonNull(response, "Response required");
        if (logoutRequest == null) {
            removeLogoutRequest(request, response);
        } else {
            Objects.requireNonNull(logoutRequest.getRelayState(), "Relay State required");

            // Get current Logout Request Identifier or generate when not found
            final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
            final String identifier = requestIdentifier.orElse(UUID.randomUUID().toString());
            cache.put(identifier, logoutRequest);

            final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).build();
            applicationCookieService.addCookie(resourceUri, response, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER, identifier);
            logger.debug("SAML Logout Request [{}] saved", identifier);
        }
    }

    /**
     * Remove Logout Request
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @return SAML 2 Logout Request removed or null when not found
     */
    @Override
    public Saml2LogoutRequest removeLogoutRequest(final HttpServletRequest request, final HttpServletResponse response) {
        Objects.requireNonNull(request, "Request required");
        Objects.requireNonNull(response, "Response required");
        final Saml2LogoutRequest logoutRequest = loadLogoutRequest(request);
        if (logoutRequest == null) {
            logger.warn("SAML Logout Request not found");
        } else {
            final URI resourceUri = RequestUriBuilder.fromHttpServletRequest(request).build();
            applicationCookieService.removeCookie(resourceUri, response, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);

            final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
            requestIdentifier.ifPresent(cache::evict);
        }
        return logoutRequest;
    }
}
