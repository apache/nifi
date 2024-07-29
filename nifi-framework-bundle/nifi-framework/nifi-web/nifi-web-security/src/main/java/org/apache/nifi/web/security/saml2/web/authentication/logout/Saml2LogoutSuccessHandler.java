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
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * SAML 2 Logout Success Handler implementation for completing application Logout Requests
 */
public class Saml2LogoutSuccessHandler implements LogoutSuccessHandler {
    private static final String LOGOUT_COMPLETE_PATH = "/nifi/logout-complete";

    private static final Logger logger = LoggerFactory.getLogger(Saml2LogoutSuccessHandler.class);

    private final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final LogoutRequestManager logoutRequestManager;

    public Saml2LogoutSuccessHandler(
            final LogoutRequestManager logoutRequestManager
    ) {
        this.logoutRequestManager = Objects.requireNonNull(logoutRequestManager, "Logout Request Manager required");
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
            final String requestIdentifier = logoutRequestIdentifier.get();
            final LogoutRequest logoutRequest = logoutRequestManager.complete(requestIdentifier);

            if (logoutRequest == null) {
                logger.warn("Logout Request [{}] not found", requestIdentifier);
            } else {
                final String mappedUserIdentity = logoutRequest.getMappedUserIdentity();
                logger.info("Logout Request [{}] Identity [{}] completed", requestIdentifier, mappedUserIdentity);
            }

            final URI logoutCompleteUri = RequestUriBuilder.fromHttpServletRequest(request).path(LOGOUT_COMPLETE_PATH).build();
            final String targetUrl = logoutCompleteUri.toString();
            response.sendRedirect(targetUrl);
        }
    }
}
