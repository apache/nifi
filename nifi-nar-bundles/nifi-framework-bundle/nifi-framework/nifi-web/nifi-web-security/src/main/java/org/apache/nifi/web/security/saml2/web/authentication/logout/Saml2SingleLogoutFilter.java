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
import org.apache.nifi.web.security.saml2.SamlUrlPath;
import org.apache.nifi.web.security.token.LogoutAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * SAML 2 Single Local Filter processes Single Logout Requests
 */
public class Saml2SingleLogoutFilter extends OncePerRequestFilter {
    private static final Logger logger = LoggerFactory.getLogger(Saml2SingleLogoutFilter.class);

    private static final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final AntPathRequestMatcher requestMatcher = new AntPathRequestMatcher(SamlUrlPath.SINGLE_LOGOUT_REQUEST.getPath());

    private final LogoutRequestManager logoutRequestManager;

    private final LogoutSuccessHandler logoutSuccessHandler;

    public Saml2SingleLogoutFilter(
            final LogoutRequestManager logoutRequestManager,
            final LogoutSuccessHandler logoutSuccessHandler
    ) {
        this.logoutRequestManager = Objects.requireNonNull(logoutRequestManager, "Request Manager require");
        this.logoutSuccessHandler = Objects.requireNonNull(logoutSuccessHandler, "Success Handler required");
    }

    /**
     * Read Logout Request Identifier cookies and find Logout Request then call Logout Success Handler
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param filterChain Filter Chain
     * @throws ServletException Thrown on FilterChain.doFilter() failures
     * @throws IOException Thrown on FilterChain.doFilter() failures
     */
    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response, final FilterChain filterChain) throws ServletException, IOException {
        if (requestMatcher.matches(request)) {
            final Optional<String> requestIdentifier = applicationCookieService.getCookieValue(request, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
            if (requestIdentifier.isPresent()) {
                final String identifier = requestIdentifier.get();
                final LogoutRequest logoutRequest = logoutRequestManager.get(identifier);
                if (logoutRequest == null) {
                    logger.warn("SAML 2 Logout Request [{}] not found", identifier);
                } else {
                    final String userIdentity = logoutRequest.getMappedUserIdentity();
                    final Authentication authentication = new LogoutAuthenticationToken(userIdentity);
                    logoutSuccessHandler.onLogoutSuccess(request, response, authentication);
                }
            } else {
                logger.warn("SAML 2 Logout Request cookie not found");
            }
        } else {
            filterChain.doFilter(request, response);
        }
    }
}
