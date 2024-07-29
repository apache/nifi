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
package org.apache.nifi.web.security;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.util.StringUtils;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard Authentication Entry Point delegates to Bearer Authentication Entry Point and performs additional processing
 */
public class StandardAuthenticationEntryPoint implements AuthenticationEntryPoint {
    protected static final String AUTHENTICATE_HEADER = "WWW-Authenticate";

    protected static final String BEARER_HEADER = "Bearer";

    protected static final String UNAUTHORIZED = "Unauthorized";

    protected static final String EXPIRED_JWT = "Expired JWT";

    protected static final String SESSION_EXPIRED = "Session Expired";

    private static final String ROOT_PATH = "/";

    private static final ApplicationCookieService applicationCookieService = new StandardApplicationCookieService();

    private final BearerTokenAuthenticationEntryPoint bearerTokenAuthenticationEntryPoint;

    public StandardAuthenticationEntryPoint(final BearerTokenAuthenticationEntryPoint bearerTokenAuthenticationEntryPoint) {
        this.bearerTokenAuthenticationEntryPoint = Objects.requireNonNull(bearerTokenAuthenticationEntryPoint);
    }

    /**
     * Commence exception handling with handling for OAuth2 Authentication Exceptions using Bearer Token implementation
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param exception Authentication Exception
     * @throws IOException Thrown on response processing failures
     */
    @Override
    public void commence(final HttpServletRequest request, final HttpServletResponse response, final AuthenticationException exception) throws IOException {
        if (exception instanceof OAuth2AuthenticationException) {
            bearerTokenAuthenticationEntryPoint.commence(request, response, exception);
        } else {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
        removeAuthorizationBearerCookie(request, response);
        sendErrorMessage(response, exception);
    }

    private void sendErrorMessage(final HttpServletResponse response, final AuthenticationException exception) throws IOException {
        response.setContentType(MediaType.TEXT_PLAIN_VALUE);
        final String message = getErrorMessage(response, exception);
        try (final PrintWriter writer = response.getWriter()) {
            writer.print(message);
        }
    }

    private String getErrorMessage(final HttpServletResponse response, final AuthenticationException exception) {
        // Use WWW-Authenticate Header from BearerTokenAuthenticationEntryPoint when found
        final String authenticateHeader = response.getHeader(AUTHENTICATE_HEADER);
        final String errorMessage = authenticateHeader == null ? UNAUTHORIZED : authenticateHeader;
        final String formattedErrorMessage = errorMessage.replaceFirst(BEARER_HEADER, UNAUTHORIZED);

        // Use simplified message for Expired JWT exceptions
        final String exceptionMessage = exception.getMessage();
        return StringUtils.endsWithIgnoreCase(exceptionMessage, EXPIRED_JWT) ? SESSION_EXPIRED : formattedErrorMessage;
    }

    private void removeAuthorizationBearerCookie(final HttpServletRequest request, final HttpServletResponse response) {
        final Optional<String> authorizationBearer = applicationCookieService.getCookieValue(request, ApplicationCookieName.AUTHORIZATION_BEARER);
        if (authorizationBearer.isPresent()) {
            final URI uri = RequestUriBuilder.fromHttpServletRequest(request).path(ROOT_PATH).build();
            applicationCookieService.removeCookie(uri, response, ApplicationCookieName.AUTHORIZATION_BEARER);
        }
    }
}
