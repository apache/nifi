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
package org.apache.nifi.web.security.csrf;

import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.cookie.ApplicationCookieService;
import org.apache.nifi.web.security.cookie.StandardApplicationCookieService;
import org.apache.nifi.web.security.http.SecurityCookieName;
import org.apache.nifi.web.security.http.SecurityHeader;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.springframework.http.ResponseCookie;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.CsrfTokenRepository;
import org.springframework.security.web.csrf.DefaultCsrfToken;
import org.springframework.util.StringUtils;
import org.springframework.web.util.WebUtils;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;

/**
 * Standard implementation of CSRF Token Repository using stateless Spring Security double-submit cookie strategy
 */
public class StandardCookieCsrfTokenRepository implements CsrfTokenRepository {
    private static final String REQUEST_PARAMETER = "requestToken";

    private static final ApplicationCookieService applicationCookieService = new CsrfApplicationCookieService();

    /**
     * Generate CSRF Token or return current Token when present in HTTP Servlet Request Cookie header
     *
     * @param httpServletRequest HTTP Servlet Request
     * @return CSRF Token
     */
    @Override
    public CsrfToken generateToken(final HttpServletRequest httpServletRequest) {
        CsrfToken csrfToken = loadToken(httpServletRequest);
        if (csrfToken == null) {
            csrfToken = getCsrfToken(generateRandomToken());
        }
        return csrfToken;
    }

    /**
     * Save CSRF Token in HTTP Servlet Response using defaults that allow JavaScript read for session cookies
     *
     * @param csrfToken CSRF Token to be saved or null indicated the token should be removed
     * @param httpServletRequest HTTP Servlet Request
     * @param httpServletResponse HTTP Servlet Response
     */
    @Override
    public void saveToken(final CsrfToken csrfToken, final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse) {
        final URI uri = RequestUriBuilder.fromHttpServletRequest(httpServletRequest).build();
        if (csrfToken == null) {
            applicationCookieService.removeCookie(uri, httpServletResponse, ApplicationCookieName.REQUEST_TOKEN);
        } else {
            final String token = csrfToken.getToken();
            applicationCookieService.addSessionCookie(uri, httpServletResponse, ApplicationCookieName.REQUEST_TOKEN, token);
        }
    }

    /**
     * Load CSRF Token from HTTP Servlet Request Cookie header
     *
     * @param httpServletRequest HTTP Servlet Request
     * @return CSRF Token or null when Cookie header not found
     */
    @Override
    public CsrfToken loadToken(final HttpServletRequest httpServletRequest) {
        final Cookie cookie = WebUtils.getCookie(httpServletRequest, SecurityCookieName.REQUEST_TOKEN.getName());
        final String token = cookie == null ? null : cookie.getValue();
        return StringUtils.hasLength(token) ? getCsrfToken(token) : null;
    }

    private CsrfToken getCsrfToken(final String token) {
        return new DefaultCsrfToken(SecurityHeader.REQUEST_TOKEN.getHeader(), REQUEST_PARAMETER, token);
    }

    private String generateRandomToken() {
        return UUID.randomUUID().toString();
    }

    private static class CsrfApplicationCookieService extends StandardApplicationCookieService {
        private static final boolean HTTP_ONLY_DISABLED = false;

        /**
         * Get Response Cookie Builder with HttpOnly disabled allowing JavaScript to read value for subsequent requests
         *
         * @param resourceUri Resource URI containing path and domain
         * @param applicationCookieName Application Cookie Name to be used
         * @param value Cookie value
         * @param maxAge Max Age
         * @return Response Cookie Builder
         */
        @Override
        protected ResponseCookie.ResponseCookieBuilder getCookieBuilder(final URI resourceUri,
                                                                        final ApplicationCookieName applicationCookieName,
                                                                        final String value,
                                                                        final Duration maxAge) {
            final ResponseCookie.ResponseCookieBuilder builder = super.getCookieBuilder(resourceUri, applicationCookieName, value, maxAge);
            builder.httpOnly(HTTP_ONLY_DISABLED);
            return builder;
        }
    }
}
