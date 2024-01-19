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
package org.apache.nifi.registry.web.security.authentication.csrf;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.CsrfTokenRepository;
import org.springframework.security.web.csrf.DefaultCsrfToken;
import org.springframework.util.StringUtils;
import org.springframework.web.util.WebUtils;

import java.time.Duration;
import java.util.UUID;

/**
 * Standard implementation of CSRF Token Repository using stateless Spring Security double-submit cookie strategy
 */
public class StandardCookieCsrfTokenRepository implements CsrfTokenRepository {
    private static final String REQUEST_HEADER = "Request-Token";

    private static final String REQUEST_PARAMETER = "requestToken";

    private static final Duration MAX_AGE_SESSION = Duration.ofSeconds(-1);

    private static final Duration MAX_AGE_REMOVE = Duration.ZERO;

    private static final String EMPTY_VALUE = "";

    private static final String SAME_SITE_POLICY_STRICT = "Strict";

    private static final String ROOT_PATH = "/";

    private static final boolean SECURE_ENABLED = true;

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
        if (csrfToken == null) {
            final ResponseCookie responseCookie = getResponseCookie(EMPTY_VALUE, MAX_AGE_REMOVE);
            httpServletResponse.addHeader(HttpHeaders.SET_COOKIE, responseCookie.toString());
        } else {
            final String token = csrfToken.getToken();
            final ResponseCookie responseCookie = getResponseCookie(token, MAX_AGE_SESSION);
            httpServletResponse.addHeader(HttpHeaders.SET_COOKIE, responseCookie.toString());
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
        final Cookie cookie = WebUtils.getCookie(httpServletRequest, CsrfCookieName.REQUEST_TOKEN.getCookieName());
        final String token = cookie == null ? null : cookie.getValue();
        return StringUtils.hasLength(token) ? getCsrfToken(token) : null;
    }

    private CsrfToken getCsrfToken(final String token) {
        return new DefaultCsrfToken(REQUEST_HEADER, REQUEST_PARAMETER, token);
    }

    private String generateRandomToken() {
        return UUID.randomUUID().toString();
    }

    private ResponseCookie getResponseCookie(final String cookieValue, final Duration maxAge) {
        return ResponseCookie.from(CsrfCookieName.REQUEST_TOKEN.getCookieName(), cookieValue)
                .sameSite(SAME_SITE_POLICY_STRICT)
                .secure(SECURE_ENABLED)
                .path(ROOT_PATH)
                .maxAge(maxAge)
                .build();
    }
}
