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
package org.apache.nifi.web.security.cookie;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseCookie;
import org.springframework.web.util.WebUtils;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.HttpHeaders;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of Application Cookie Service using Spring Framework utilities
 */
public class StandardApplicationCookieService implements ApplicationCookieService {
    private static final Duration MAX_AGE_SESSION = Duration.ofSeconds(-1);

    private static final Duration MAX_AGE_REMOVE = Duration.ZERO;

    private static final Duration MAX_AGE_STANDARD = Duration.ofSeconds(60);

    private static final String DEFAULT_PATH = "/";

    private static final boolean SECURE_ENABLED = true;

    private static final boolean HTTP_ONLY_ENABLED = true;

    private static final Logger logger = LoggerFactory.getLogger(StandardApplicationCookieService.class);

    /**
     * Generate cookie with specified value
     *
     * @param resourceUri Resource URI containing path and domain
     * @param response HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name to be added
     * @param value Cookie value to be added
     */
    @Override
    public void addCookie(final URI resourceUri, final HttpServletResponse response, final ApplicationCookieName applicationCookieName, final String value) {
        final ResponseCookie.ResponseCookieBuilder responseCookieBuilder = getCookieBuilder(resourceUri, applicationCookieName, value, MAX_AGE_STANDARD);
        setResponseCookie(response, responseCookieBuilder.build());
        logger.debug("Added Cookie [{}] URI [{}]", applicationCookieName.getCookieName(), resourceUri);
    }

    /**
     * Generate cookie with session-based expiration and specified value as well as SameSite Strict property
     *
     * @param resourceUri Resource URI containing path and domain
     * @param response HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name
     * @param value Cookie value to be added
     */
    @Override
    public void addSessionCookie(final URI resourceUri, final HttpServletResponse response, final ApplicationCookieName applicationCookieName, final String value) {
        final ResponseCookie.ResponseCookieBuilder responseCookieBuilder = getCookieBuilder(resourceUri, applicationCookieName, value, MAX_AGE_SESSION);
        setResponseCookie(response, responseCookieBuilder.build());
        logger.debug("Added Session Cookie [{}] URI [{}]", applicationCookieName.getCookieName(), resourceUri);
    }

    /**
     * Get cookie value using specified name
     *
     * @param request HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name to be retrieved
     * @return Optional Cookie Value
     */
    @Override
    public Optional<String> getCookieValue(final HttpServletRequest request, final ApplicationCookieName applicationCookieName) {
        final Cookie cookie = WebUtils.getCookie(request, applicationCookieName.getCookieName());
        return cookie == null ? Optional.empty() : Optional.of(cookie.getValue());
    }

    /**
     * Generate cookie with an empty value instructing the client to remove the cookie with a maximum age of 60 seconds
     *
     * @param resourceUri Resource URI containing path and domain
     * @param response HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name to be removed
     */
    @Override
    public void removeCookie(final URI resourceUri, final HttpServletResponse response, final ApplicationCookieName applicationCookieName) {
        Objects.requireNonNull(response, "Response required");
        final ResponseCookie.ResponseCookieBuilder responseCookieBuilder = getCookieBuilder(resourceUri, applicationCookieName, StringUtils.EMPTY, MAX_AGE_REMOVE);
        setResponseCookie(response, responseCookieBuilder.build());
        logger.debug("Removed Cookie [{}] URI [{}]", applicationCookieName.getCookieName(), resourceUri);
    }

    /**
     * Get Response Cookie Builder with standard properties
     *
     * @param resourceUri Resource URI containing path and domain
     * @param applicationCookieName Application Cookie Name to be used
     * @param value Cookie value
     * @param maxAge Max Age
     * @return Response Cookie Builder
     */
    protected ResponseCookie.ResponseCookieBuilder getCookieBuilder(final URI resourceUri,
                                             final ApplicationCookieName applicationCookieName,
                                             final String value,
                                             final Duration maxAge) {
        Objects.requireNonNull(resourceUri, "Resource URI required");
        Objects.requireNonNull(applicationCookieName, "Response Cookie Name required");

        final SameSitePolicy sameSitePolicy = applicationCookieName.getSameSitePolicy();
        return ResponseCookie.from(applicationCookieName.getCookieName(), value)
                .path(getCookiePath(resourceUri))
                .domain(resourceUri.getHost())
                .sameSite(sameSitePolicy.getPolicy())
                .secure(SECURE_ENABLED)
                .httpOnly(HTTP_ONLY_ENABLED)
                .maxAge(maxAge);
    }

    private void setResponseCookie(final HttpServletResponse response, final ResponseCookie responseCookie) {
        response.addHeader(HttpHeaders.SET_COOKIE, responseCookie.toString());
    }

    private String getCookiePath(final URI resourceUri) {
        return StringUtils.defaultIfBlank(resourceUri.getPath(), DEFAULT_PATH);
    }
}
