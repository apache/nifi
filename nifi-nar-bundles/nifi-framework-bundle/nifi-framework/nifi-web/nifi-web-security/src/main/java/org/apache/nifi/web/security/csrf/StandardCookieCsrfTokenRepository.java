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

import org.apache.nifi.web.security.http.SecurityCookieName;
import org.apache.nifi.web.security.http.SecurityHeader;
import org.apache.nifi.web.util.RequestUriBuilder;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.CsrfTokenRepository;
import org.springframework.security.web.csrf.DefaultCsrfToken;
import org.springframework.util.StringUtils;
import org.springframework.web.util.WebUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Standard implementation of CSRF Token Repository using stateless Spring Security double-submit cookie strategy
 */
public class StandardCookieCsrfTokenRepository implements CsrfTokenRepository {
    private static final String REQUEST_PARAMETER = "requestToken";

    private static final String ROOT_PATH = "/";

    private static final String EMPTY = "";

    private static final boolean SECURE_ENABLED = true;

    private static final int MAX_AGE_EXPIRED = 0;

    private static final int MAX_AGE_SESSION = -1;

    private final List<String> allowedContextPaths;

    /**
     * Standard Cookie CSRF Token Repository with list of allowed context paths from proxy headers
     *
     * @param allowedContextPaths Allowed context paths from proxy headers
     */
    public StandardCookieCsrfTokenRepository(final List<String> allowedContextPaths) {
        this.allowedContextPaths = Objects.requireNonNull(allowedContextPaths, "Allowed Context Paths required");
    }

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
        final String token = csrfToken == null ? EMPTY : csrfToken.getToken();
        final int maxAge = csrfToken == null ? MAX_AGE_EXPIRED : MAX_AGE_SESSION;

        final Cookie cookie = new Cookie(SecurityCookieName.REQUEST_TOKEN.getName(), token);
        cookie.setSecure(SECURE_ENABLED);
        cookie.setMaxAge(maxAge);

        final String cookiePath = getCookiePath(httpServletRequest);
        cookie.setPath(cookiePath);
        httpServletResponse.addCookie(cookie);
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

    private String getCookiePath(final HttpServletRequest httpServletRequest) {
        final RequestUriBuilder requestUriBuilder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest, allowedContextPaths);
        requestUriBuilder.path(ROOT_PATH);
        final URI uri = requestUriBuilder.build();
        return uri.getPath();
    }
}
