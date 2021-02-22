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
package org.apache.nifi.web;


import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.CsrfTokenRepository;
import org.springframework.security.web.csrf.DefaultCsrfToken;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A {@link CsrfTokenRepository} implementation for NiFi that matches the NiFi Cookie JWT against the
 * Authorization header JWT to protect against CSRF. If the request is an idempotent method type, then only the Cookie
 * is required to be present - this allows authenticating access to static resources using a Cookie. If the request is a non-idempotent
 * method, NiFi requires the Authorization header (eg. for POST requests).
 */
public final class NiFiCsrfTokenRepository implements CsrfTokenRepository {

    private static String EMPTY = "empty";
    private CookieCsrfTokenRepository cookieRepository;

    public NiFiCsrfTokenRepository() {
        cookieRepository = new CookieCsrfTokenRepository();
    }

    @Override
    public CsrfToken generateToken(HttpServletRequest request) {
        // Return an empty value CsrfToken - it will not be saved to the response as our CSRF token is added elsewhere
        return new DefaultCsrfToken(EMPTY, EMPTY, EMPTY);
    }

    @Override
    public void saveToken(CsrfToken token, HttpServletRequest request,
                          HttpServletResponse response) {
        // Do nothing - we don't need to add new CSRF tokens to the response
    }

    @Override
    public CsrfToken loadToken(HttpServletRequest request) {
        CsrfToken cookie = cookieRepository.loadToken(request);
        // We add the Bearer string here in order to match the Authorization header on comparison in CsrfFilter
        return cookie != null ? new DefaultCsrfToken(cookie.getHeaderName(), cookie.getParameterName(), String.format("Bearer %s", cookie.getToken())) : null;
    }

    /**
     * Sets the name of the HTTP request parameter that should be used to provide a token.
     *
     * @param parameterName the name of the HTTP request parameter that should be used to
     * provide a token
     */
    public void setParameterName(String parameterName) {
        cookieRepository.setParameterName(parameterName);
    }

    /**
     * Sets the name of the HTTP header that should be used to provide the token.
     *
     * @param headerName the name of the HTTP header that should be used to provide the
     * token
     */
    public void setHeaderName(String headerName) {
        cookieRepository.setHeaderName(headerName);
    }

    /**
     * Sets the name of the cookie that the expected CSRF token is saved to and read from.
     *
     * @param cookieName the name of the cookie that the expected CSRF token is saved to
     * and read from
     */
    public void setCookieName(String cookieName) {
        cookieRepository.setCookieName(cookieName);
    }
}
