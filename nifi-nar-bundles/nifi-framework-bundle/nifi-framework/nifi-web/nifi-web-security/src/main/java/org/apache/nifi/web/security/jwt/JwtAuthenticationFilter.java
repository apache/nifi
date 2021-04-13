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
package org.apache.nifi.web.security.jwt;

import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.web.util.WebUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class JwtAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    // The Authorization header contains authentication credentials
    public static final String AUTHORIZATION = "Authorization";
    public static final String JWT_COOKIE_NAME = "__Host-jwt-auth-cookie";
    private static final Pattern BEARER_HEADER_PATTERN = Pattern.compile("^Bearer (\\S*\\.\\S*\\.\\S*)$");
    private static final Pattern JWT_PATTERN = Pattern.compile("^(\\S*\\.\\S*\\.\\S*)$");
    private static final List<String> IDEMPOTENT_METHODS = Arrays.asList("GET", "HEAD", "TRACE", "OPTIONS");


    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request) {
        // Only support JWT login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // Check for JWT in cookie and header
        final String cookieToken = WebUtils.getCookie(request, JWT_COOKIE_NAME).getValue();
        final String headerToken = getTokenFromHeader(request);
        if (cookieToken != null && !cookieToken.isEmpty()) {
            if (!IDEMPOTENT_METHODS.contains(request.getMethod().toUpperCase())) {
                // To protect against CSRF when using a cookie, if the request method requires authentication the request must have a matching Authorization header JWT
                if (headerToken.equals(cookieToken)) {
                    return new JwtAuthenticationRequestToken(headerToken, request.getRemoteAddr());
                } else {
                    throw new InvalidAuthenticationException("Authorization HTTP header and authentication cookie did not match.");
                }
            } else {
                return new JwtAuthenticationRequestToken(cookieToken, request.getRemoteAddr());
            }
        } else if (StringUtils.isNotBlank(headerToken)) {
            return new JwtAuthenticationRequestToken(headerToken, request.getRemoteAddr());
        } else {
            return null;
        }
    }

    private boolean validJwtFormat(String authenticationHeader) {
        Matcher matcher = BEARER_HEADER_PATTERN.matcher(authenticationHeader);
        return matcher.matches();
    }

    private String getTokenFromHeader(final HttpServletRequest request) {
        final String authorizationHeader = request.getHeader(AUTHORIZATION);

        // if there is no authorization header, we don't know the user
        if (authorizationHeader == null || !validJwtFormat(authorizationHeader)) {
            return null;
        } else {
            // Extract the Base64 encoded token from the Authorization header
            return getTokenFromHeader(authorizationHeader);
        }
    }

    public static String getTokenFromHeader(String authenticationHeader) {
        Matcher matcher = BEARER_HEADER_PATTERN.matcher(authenticationHeader);
        if(matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new InvalidAuthenticationException("JWT did not match expected pattern.");
        }
    }
}
