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

import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

import javax.servlet.http.HttpServletRequest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class JwtAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    // The Authorization header contains authentication credentials
    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER = "Bearer ";
    private static final Pattern tokenPattern = Pattern.compile("^Bearer (\\S*\\.\\S*\\.\\S*)$");

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request) {
        // only support jwt login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // TODO: Refactor request header extraction logic to shared utility as it is duplicated in AccessResource

        // get the principal out of the user token
        final String authentication = request.getHeader(AUTHORIZATION);

        // if there is no authorization (authentication) header, we don't know the user
        if (authentication == null || !validJwtFormat(authentication)) {
            return null;
        } else {
            // Extract the Base64 encoded token from the Authorization header
            final String token = getTokenFromHeader(authentication);
            return new JwtAuthenticationRequestToken(token, request.getRemoteAddr());
        }
    }

    private boolean validJwtFormat(String authenticationHeader) {
        Matcher matcher = tokenPattern.matcher(authenticationHeader);
        return matcher.matches();
    }

    private String getTokenFromHeader(String authenticationHeader) {
        Matcher matcher = tokenPattern.matcher(authenticationHeader);
        matcher.matches();
        return matcher.group(1);
    }

}
