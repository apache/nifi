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

import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

import javax.servlet.http.HttpServletRequest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JwtAuthenticationFilter is used to authenticate requests that contain a JSON Web Token in the Authorization HTTP header.
 */
public class JwtAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    // The Authorization header contains authentication credentials
    public static final String AUTHORIZATION = "Authorization";
    private static final Pattern tokenPattern = Pattern.compile("^Bearer (\\S*\\.\\S*\\.\\S*)$");

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request) {
        // only support jwt login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // TODO: Refactor request header extraction logic to shared utility as it is duplicated in AccessResource

        // Get the principal out of the user token
        final String authorizationHeader = request.getHeader(AUTHORIZATION);

        // If there is no authorization header, we can't know the user
        if (authorizationHeader == null || !isValidJwtFormat(authorizationHeader)) {
            return null;
        } else {
            // Extract the Base64 encoded JWT from the Authorization header
            final String token = getTokenFromHeader(authorizationHeader);
            return new JwtAuthenticationRequestToken(token, request.getRemoteAddr());
        }
    }


    /**
     * Checks if the Authorization HTTP header value matches the expected 'Bearer: ${header.payload.signature}' pattern.
     * @param authorizationHeader The HTTP 'Authorization' header which actually contains authentication credentials (the JWT).
     * @return Returns true if the header matches expected 'Bearer: ${header.payload.signature}' format, false if it doesn't.
     */
    private boolean isValidJwtFormat(String authorizationHeader) {
        Matcher matcher = tokenPattern.matcher(authorizationHeader);
        return matcher.matches();
    }

    /**
     * Extracts the Base64 encoded JWT string from the Authorization HTTP header as long as it matches the
     * strict 'Bearer: ${header.payload.signature}' pattern.
     * @param authorizationHeader The HTTP 'Authorization' header which actually contains authentication credentials (the JWT).
     * @return The Base64 encoded JWT string.
     */
    private String getTokenFromHeader(String authorizationHeader) {
        Matcher matcher = tokenPattern.matcher(authorizationHeader);
        if(matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new InvalidAuthenticationException("JWT did not match expected pattern.");
        }
    }

}
