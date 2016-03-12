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

import io.jsonwebtoken.JwtException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.apache.nifi.web.security.token.NewAccountAuthorizationRequestToken;
import org.apache.nifi.web.security.token.NiFiAuthorizationRequestToken;
import org.apache.nifi.web.security.user.NewAccountRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import org.apache.nifi.web.security.InvalidAuthenticationException;

/**
 */
public class JwtAuthenticationFilter extends NiFiAuthenticationFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);

    public static final String AUTHORIZATION = "Authorization";

    private JwtService jwtService;

    @Override
    public NiFiAuthorizationRequestToken attemptAuthentication(final HttpServletRequest request) {
        // only suppport jwt login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // TODO: Refactor request header extraction logic to shared utility as it is duplicated in AccessResource

        // get the principal out of the user token
        final String authorization = request.getHeader(AUTHORIZATION);

        // if there is no authorization header, we don't know the user
        if (authorization == null) {
            return null;
        } else {
            if (jwtService == null) {
                throw new InvalidAuthenticationException("NiFi is not configured to support username/password logins.");
            }

            // Extract the Base64 encoded token from the Authorization header
            final String token = StringUtils.substringAfterLast(authorization, " ");

            try {
                final String jwtPrincipal = jwtService.getAuthenticationFromToken(token);

                if (isNewAccountRequest(request)) {
                    return new NewAccountAuthorizationRequestToken(new NewAccountRequest(Arrays.asList(jwtPrincipal), getJustification(request)));
                } else {
                    return new NiFiAuthorizationRequestToken(Arrays.asList(jwtPrincipal));
                }
            } catch (JwtException e) {
                throw new InvalidAuthenticationException(e.getMessage(), e);
            }
        }
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

}
