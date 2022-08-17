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
package org.apache.nifi.registry.security.authentication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.Charset;
import java.util.Base64;

public abstract class BasicAuthIdentityProvider implements IdentityProvider {

    public static final String AUTHORIZATION = "Authorization";
    public static final String BASIC = "Basic ";

    private static final Logger logger = LoggerFactory.getLogger(BasicAuthIdentityProvider.class);

    private static final IdentityProviderUsage usage = new IdentityProviderUsage() {
        @Override
        public String getText() {
            return "The user credentials must be passed in standard HTTP Basic Auth format. " +
                    "That is: 'Authorization: Basic <credentials>', " +
                    "where <credentials> is the base64 encoded value of '<username>:<password>'.";
        }

        @Override
        public AuthType getAuthType() {
            return AuthType.BASIC;
        }
    };

    @Override
    public IdentityProviderUsage getUsageInstructions() {
        return usage;
    }

    @Override
    public AuthenticationRequest extractCredentials(HttpServletRequest servletRequest) {

        if (servletRequest == null) {
            logger.debug("Cannot extract user credentials from null servletRequest");
            return null;
        }

        // only support this type of login when running securely
        if (!servletRequest.isSecure()) {
            return null;
        }

        final String authorization = servletRequest.getHeader(AUTHORIZATION);
        if (authorization == null || !authorization.startsWith(BASIC)) {
            logger.debug("HTTP Basic Auth credentials not present. Not attempting to extract credentials for authentication.");
            return null;
        }

        AuthenticationRequest authenticationRequest;

        try {

            // Authorization: Basic {base64credentials}
            String base64Credentials = authorization.substring(BASIC.length()).trim();
            String credentials = new String(Base64.getDecoder().decode(base64Credentials), Charset.forName("UTF-8"));
            // credentials = username:password
            final String[] credentialParts = credentials.split(":", 2);
            String username = credentialParts[0];
            String password = credentialParts[1];

            authenticationRequest = new UsernamePasswordAuthenticationRequest(username, password);

        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            logger.info("Failed to extract user identity credentials.");
            logger.debug("", e);
            return null;
        }

        return authenticationRequest;

    }

    @Override
    public boolean supports(Class<? extends AuthenticationRequest> authenticationRequestClazz) {
        return UsernamePasswordAuthenticationRequest.class.isAssignableFrom(authenticationRequestClazz);
    }

}
