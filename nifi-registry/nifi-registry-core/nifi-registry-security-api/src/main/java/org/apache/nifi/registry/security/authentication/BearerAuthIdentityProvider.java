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

public abstract class BearerAuthIdentityProvider implements IdentityProvider {

    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER = "Bearer ";

    private static final Logger logger = LoggerFactory.getLogger(BearerAuthIdentityProvider.class);

    private static final IdentityProviderUsage usage = new IdentityProviderUsage() {
        @Override
        public String getText() {
            return "The user credentials must be passed in standard HTTP Bearer Authorization format. " +
                    "That is: 'Authorization: Bearer <token>', " +
                    "where <token> is a value that will be validated by this identity provider.";
        }

        @Override
        public AuthType getAuthType() {
            return AuthType.BEARER;
        }
    };

    @Override
    public IdentityProviderUsage getUsageInstructions() {
        return usage;
    }

    @Override
    public AuthenticationRequest extractCredentials(HttpServletRequest request) {

        if (request == null) {
            logger.debug("Cannot extract user credentials from null servletRequest");
            return null;
        }

        // only support this type of login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // get the principal out of the user token
        final String authorization = request.getHeader(AUTHORIZATION);
        if (authorization == null || !authorization.startsWith(BEARER)) {
            logger.debug("HTTP Bearer Auth credentials not present. Not attempting to extract credentials for authentication.");
            return null;
        }

        // Extract the encoded token from the Authorization header
        final String token = authorization.substring(BEARER.length()).trim();

        return new AuthenticationRequest(null, token, null);

    }

}
