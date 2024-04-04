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
package org.apache.nifi.web.security.jwt.revocation;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

/**
 * Standard JSON Web Token Logout Listener handles parsing and revocation
 */
public class StandardJwtLogoutListener implements JwtLogoutListener {
    /** JWT Decoder */
    private final JwtDecoder jwtDecoder;

    /** JWT Revocation Service */
    private final JwtRevocationService jwtRevocationService;

    public StandardJwtLogoutListener(final JwtDecoder jwtDecoder, final JwtRevocationService jwtRevocationService) {
        this.jwtDecoder = jwtDecoder;
        this.jwtRevocationService = jwtRevocationService;
    }

    /**
     * Logout Bearer Token sets revoked status using the JSON Web Token Identifier
     *
     * @param bearerToken Bearer Token
     */
    @Override
    public void logout(final String bearerToken) {
        if (StringUtils.isNotBlank(bearerToken)) {
            final Jwt jwt = jwtDecoder.decode(bearerToken);
            jwtRevocationService.setRevoked(jwt.getId(), jwt.getExpiresAt());
        }
    }
}
