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

import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.BearerTokenError;
import org.springframework.security.oauth2.server.resource.BearerTokenErrors;

import static org.springframework.security.oauth2.core.OAuth2TokenValidatorResult.success;
import static org.springframework.security.oauth2.core.OAuth2TokenValidatorResult.failure;

/**
 * JSON Web Token Validator checks the JWT Identifier against a Revocation Service
 */
public class JwtRevocationValidator implements OAuth2TokenValidator<Jwt> {
    private static final BearerTokenError REVOKED_ERROR = BearerTokenErrors.invalidToken("Access Token Revoked");

    private static final OAuth2TokenValidatorResult FAILURE_RESULT = failure(REVOKED_ERROR);

    private final JwtRevocationService jwtRevocationService;

    public JwtRevocationValidator(final JwtRevocationService jwtRevocationService) {
        this.jwtRevocationService = jwtRevocationService;
    }

    /**
     * Validate checks JSON Web Token Identifier against Revocation Service
     *
     * @param jwt JSON Web Token Identifier
     * @return Validator Result based on Revoked Status
     */
    @Override
    public OAuth2TokenValidatorResult validate(final Jwt jwt) {
        return jwtRevocationService.isRevoked(jwt.getId()) ? FAILURE_RESULT : success();
    }
}
