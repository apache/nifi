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
package org.apache.nifi.web.security.oidc.authentication;

import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.web.client.RestOperations;

/**
 * Access Token Decoder Factory provides Token Validation based on expected Access Token claims instead of ID Token Claims
 */
public class AccessTokenDecoderFactory extends StandardOidcIdTokenDecoderFactory {
    /**
     * Standard constructor with optional JWS Algorithm and required REST Operations for retrieving JSON Web Keys
     *
     * @param preferredJwsAlgorithm Preferred JSON Web Signature Algorithm default to RS256 when not provided
     * @param restOperations REST Operations required for retrieving JSON Web Key Set with Signature Algorithms
     */
    public AccessTokenDecoderFactory(final String preferredJwsAlgorithm, final RestOperations restOperations) {
        super(preferredJwsAlgorithm, restOperations);
    }

    /**
     * Get OAuth2 Token Validator with Timestamp and Issuer Validators
     *
     * @param clientRegistration Client Registration
     * @return OAuth2 Token Validator with Timestamp and Issuer Validators
     */
    @Override
    protected OAuth2TokenValidator<Jwt> getTokenValidator(final ClientRegistration clientRegistration) {
        final String issuerUri = clientRegistration.getProviderDetails().getIssuerUri();
        return JwtValidators.createDefaultWithIssuer(issuerUri);
    }
}
