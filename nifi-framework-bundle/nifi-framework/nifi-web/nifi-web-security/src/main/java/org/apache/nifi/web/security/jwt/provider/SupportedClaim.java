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
package org.apache.nifi.web.security.jwt.provider;

/**
 * Supported Claim for JSON Web Tokens
 */
public enum SupportedClaim {
    /** RFC 7519 Section 4.1.1 */
    ISSUER("iss"),

    /** RFC 7519 Section 4.1.2 */
    SUBJECT("sub"),

    /** RFC 7519 Section 4.1.3 */
    AUDIENCE("aud"),

    /** RFC 7519 Section 4.1.4 */
    EXPIRATION("exp"),

    /** RFC 7519 Section 4.1.5 */
    NOT_BEFORE("nbf"),

    /** RFC 7519 Section 4.1.6 */
    ISSUED_AT("iat"),

    /** RFC 7519 Section 4.1.7 */
    JWT_ID("jti"),

    /** Preferred Username defined in OpenID Connect Core 1.0 Standard Claims */
    PREFERRED_USERNAME("preferred_username"),

    /** RFC 7643 Section 4.1.2 */
    GROUPS("groups");

    private final String claim;

    SupportedClaim(final String claim) {
        this.claim = claim;
    }

    public String getClaim() {
        return claim;
    }
}
