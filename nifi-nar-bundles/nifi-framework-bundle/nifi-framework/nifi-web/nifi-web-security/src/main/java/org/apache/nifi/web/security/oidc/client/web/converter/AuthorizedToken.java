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
package org.apache.nifi.web.security.oidc.client.web.converter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Serialized representation of Authorized Token with minimum required properties
 */
public class AuthorizedToken {
    private final String tokenValue;

    private final Instant issuedAt;

    private final Instant expiresAt;

    @JsonCreator
    public AuthorizedToken(
            @JsonProperty("tokenValue")
            final String tokenValue,
            @JsonProperty("issuedAt")
            final Instant issuedAt,
            @JsonProperty("expiresAt")
            final Instant expiresAt
    ) {
        this.tokenValue = Objects.requireNonNull(tokenValue, "Token Value required");
        this.issuedAt = issuedAt;
        this.expiresAt = expiresAt;
    }

    public String getTokenValue() {
        return tokenValue;
    }

    public Instant getIssuedAt() {
        return issuedAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }
}
