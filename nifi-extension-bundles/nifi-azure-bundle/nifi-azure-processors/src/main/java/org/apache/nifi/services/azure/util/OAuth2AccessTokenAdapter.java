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
package org.apache.nifi.services.azure.util;

import com.azure.core.credential.AccessToken;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * Utility methods for adapting NiFi OAuth2 access tokens to Azure SDK access tokens.
 */
public final class OAuth2AccessTokenAdapter {
    private static final long DEFAULT_EXPIRATION_OFFSET_SECONDS = 300;

    private OAuth2AccessTokenAdapter() {
    }

    public static AccessToken toAzureAccessToken(final org.apache.nifi.oauth2.AccessToken oauth2AccessToken) {
        Objects.requireNonNull(oauth2AccessToken, "OAuth2 Access Token required");

        final String tokenValue = oauth2AccessToken.getAccessToken();
        if (tokenValue == null || tokenValue.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 Access Token value required");
        }

        final Instant fetchTime = Objects.requireNonNull(oauth2AccessToken.getFetchTime(), "OAuth2 Access Token fetch time required");
        final long expiresIn = oauth2AccessToken.getExpiresIn();
        final Instant expirationInstant = expiresIn > 0
                ? fetchTime.plusSeconds(expiresIn)
                : fetchTime.plusSeconds(DEFAULT_EXPIRATION_OFFSET_SECONDS);

        final OffsetDateTime expirationTime = OffsetDateTime.ofInstant(expirationInstant, ZoneOffset.UTC);
        return new AccessToken(tokenValue, expirationTime);
    }
}
