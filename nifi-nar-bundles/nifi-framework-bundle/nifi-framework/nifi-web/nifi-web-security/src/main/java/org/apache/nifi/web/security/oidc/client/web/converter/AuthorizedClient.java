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

import java.util.Objects;

/**
 * Serialized representation of Authorized Client
 */
public class AuthorizedClient {
    private final String principalName;

    private final AuthorizedToken accessToken;

    private final AuthorizedToken refreshToken;

    private final AuthorizedToken idToken;

    @JsonCreator
    public AuthorizedClient(
            @JsonProperty("principalName")
            final String principalName,
            @JsonProperty("accessToken")
            final AuthorizedToken accessToken,
            @JsonProperty("refreshToken")
            final AuthorizedToken refreshToken,
            @JsonProperty("idToken")
            final AuthorizedToken idToken
    ) {
        this.principalName = Objects.requireNonNull(principalName, "Principal Name required");
        this.accessToken = Objects.requireNonNull(accessToken, "Access Token required");
        this.refreshToken = refreshToken;
        this.idToken = Objects.requireNonNull(idToken, "ID Token required");
    }

    public String getPrincipalName() {
        return principalName;
    }

    public AuthorizedToken getAccessToken() {
        return accessToken;
    }

    public AuthorizedToken getRefreshToken() {
        return refreshToken;
    }

    public AuthorizedToken getIdToken() {
        return idToken;
    }
}
