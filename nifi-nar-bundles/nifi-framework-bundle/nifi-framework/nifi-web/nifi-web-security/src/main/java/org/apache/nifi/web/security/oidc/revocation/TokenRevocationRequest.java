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
package org.apache.nifi.web.security.oidc.revocation;

import java.util.Objects;

/**
 * OAuth2 Token Revocation Request as described in RFC 7009 Section 2.1
 */
public class TokenRevocationRequest {
    private final String token;

    private final String tokenTypeHint;

    public TokenRevocationRequest(
            final String token,
            final String tokenTypeHint
    ) {
        this.token = Objects.requireNonNull(token, "Token required");
        this.tokenTypeHint = tokenTypeHint;
    }

    public String getToken() {
        return token;
    }

    public String getTokenTypeHint() {
        return tokenTypeHint;
    }
}
