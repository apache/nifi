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
package org.apache.nifi.web.security.token;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

/**
 * Login Authentication Token containing mapped Identity and Expiration Time to exchange for Application Bearer Token
 */
public class LoginAuthenticationToken extends AbstractAuthenticationToken {

    private final String identity;

    private final Instant expiration;

    /**
     * Creates a representation of the authentication token for a user.
     *
     * @param identity    The unique identifier for this user (cannot be null or empty)
     * @param expiration  Instant at which the authenticated token is no longer valid
     * @param authorities The authorities that have been granted this token.
     */
    public LoginAuthenticationToken(final String identity, final Instant expiration, Collection<? extends GrantedAuthority> authorities) {
        super(authorities);
        setAuthenticated(true);
        this.identity = identity;
        this.expiration = Objects.requireNonNull(expiration);
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return identity;
    }

    public Instant getExpiration() {
        return expiration;
    }

    @Override
    public String getName() {
        return identity;
    }

    @Override
    public String toString() {
        return getName();
    }
}
