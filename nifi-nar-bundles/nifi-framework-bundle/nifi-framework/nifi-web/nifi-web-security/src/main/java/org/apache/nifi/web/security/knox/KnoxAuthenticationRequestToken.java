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
package org.apache.nifi.web.security.knox;

import org.apache.nifi.web.security.NiFiAuthenticationRequestToken;

/**
 * This is an authentication request with a given JWT token.
 */
public class KnoxAuthenticationRequestToken extends NiFiAuthenticationRequestToken {

    private final String token;

    /**
     * Creates a representation of the jwt authentication request for a user.
     *
     * @param token   The unique token for this user
     * @param clientAddress the address of the client making the request
     */
    public KnoxAuthenticationRequestToken(final String token, final String clientAddress) {
        super(clientAddress);
        setAuthenticated(false);
        this.token = token;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return token;
    }

    public String getToken() {
        return token;
    }

    @Override
    public String toString() {
        return "<Knox JWT token>";
    }

}
