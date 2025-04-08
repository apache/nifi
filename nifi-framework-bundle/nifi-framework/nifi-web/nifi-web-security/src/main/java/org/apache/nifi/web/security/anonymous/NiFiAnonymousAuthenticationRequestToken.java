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
package org.apache.nifi.web.security.anonymous;

import org.apache.nifi.web.security.NiFiAuthenticationRequestToken;

import static org.apache.nifi.authorization.user.StandardNiFiUser.ANONYMOUS_IDENTITY;

/**
 * This is an authentication request for an anonymous user.
 */
public class NiFiAnonymousAuthenticationRequestToken extends NiFiAuthenticationRequestToken {

    final boolean secureRequest;

    /**
     * Creates a representation of the anonymous authentication request for a user.
     *
     * @param clientAddress the address of the client making the request
     * @param authenticationDetails the authentication details of teh client making the request
     */
    public NiFiAnonymousAuthenticationRequestToken(final boolean secureRequest, final String clientAddress, final Object authenticationDetails) {
        super(clientAddress, authenticationDetails);
        setAuthenticated(false);
        this.secureRequest = secureRequest;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    public boolean isSecureRequest() {
        return secureRequest;
    }

    @Override
    public Object getPrincipal() {
        return ANONYMOUS_IDENTITY;
    }

    @Override
    public String toString() {
        return "<anonymous>";
    }

}
