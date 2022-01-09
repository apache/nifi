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
package org.apache.nifi.registry.web.security.authentication;

import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.security.Principal;
import java.util.Collection;

/**
 * Wraps an AuthenticationRequest in a Token that implements the Spring Security Authentication interface.
 */
public class AuthenticationRequestToken implements Authentication {

    private final AuthenticationRequest authenticationRequest;
    private final Class<?> authenticationRequestOrigin;
    private final String clientAddress;

    public AuthenticationRequestToken(AuthenticationRequest authenticationRequest, Class<?> authenticationRequestOrigin, String clientAddress) {
        this.authenticationRequest = authenticationRequest;
        this.authenticationRequestOrigin = authenticationRequestOrigin;
        this.clientAddress = clientAddress;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return null;
    }

    @Override
    public Object getCredentials() {
        return authenticationRequest.getCredentials();
    }

    @Override
    public Object getDetails() {
        return authenticationRequest.getDetails();
    }

    @Override
    public Object getPrincipal() {
        return new Principal() {
            @Override
            public String getName() {
                return authenticationRequest.getUsername();
            }
        };
    }

    @Override
    public boolean isAuthenticated() {
        return false;
    }

    @Override
    public void setAuthenticated(boolean b) throws IllegalArgumentException {
        throw new IllegalArgumentException("AuthenticationRequestWrapper cannot be trusted. It is only to be used for storing an identity claim.");
    }

    @Override
    public String getName() {
        return authenticationRequest.getUsername();
    }

    @Override
    public int hashCode() {
        return authenticationRequest.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return authenticationRequest.equals(obj);
    }

    @Override
    public String toString() {
        return authenticationRequest.toString();
    }

    public AuthenticationRequest getAuthenticationRequest() {
        return authenticationRequest;
    }

    public Class<?> getAuthenticationRequestOrigin() {
        return authenticationRequestOrigin;
    }

    public String getClientAddress() {
        return clientAddress;
    }
}
