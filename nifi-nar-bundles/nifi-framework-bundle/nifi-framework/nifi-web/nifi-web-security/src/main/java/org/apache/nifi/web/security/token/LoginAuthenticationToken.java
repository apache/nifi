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

import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.security.util.CertificateUtils;
import org.springframework.security.authentication.AbstractAuthenticationToken;

/**
 * This is an Authentication Token for logging in. Once a user is authenticated, they can be issues an ID token.
 */
public class LoginAuthenticationToken extends AbstractAuthenticationToken {

    final LoginCredentials credentials;

    public LoginAuthenticationToken(final LoginCredentials credentials) {
        super(null);
        setAuthenticated(true);
        this.credentials = credentials;
    }

    public LoginCredentials getLoginCredentials() {
        return credentials;
    }

    @Override
    public Object getCredentials() {
        return credentials.getPassword();
    }

    @Override
    public Object getPrincipal() {
        return credentials.getUsername();
    }

    @Override
    public String getName() {
        // if the username is a DN this will extract the username or CN... if not will return what was passed
        return CertificateUtils.extractUsername(credentials.getUsername());
    }

}
