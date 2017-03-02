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
package org.apache.nifi.integration.util;

import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.LoginIdentityProviderInitializationContext;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authentication.exception.ProviderCreationException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class NiFiTestLoginIdentityProvider implements LoginIdentityProvider {

    private final Map<String, String> users;

    /**
     * Creates a new FileAuthorizationProvider.
     */
    public NiFiTestLoginIdentityProvider() {
        users = new HashMap<>();
        users.put("user@nifi", "whatever");
        users.put("unregistered-user@nifi", "password");
    }

    private void checkUser(final String user, final String password) {
        if (!users.containsKey(user)) {
            throw new InvalidLoginCredentialsException("Unknown user");
        }

        if (!users.get(user).equals(password)) {
            throw new InvalidLoginCredentialsException("Invalid password");
        }
    }

    @Override
    public AuthenticationResponse authenticate(LoginCredentials credentials) throws InvalidLoginCredentialsException, IdentityAccessException {
        checkUser(credentials.getUsername(), credentials.getPassword());
        return new AuthenticationResponse(credentials.getUsername(), credentials.getUsername(), TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS), getClass().getSimpleName());
    }

    @Override
    public void initialize(LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public void onConfigured(LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
    }

    @Override
    public void preDestruction() {
    }

}
