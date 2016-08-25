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
package org.apache.nifi.kerberos;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.LoginIdentityProviderInitializationContext;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.authentication.exception.ProviderDestructionException;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.kerberos.authentication.KerberosAuthenticationProvider;
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosClient;

import java.util.concurrent.TimeUnit;

/**
 * Kerberos-based implementation of a login identity provider.
 */
public class KerberosProvider implements LoginIdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(KerberosProvider.class);

    private KerberosAuthenticationProvider provider;
    private String issuer;
    private long expiration;

    @Override
    public final void initialize(final LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
        this.issuer = getClass().getSimpleName();
    }

    @Override
    public final void onConfigured(final LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final String rawExpiration = configurationContext.getProperty("Authentication Expiration");
        if (StringUtils.isBlank(rawExpiration)) {
            throw new ProviderCreationException("The Authentication Expiration must be specified.");
        }

        try {
            expiration = FormatUtils.getTimeDuration(rawExpiration, TimeUnit.MILLISECONDS);
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("The Expiration Duration '%s' is not a valid time duration", rawExpiration));
        }

        provider = new KerberosAuthenticationProvider();
        SunJaasKerberosClient client = new SunJaasKerberosClient();
        client.setDebug(true);
        provider.setKerberosClient(client);
        provider.setUserDetailsService(new KerberosUserDetailsService());
    }

    @Override
    public final AuthenticationResponse authenticate(final LoginCredentials credentials) throws InvalidLoginCredentialsException, IdentityAccessException {
        if (provider == null) {
            throw new IdentityAccessException("The Kerberos authentication provider is not initialized.");
        }

        try {
            // Perform the authentication
            final UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(credentials.getUsername(), credentials.getPassword());
            logger.debug("Created authentication token for principal {} with name {} and is authenticated {}", token.getPrincipal(), token.getName(), token.isAuthenticated());

            final Authentication authentication = provider.authenticate(token);
            logger.debug("Ran provider.authenticate() and returned authentication for " +
                    "principal {} with name {} and is authenticated {}", authentication.getPrincipal(), authentication.getName(), authentication.isAuthenticated());

            return new AuthenticationResponse(authentication.getName(), credentials.getUsername(), expiration, issuer);
        } catch (final AuthenticationException e) {
            throw new InvalidLoginCredentialsException(e.getMessage(), e);
        }
    }

    @Override
    public final void preDestruction() throws ProviderDestructionException {
    }

}
