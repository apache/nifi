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
import org.apache.nifi.kerberos.parser.KerberosPrincipalParser;
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
    private String defaultRealm;
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
            expiration = Double.valueOf(FormatUtils.getPreciseTimeDuration(rawExpiration, TimeUnit.MILLISECONDS)).longValue();
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("The Expiration Duration '%s' is not a valid time duration", rawExpiration));
        }

        defaultRealm = configurationContext.getProperty("Default Realm");
        if (StringUtils.isNotBlank(defaultRealm) && defaultRealm.contains("@")) {
            throw new ProviderCreationException(String.format("The Default Realm '%s' must not contain \"@\"", defaultRealm));
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
            final String rawPrincipal = credentials.getUsername();
            final String parsedRealm = KerberosPrincipalParser.getRealm(rawPrincipal);

            // Apply default realm from KerberosIdentityProvider's configuration specified in login-identity-providers.xml if a principal without a realm was given
            // Otherwise, the default realm configured from the krb5 configuration specified in the nifi.kerberos.krb5.file property will end up being used
            boolean realmInRawPrincipal = StringUtils.isNotBlank(parsedRealm);
            final String identity;
            if (realmInRawPrincipal) {
                // there's a realm already in the given principal, use it
                identity = rawPrincipal;
                logger.debug("Realm was specified in principal {}, default realm was not added to the identity being authenticated", rawPrincipal);
            } else if (StringUtils.isNotBlank(defaultRealm)) {
                // the value for the default realm is not blank, append the realm to the given principal
                identity = StringUtils.joinWith("@", rawPrincipal, defaultRealm);
                logger.debug("Realm was not specified in principal {}, default realm {} was added to the identity being authenticated", rawPrincipal, defaultRealm);
            } else {
                // otherwise, use the given principal, which will use the default realm as specified in the krb5 configuration
                identity = rawPrincipal;
                logger.debug("Realm was not specified in principal {}, default realm is blank and was not added to the identity being authenticated", rawPrincipal);
            }

            // Perform the authentication
            final UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(identity, credentials.getPassword());
            if (logger.isDebugEnabled()) {
                logger.debug("Created authentication token for principal {} with name {} and is authenticated {}", token.getPrincipal(), token.getName(), token.isAuthenticated());
            }

            final Authentication authentication = provider.authenticate(token);
            if (logger.isDebugEnabled()) {
                logger.debug("Ran provider.authenticate() and returned authentication for " +
                        "principal {} with name {} and is authenticated {}", authentication.getPrincipal(), authentication.getName(), authentication.isAuthenticated());
            }

            return new AuthenticationResponse(authentication.getName(), identity, expiration, issuer);
        } catch (final AuthenticationException e) {
            throw new InvalidLoginCredentialsException(e.getMessage(), e);
        }
    }

    @Override
    public final void preDestruction() throws ProviderDestructionException {
    }

}
