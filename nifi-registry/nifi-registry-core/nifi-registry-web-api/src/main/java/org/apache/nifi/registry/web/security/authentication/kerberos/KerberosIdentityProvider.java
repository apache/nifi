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
package org.apache.nifi.registry.web.security.authentication.kerberos;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationResponse;
import org.apache.nifi.registry.security.authentication.BasicAuthIdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProviderConfigurationContext;
import org.apache.nifi.registry.security.authentication.exception.IdentityAccessException;
import org.apache.nifi.registry.security.authentication.exception.InvalidCredentialsException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.security.util.kerberos.KerberosPrincipalParser;
import org.apache.nifi.registry.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.kerberos.authentication.KerberosAuthenticationProvider;
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosClient;

import java.util.concurrent.TimeUnit;

public class KerberosIdentityProvider extends BasicAuthIdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(KerberosIdentityProvider.class);
    private static final String issuer = KerberosIdentityProvider.class.getSimpleName();
    private static final String default_expiration = "12 hours";

    private KerberosAuthenticationProvider provider;

    private String defaultRealm;
    private long expiration;

    @Override
    public void onConfigured(IdentityProviderConfigurationContext configurationContext) throws SecurityProviderCreationException {

        String rawDebug = configurationContext.getProperty("Enable Debug");
        boolean enableDebug = (rawDebug != null && rawDebug.equalsIgnoreCase("true"));

        String rawExpiration = configurationContext.getProperty("Authentication Expiration");
        if (StringUtils.isBlank(rawExpiration)) {
            rawExpiration = default_expiration;
            logger.info("No Authentication Expiration specified, defaulting to " + default_expiration);
        }

        try {
            expiration = FormatUtils.getTimeDuration(rawExpiration, TimeUnit.MILLISECONDS);
        } catch (final IllegalArgumentException iae) {
            throw new SecurityProviderCreationException(
                    String.format("The Expiration Duration '%s' is not a valid time duration", rawExpiration));
        }

        defaultRealm = configurationContext.getProperty("Default Realm");
        if (StringUtils.isNotBlank(defaultRealm) && defaultRealm.contains("@")) {
            throw new SecurityProviderCreationException(String.format("The Default Realm '%s' must not contain \"@\"", defaultRealm));
        }

        provider = new KerberosAuthenticationProvider();
        SunJaasKerberosClient client = new SunJaasKerberosClient();
        client.setDebug(enableDebug);
        provider.setKerberosClient(client);
        provider.setUserDetailsService(new KerberosUserDetailsService());

    }

    @Override
    public AuthenticationResponse authenticate(AuthenticationRequest authenticationRequest) throws InvalidCredentialsException, IdentityAccessException {

        if (provider == null) {
            throw new IdentityAccessException("The Kerberos authentication provider is not initialized.");
        }


        try {
            final String rawPrincipal = authenticationRequest.getUsername();
            final Object credentials = authenticationRequest.getCredentials();
            final String parsedRealm = KerberosPrincipalParser.getRealm(rawPrincipal);

            // Apply default realm from KerberosIdentityProvider's configuration specified in identity-providers.xml if a principal without a realm was given
            // Otherwise, the default realm configured from the krb5 configuration specified in the nifi.registry.kerberos.krb5.file property will end up being used
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

            // perform the authentication
            final UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(identity, credentials);

            if (logger.isDebugEnabled()) {
                logger.debug("Created authentication token " + token.toString());
            }

            final Authentication authentication = provider.authenticate(token);
            if (logger.isDebugEnabled()) {
                logger.debug("Ran provider.authenticate(token) and returned authentication for " +
                                "principal={} with name={} and isAuthenticated={}",
                        authentication.getPrincipal(),
                        authentication.getName(),
                        authentication.isAuthenticated());
            }

            return new AuthenticationResponse(authentication.getName(), identity, expiration, issuer);
        } catch (final AuthenticationException e) {
            throw new InvalidCredentialsException(e.getMessage(), e);
        }

    }

    @Override
    public void preDestruction() throws SecurityProviderDestructionException {

    }
}
