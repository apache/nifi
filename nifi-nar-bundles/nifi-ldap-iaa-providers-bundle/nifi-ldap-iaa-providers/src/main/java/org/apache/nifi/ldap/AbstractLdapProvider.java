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
package org.apache.nifi.ldap;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.LoginIdentityProviderInitializationContext;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.ProviderDestructionException;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ldap.CommunicationException;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.ldap.authentication.AbstractLdapAuthenticationProvider;
import org.springframework.security.ldap.userdetails.LdapUserDetails;

/**
 * Abstract LDAP based implementation of a login identity provider.
 */
public abstract class AbstractLdapProvider implements LoginIdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(AbstractLdapProvider.class);

    private AbstractLdapAuthenticationProvider provider;
    private long expiration;

    @Override
    public final void initialize(final LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public final void onConfigured(final LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final String rawExpiration = configurationContext.getProperty("Expiration Duration");
        if (StringUtils.isBlank(rawExpiration)) {
            throw new ProviderCreationException("The Expiration Duration must be specified.");
        }

        try {
            expiration = FormatUtils.getTimeDuration(rawExpiration, TimeUnit.MILLISECONDS);
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("The Expiration Duration '%s' is not a valid time duration", rawExpiration));
        }

        provider = getLdapAuthenticationProvider(configurationContext);
    }

    protected abstract AbstractLdapAuthenticationProvider getLdapAuthenticationProvider(LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException;

    @Override
    public final AuthenticationResponse authenticate(final LoginCredentials credentials) throws InvalidLoginCredentialsException, IdentityAccessException {
        if (provider == null) {
            throw new IdentityAccessException("The LDAP authentication provider is not initialized.");
        }

        try {
            // perform the authentication
            final UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(credentials.getUsername(), credentials.getPassword());
            final Authentication authentication = provider.authenticate(token);

            // attempt to get the ldap user details to get the DN
            if (authentication.getPrincipal() instanceof LdapUserDetails) {
                final LdapUserDetails userDetails = (LdapUserDetails) authentication.getPrincipal();
                return new AuthenticationResponse(userDetails.getDn(), credentials.getUsername(), expiration);
            } else {
                return new AuthenticationResponse(authentication.getName(), credentials.getUsername(), expiration);
            }
        } catch (final CommunicationException | AuthenticationServiceException e) {
            logger.error(e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug(StringUtils.EMPTY, e);
            }
            throw new IdentityAccessException("Unable to query the configured directory server. See the logs for additional details.", e);
        } catch (final BadCredentialsException bce) {
            throw new InvalidLoginCredentialsException(bce.getMessage(), bce);
        }
    }

    @Override
    public final void preDestruction() throws ProviderDestructionException {
    }

}
