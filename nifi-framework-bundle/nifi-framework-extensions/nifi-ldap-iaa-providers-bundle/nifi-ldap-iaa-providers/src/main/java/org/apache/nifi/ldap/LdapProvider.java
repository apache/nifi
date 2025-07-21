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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;
import javax.net.ssl.SSLContext;
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
import org.apache.nifi.configuration.NonComponentConfigurationContext;
import org.apache.nifi.ldap.ssl.LdapSslContextProvider;
import org.apache.nifi.ldap.ssl.StandardLdapSslContextProvider;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ldap.AuthenticationException;
import org.springframework.ldap.core.support.AbstractTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.DefaultTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.core.support.SimpleDirContextAuthenticationStrategy;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.ldap.authentication.AbstractLdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch;
import org.springframework.security.ldap.search.LdapUserSearch;
import org.springframework.security.ldap.userdetails.LdapUserDetails;

/**
 * Abstract LDAP based implementation of a login identity provider.
 */
public class LdapProvider implements LoginIdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(LdapProvider.class);

    private AbstractLdapAuthenticationProvider provider;
    private String issuer;
    private long expiration;
    private IdentityStrategy identityStrategy;

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

        final LdapContextSource context = new LdapContextSource();

        final Map<String, Object> baseEnvironment = new HashMap<>();

        // connect/read time out
        setTimeout(configurationContext, baseEnvironment, "Connect Timeout", "com.sun.jndi.ldap.connect.timeout");
        setTimeout(configurationContext, baseEnvironment, "Read Timeout", "com.sun.jndi.ldap.read.timeout");

        // authentication strategy
        final String rawAuthenticationStrategy = configurationContext.getProperty("Authentication Strategy");
        final LdapAuthenticationStrategy authenticationStrategy;
        try {
            authenticationStrategy = LdapAuthenticationStrategy.valueOf(rawAuthenticationStrategy);
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("Unrecognized authentication strategy '%s'. Possible values are [%s]",
                    rawAuthenticationStrategy, StringUtils.join(LdapAuthenticationStrategy.values(), ", ")));
        }

        if (authenticationStrategy == LdapAuthenticationStrategy.ANONYMOUS) {
            context.setAnonymousReadOnly(true);
        } else {
            final String userDn = configurationContext.getProperty("Manager DN");
            final String password = configurationContext.getProperty("Manager Password");

            context.setUserDn(userDn);
            context.setPassword(password);

            switch (authenticationStrategy) {
                case SIMPLE:
                    context.setAuthenticationStrategy(new SimpleDirContextAuthenticationStrategy());
                    break;
                case LDAPS:
                    context.setAuthenticationStrategy(new SimpleDirContextAuthenticationStrategy());

                    // indicate a secure connection
                    baseEnvironment.put(Context.SECURITY_PROTOCOL, "ssl");

                    // get the configured ssl context
                    final SSLContext ldapsSslContext = getConfiguredSslContext(configurationContext);
                    if (ldapsSslContext != null) {
                        // initialize the ldaps socket factory prior to use
                        LdapsSocketFactory.initialize(ldapsSslContext.getSocketFactory());
                        baseEnvironment.put("java.naming.ldap.factory.socket", LdapsSocketFactory.class.getName());
                    }
                    break;
                case START_TLS:
                    final AbstractTlsDirContextAuthenticationStrategy tlsAuthenticationStrategy = new DefaultTlsDirContextAuthenticationStrategy();

                    // shutdown gracefully
                    final String rawShutdownGracefully = configurationContext.getProperty("TLS - Shutdown Gracefully");
                    if (StringUtils.isNotBlank(rawShutdownGracefully)) {
                        final boolean shutdownGracefully = Boolean.TRUE.toString().equalsIgnoreCase(rawShutdownGracefully);
                        tlsAuthenticationStrategy.setShutdownTlsGracefully(shutdownGracefully);
                    }

                    // get the configured ssl context
                    final SSLContext startTlsSslContext = getConfiguredSslContext(configurationContext);
                    if (startTlsSslContext != null) {
                        tlsAuthenticationStrategy.setSslSocketFactory(startTlsSslContext.getSocketFactory());
                    }

                    // set the authentication strategy
                    context.setAuthenticationStrategy(tlsAuthenticationStrategy);
                    break;
            }
        }

        // referrals
        final String rawReferralStrategy = configurationContext.getProperty("Referral Strategy");

        final ReferralStrategy referralStrategy;
        try {
            referralStrategy = ReferralStrategy.valueOf(rawReferralStrategy);
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("Unrecognized referral strategy '%s'. Possible values are [%s]",
                    rawReferralStrategy, StringUtils.join(ReferralStrategy.values(), ", ")));
        }

        // using the value as this needs to be the lowercase version while the value is configured with the enum constant
        context.setReferral(referralStrategy.getValue());

        // url
        final String urls = configurationContext.getProperty("Url");

        if (StringUtils.isBlank(urls)) {
            throw new ProviderCreationException("LDAP identity provider 'Url' must be specified.");
        }

        // connection
        context.setUrls(StringUtils.split(urls));

        // search criteria
        final String userSearchBase = configurationContext.getProperty("User Search Base");
        final String userSearchFilter = configurationContext.getProperty("User Search Filter");

        if (StringUtils.isBlank(userSearchBase) || StringUtils.isBlank(userSearchFilter)) {
            throw new ProviderCreationException("LDAP identity provider 'User Search Base' and 'User Search Filter' must be specified.");
        }

        final LdapUserSearch userSearch = new FilterBasedLdapUserSearch(userSearchBase, userSearchFilter, context);

        // bind
        final BindAuthenticator authenticator = new BindAuthenticator(context);
        authenticator.setUserSearch(userSearch);

        // identity strategy
        final String rawIdentityStrategy = configurationContext.getProperty("Identity Strategy");

        if (StringUtils.isBlank(rawIdentityStrategy)) {
            logger.info("Identity Strategy is not configured, defaulting strategy to {}.", IdentityStrategy.USE_DN);

            // if this value is not configured, default to use dn which was the previous implementation
            identityStrategy = IdentityStrategy.USE_DN;
        } else {
            try {
                // attempt to get the configured identity strategy
                identityStrategy = IdentityStrategy.valueOf(rawIdentityStrategy);
            } catch (final IllegalArgumentException iae) {
                throw new ProviderCreationException(String.format("Unrecognized identity strategy '%s'. Possible values are [%s]",
                        rawIdentityStrategy, StringUtils.join(IdentityStrategy.values(), ", ")));
            }
        }

        // set the base environment is necessary
        if (!baseEnvironment.isEmpty()) {
            context.setBaseEnvironmentProperties(baseEnvironment);
        }

        try {
            // handling initializing beans
            context.afterPropertiesSet();
            authenticator.afterPropertiesSet();
        } catch (final Exception e) {
            throw new ProviderCreationException(e.getMessage(), e);
        }

        // create the underlying provider
        provider = new LdapAuthenticationProvider(authenticator);
    }

    private void setTimeout(final LoginIdentityProviderConfigurationContext configurationContext,
            final Map<String, Object> baseEnvironment,
            final String configurationProperty,
            final String environmentKey) {

        final String rawTimeout = configurationContext.getProperty(configurationProperty);
        if (StringUtils.isNotBlank(rawTimeout)) {
            try {
                final long timeout = (long) FormatUtils.getPreciseTimeDuration(rawTimeout, TimeUnit.MILLISECONDS);
                baseEnvironment.put(environmentKey, String.valueOf(timeout));
            } catch (final IllegalArgumentException iae) {
                throw new ProviderCreationException(String.format("The %s '%s' is not a valid time duration", configurationProperty, rawTimeout));
            }
        }
    }

    private static SSLContext getConfiguredSslContext(final NonComponentConfigurationContext configurationContext) {
        final LdapSslContextProvider ldapSslContextProvider = new StandardLdapSslContextProvider();
        final Map<String, String> contextProperties = configurationContext.getProperties();
        return ldapSslContextProvider.createContext(contextProperties);
    }

    @Override
    public final AuthenticationResponse authenticate(final LoginCredentials credentials) throws InvalidLoginCredentialsException, IdentityAccessException {
        if (provider == null) {
            throw new IdentityAccessException("The LDAP authentication provider is not initialized.");
        }

        try {
            // perform the authentication
            final UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(credentials.getUsername(), credentials.getPassword());
            final Authentication authentication = provider.authenticate(token);

            // use dn if configured
            if (IdentityStrategy.USE_DN.equals(identityStrategy)) {
                // attempt to get the ldap user details to get the DN
                if (authentication.getPrincipal() instanceof LdapUserDetails userDetails) {
                    return new AuthenticationResponse(userDetails.getDn(), credentials.getUsername(), expiration, issuer);
                } else {
                    logger.warn("Unable to determine user DN for {}, using username.", authentication.getName());
                    return new AuthenticationResponse(authentication.getName(), credentials.getUsername(), expiration, issuer);
                }
            } else {
                return new AuthenticationResponse(authentication.getName(), credentials.getUsername(), expiration, issuer);
            }
        } catch (final BadCredentialsException | UsernameNotFoundException | AuthenticationException e) {
            throw new InvalidLoginCredentialsException(e.getMessage(), e);
        } catch (final Exception e) {
            // there appears to be a bug that generates a InternalAuthenticationServiceException wrapped around an AuthenticationException. this
            // shouldn't be the case as they the service exception suggestions that something was wrong with the service. while the authentication
            // exception suggests that username and/or credentials were incorrect. checking the cause seems to address this scenario.
            final Throwable cause = e.getCause();
            if (cause instanceof AuthenticationException) {
                throw new InvalidLoginCredentialsException(e.getMessage(), e);
            }

            logger.error(e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug(StringUtils.EMPTY, e);
            }
            throw new IdentityAccessException("Unable to validate the supplied credentials. Please contact the system administrator.", e);
        }
    }

    @Override
    public final void preDestruction() throws ProviderDestructionException {
    }

}