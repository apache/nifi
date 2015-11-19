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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
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
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ldap.CommunicationException;
import org.springframework.ldap.core.support.AbstractTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.DefaultTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.DigestMd5DirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.core.support.SimpleDirContextAuthenticationStrategy;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
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
    private static final String TLS = "TLS";

    private AbstractLdapAuthenticationProvider provider;
    private String issuer;
    private long expiration;

    @Override
    public final void initialize(final LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
        this.issuer = getClass().getSimpleName();
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

        final LdapContextSource context = new LdapContextSource();

        final Map<String, Object> baseEnvironment = new HashMap<>();

        // connect/read time out
        setTimeout(configurationContext, baseEnvironment, "Connect Timeout", "com.sun.jndi.ldap.connect.timeout");
        setTimeout(configurationContext, baseEnvironment, "Read Timeout", "com.sun.jndi.ldap.read.timeout");

        // set the base environment is necessary
        if (!baseEnvironment.isEmpty()) {
            context.setBaseEnvironmentProperties(baseEnvironment);
        }

        // authentication strategy
        final String rawAuthenticationStrategy = configurationContext.getProperty("Authentication Strategy");
        final LdapAuthenticationStrategy authenticationStrategy;
        try {
            authenticationStrategy = LdapAuthenticationStrategy.valueOf(rawAuthenticationStrategy);
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("Unrecognized authentication strategy '%s'. Possible values are [%s]",
                    rawAuthenticationStrategy, StringUtils.join(LdapAuthenticationStrategy.values(), ", ")));
        }

        switch (authenticationStrategy) {
            case ANONYMOUS:
                context.setAnonymousReadOnly(true);
                break;
            default:
                final String userDn = configurationContext.getProperty("Manager DN");
                final String password = configurationContext.getProperty("Manager Password");

                context.setUserDn(userDn);
                context.setPassword(password);

                switch (authenticationStrategy) {
                    case SIMPLE:
                        context.setAuthenticationStrategy(new SimpleDirContextAuthenticationStrategy());
                        break;
                    case DIGEST_MD5:
                        context.setAuthenticationStrategy(new DigestMd5DirContextAuthenticationStrategy());
                        break;
                    case TLS:
                        final AbstractTlsDirContextAuthenticationStrategy tlsAuthenticationStrategy = new DefaultTlsDirContextAuthenticationStrategy();

                        // shutdown gracefully
                        final String rawShutdownGracefully = configurationContext.getProperty("TLS - Shutdown Gracefully");
                        if (StringUtils.isNotBlank(rawShutdownGracefully)) {
                            final boolean shutdownGracefully = Boolean.TRUE.toString().equalsIgnoreCase(rawShutdownGracefully);
                            tlsAuthenticationStrategy.setShutdownTlsGracefully(shutdownGracefully);
                        }

                        final String rawKeystore = configurationContext.getProperty("TLS - Keystore");
                        final String rawKeystorePassword = configurationContext.getProperty("TLS - Keystore Password");
                        final String rawKeystoreType = configurationContext.getProperty("TLS - Keystore Type");
                        final String rawTruststore = configurationContext.getProperty("TLS - Truststore");
                        final String rawTruststorePassword = configurationContext.getProperty("TLS - Truststore Password");
                        final String rawTruststoreType = configurationContext.getProperty("TLS - Truststore Type");
                        final String rawClientAuth = configurationContext.getProperty("TLS - Client Auth");

                        try {
                            final SSLContext sslContext;
                            if (StringUtils.isBlank(rawKeystore)) {
                                sslContext = SslContextFactory.createTrustSslContext(rawTruststore, rawTruststorePassword.toCharArray(), rawTruststoreType, TLS);
                            } else {
                                if (StringUtils.isBlank(rawTruststore)) {
                                    sslContext = SslContextFactory.createSslContext(rawKeystore, rawKeystorePassword.toCharArray(), rawKeystoreType, TLS);
                                } else {
                                    try {
                                        final SslContextFactory.ClientAuth clientAuth = SslContextFactory.ClientAuth.valueOf(rawClientAuth);
                                        sslContext = SslContextFactory.createSslContext(rawKeystore, rawKeystorePassword.toCharArray(), rawKeystoreType,
                                                rawTruststore, rawTruststorePassword.toCharArray(), rawTruststoreType, clientAuth, TLS);
                                    } catch (final IllegalArgumentException iae) {
                                        throw new ProviderCreationException(String.format("Unrecognized client auth '%s'", rawClientAuth));
                                    }
                                }
                            }
                            tlsAuthenticationStrategy.setSslSocketFactory(sslContext.getSocketFactory());
                        } catch (final KeyStoreException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException | KeyManagementException | IOException e) {
                            throw new ProviderCreationException(e.getMessage(), e);
                        }

                        context.setAuthenticationStrategy(tlsAuthenticationStrategy);
                        break;
                }
                break;
        }

        // referrals
        final String rawReferralStrategy = configurationContext.getProperty("Referral Strategy");

        final ReferralStrategy referralStrategy;
        try {
            referralStrategy = ReferralStrategy.valueOf(rawReferralStrategy);
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("Unrecgonized authentication strategy '%s'. Possible values are [%s]",
                    rawAuthenticationStrategy, StringUtils.join(ReferralStrategy.values(), ", ")));
        }

        context.setReferral(referralStrategy.toString());

        // url
        final String url = configurationContext.getProperty("Url");

        if (StringUtils.isBlank(url)) {
            throw new ProviderCreationException("LDAP identity provider 'Url' must be specified.");
        }

        // connection
        context.setUrl(url);

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
                final Long timeout = FormatUtils.getTimeDuration(rawTimeout, TimeUnit.MILLISECONDS);
                baseEnvironment.put(environmentKey, timeout.toString());
            } catch (final IllegalArgumentException iae) {
                throw new ProviderCreationException(String.format("The %s '%s' is not a valid time duration", configurationProperty, rawTimeout));
            }
        }
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

            // attempt to get the ldap user details to get the DN
            if (authentication.getPrincipal() instanceof LdapUserDetails) {
                final LdapUserDetails userDetails = (LdapUserDetails) authentication.getPrincipal();
                return new AuthenticationResponse(userDetails.getDn(), credentials.getUsername(), expiration, issuer);
            } else {
                return new AuthenticationResponse(authentication.getName(), credentials.getUsername(), expiration, issuer);
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
