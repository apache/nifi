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
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.SslContextFactory.ClientAuth;
import org.springframework.ldap.core.support.AbstractTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.DefaultTlsDirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.DigestMd5DirContextAuthenticationStrategy;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.core.support.SimpleDirContextAuthenticationStrategy;
import org.springframework.security.ldap.authentication.AbstractLdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch;
import org.springframework.security.ldap.search.LdapUserSearch;

/**
 * LDAP based implementation of a login identity provider.
 */
public class LdapProvider extends AbstractLdapProvider {

    private static final String TLS = "TLS";
    
    @Override
    protected AbstractLdapAuthenticationProvider getLdapAuthenticationProvider(LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        final LdapContextSource context = new LdapContextSource();

        final String rawAuthenticationStrategy = configurationContext.getProperty("Authentication Strategy");
        final LdapAuthenticationStrategy authenticationStrategy;
        try {
            authenticationStrategy = LdapAuthenticationStrategy.valueOf(rawAuthenticationStrategy);
        } catch (final IllegalArgumentException iae) {
            throw new ProviderCreationException(String.format("Unrecgonized authentication strategy '%s'", rawAuthenticationStrategy));
        }

        switch (authenticationStrategy) {
            case ANONYMOUS:
                context.setAnonymousReadOnly(true);
                break;
            default:
                final String userDn = configurationContext.getProperty("Bind DN");
                final String password = configurationContext.getProperty("Bind Password");

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
                                        final ClientAuth clientAuth = ClientAuth.valueOf(rawClientAuth);
                                        sslContext = SslContextFactory.createSslContext(rawKeystore, rawKeystorePassword.toCharArray(), rawKeystoreType,
                                                rawTruststore, rawTruststorePassword.toCharArray(), rawTruststoreType, clientAuth, TLS);
                                    } catch (final IllegalArgumentException iae) {
                                        throw new ProviderCreationException(String.format("Unrecgonized client auth '%s'", rawClientAuth));
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

        final String url = configurationContext.getProperty("Url");

        if (StringUtils.isBlank(url)) {
            throw new ProviderCreationException("LDAP identity provider 'Url' must be specified.");
        }

        // connection
        context.setUrl(url);

        final String userSearchBase = configurationContext.getProperty("User Search Base");
        final String userSearchFilter = configurationContext.getProperty("User Search Filter");

        if (StringUtils.isBlank(userSearchBase) || StringUtils.isBlank(userSearchFilter)) {
            throw new ProviderCreationException("LDAP identity provider 'User Search Base' and 'User Search Filter' must be specified.");
        }

        // query
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
        return new LdapAuthenticationProvider(authenticator);
    }
}
