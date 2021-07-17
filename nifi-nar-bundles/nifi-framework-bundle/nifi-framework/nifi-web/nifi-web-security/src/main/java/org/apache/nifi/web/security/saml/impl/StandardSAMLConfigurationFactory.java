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
package org.apache.nifi.web.security.saml.impl;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.saml.NiFiSAMLContextProvider;
import org.apache.nifi.web.security.saml.SAMLConfiguration;
import org.apache.nifi.web.security.saml.SAMLConfigurationFactory;
import org.apache.nifi.web.security.saml.impl.tls.CompositeKeyManager;
import org.apache.nifi.web.security.saml.impl.tls.CustomTLSProtocolSocketFactory;
import org.apache.nifi.web.security.saml.impl.tls.TruststoreStrategy;
import org.apache.velocity.app.VelocityEngine;
import org.opensaml.Configuration;
import org.opensaml.saml2.metadata.provider.FilesystemMetadataProvider;
import org.opensaml.saml2.metadata.provider.HTTPMetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.parse.ParserPool;
import org.opensaml.xml.parse.StaticBasicParserPool;
import org.opensaml.xml.parse.XMLParserException;
import org.opensaml.xml.security.BasicSecurityConfiguration;
import org.opensaml.xml.security.SecurityHelper;
import org.opensaml.xml.security.credential.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml.SAMLBootstrap;
import org.springframework.security.saml.key.JKSKeyManager;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLDefaultLogger;
import org.springframework.security.saml.log.SAMLLogger;
import org.springframework.security.saml.metadata.CachingMetadataManager;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.processor.HTTPArtifactBinding;
import org.springframework.security.saml.processor.HTTPPAOS11Binding;
import org.springframework.security.saml.processor.HTTPPostBinding;
import org.springframework.security.saml.processor.HTTPRedirectDeflateBinding;
import org.springframework.security.saml.processor.HTTPSOAP11Binding;
import org.springframework.security.saml.processor.SAMLBinding;
import org.springframework.security.saml.processor.SAMLProcessor;
import org.springframework.security.saml.processor.SAMLProcessorImpl;
import org.springframework.security.saml.storage.EmptyStorageFactory;
import org.springframework.security.saml.util.VelocityFactory;
import org.springframework.security.saml.websso.ArtifactResolutionProfileImpl;
import org.springframework.security.saml.websso.SingleLogoutProfile;
import org.springframework.security.saml.websso.SingleLogoutProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileConsumerHoKImpl;
import org.springframework.security.saml.websso.WebSSOProfileConsumerImpl;
import org.springframework.security.saml.websso.WebSSOProfileECPImpl;
import org.springframework.security.saml.websso.WebSSOProfileHoKImpl;
import org.springframework.security.saml.websso.WebSSOProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfileOptions;

import javax.net.ssl.SSLSocketFactory;
import javax.servlet.ServletException;
import java.io.File;
import java.net.URI;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

public class StandardSAMLConfigurationFactory implements SAMLConfigurationFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardSAMLConfigurationFactory.class);

    public SAMLConfiguration create(final NiFiProperties properties) throws Exception {
        // ensure we only configure SAML when OIDC/KnoxSSO/LoginIdentityProvider are not enabled
        if (properties.isOidcEnabled() || properties.isKnoxSsoEnabled() || properties.isLoginIdentityProviderEnabled()) {
            throw new RuntimeException("SAML cannot be enabled if the Login Identity Provider or OpenId Connect or KnoxSSO is configured.");
        }

        LOGGER.info("Initializing SAML configuration...");

        // Load and validate config from nifi.properties...

        final String rawEntityId = properties.getSamlServiceProviderEntityId();
        if (StringUtils.isBlank(rawEntityId)) {
            throw new RuntimeException("Entity ID is required when configuring SAML");
        }

        final String spEntityId = rawEntityId;
        LOGGER.info("Service Provider Entity ID = '{}'", spEntityId);

        final String rawIdpMetadataUrl = properties.getSamlIdentityProviderMetadataUrl();

        if (StringUtils.isBlank(rawIdpMetadataUrl)) {
            throw new RuntimeException("IDP Metadata URL is required when configuring SAML");
        }

        if (!rawIdpMetadataUrl.startsWith("file://")
                && !rawIdpMetadataUrl.startsWith("http://")
                && !rawIdpMetadataUrl.startsWith("https://")) {
            throw new RuntimeException("IDP Medata URL must start with file://, http://, or https://");
        }

        final URI idpMetadataLocation = URI.create(rawIdpMetadataUrl);
        LOGGER.info("Identity Provider Metadata Location = '{}'", idpMetadataLocation);

        final String authExpirationFromProperties = properties.getSamlAuthenticationExpiration();
        LOGGER.info("Authentication Expiration = '{}'", authExpirationFromProperties);

        final long authExpiration;
        try {
            authExpiration = Math.round(FormatUtils.getPreciseTimeDuration(authExpirationFromProperties, TimeUnit.MILLISECONDS));
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid SAML authentication expiration: " + authExpirationFromProperties);
        }

        final String identityAttributeName = properties.getSamlIdentityAttributeName();
        if (!StringUtils.isBlank(identityAttributeName)) {
            LOGGER.info("Identity Attribute Name = '{}'", identityAttributeName);
        }

        final String groupAttributeName = properties.getSamlGroupAttributeName();
        if (!StringUtils.isBlank(groupAttributeName)) {
            LOGGER.info("Group Attribute Name = '{}'", groupAttributeName);
        }

        final TruststoreStrategy truststoreStrategy;
        try {
            truststoreStrategy = TruststoreStrategy.valueOf(properties.getSamlHttpClientTruststoreStrategy());
            LOGGER.info("HttpClient Truststore Strategy = `{}`", truststoreStrategy.name());
        } catch (Exception e) {
            throw new RuntimeException("Truststore Strategy must be one of " + TruststoreStrategy.NIFI.name() + " or " + TruststoreStrategy.JDK.name());
        }

        int connectTimeout;
        final String rawConnectTimeout = properties.getSamlHttpClientConnectTimeout();
        try {
            connectTimeout = (int) FormatUtils.getPreciseTimeDuration(rawConnectTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.warn("Failed to parse value of property '{}' as a valid time period. Value was '{}'. Ignoring this value and using the default value of '{}'",
                    NiFiProperties.SECURITY_USER_SAML_HTTP_CLIENT_CONNECT_TIMEOUT, rawConnectTimeout, NiFiProperties.DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_CONNECT_TIMEOUT);
            connectTimeout = (int) FormatUtils.getPreciseTimeDuration(NiFiProperties.DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        int readTimeout;
        final String rawReadTimeout = properties.getSamlHttpClientReadTimeout();
        try {
            readTimeout = (int) FormatUtils.getPreciseTimeDuration(rawReadTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.warn("Failed to parse value of property '{}' as a valid time period. Value was '{}'. Ignoring this value and using the default value of '{}'",
                    NiFiProperties.SECURITY_USER_SAML_HTTP_CLIENT_READ_TIMEOUT, rawReadTimeout, NiFiProperties.DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_READ_TIMEOUT);
            readTimeout = (int) FormatUtils.getPreciseTimeDuration(NiFiProperties.DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_READ_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        // Initialize spring-security-saml/OpenSAML objects...

        final SAMLBootstrap samlBootstrap = new SAMLBootstrap();
        samlBootstrap.postProcessBeanFactory(null);

        final ParserPool parserPool = createParserPool();
        final VelocityEngine velocityEngine = VelocityFactory.getEngine();

        final TlsConfiguration tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);
        final KeyManager keyManager = createKeyManager(tlsConfiguration);

        final HttpClient httpClient = createHttpClient(connectTimeout, readTimeout);
        if (truststoreStrategy == TruststoreStrategy.NIFI) {
            configureCustomTLSSocketFactory(tlsConfiguration);
        }

        final boolean signMetadata = properties.isSamlMetadataSigningEnabled();
        final String signatureAlgorithm = properties.getSamlSignatureAlgorithm();
        final String signatureDigestAlgorithm = properties.getSamlSignatureDigestAlgorithm();
        configureGlobalSecurityDefaults(keyManager, signatureAlgorithm, signatureDigestAlgorithm);

        final ExtendedMetadata extendedMetadata = createExtendedMetadata(signatureAlgorithm, signMetadata);

        final Timer backgroundTaskTimer = new Timer(true);
        final MetadataProvider idpMetadataProvider = createIdpMetadataProvider(idpMetadataLocation, httpClient, backgroundTaskTimer, parserPool);

        final MetadataManager metadataManager = createMetadataManager(idpMetadataProvider, extendedMetadata, keyManager);

        final SAMLProcessor processor = createSAMLProcessor(parserPool, velocityEngine, httpClient);
        final NiFiSAMLContextProvider contextProvider = createContextProvider(metadataManager, keyManager);

        // Build the configuration instance...

        return new StandardSAMLConfiguration.Builder()
                .spEntityId(spEntityId)
                .processor(processor)
                .contextProvider(contextProvider)
                .logger(createSAMLLogger(properties))
                .webSSOProfileOptions(createWebSSOProfileOptions())
                .webSSOProfile(createWebSSOProfile(metadataManager, processor))
                .webSSOProfileECP(createWebSSOProfileECP(metadataManager, processor))
                .webSSOProfileHoK(createWebSSOProfileHok(metadataManager, processor))
                .webSSOProfileConsumer(createWebSSOProfileConsumer(metadataManager, processor))
                .webSSOProfileHoKConsumer(createWebSSOProfileHokConsumer(metadataManager, processor))
                .singleLogoutProfile(createSingeLogoutProfile(metadataManager, processor))
                .metadataManager(metadataManager)
                .extendedMetadata(extendedMetadata)
                .backgroundTaskTimer(backgroundTaskTimer)
                .keyManager(keyManager)
                .authExpiration(authExpiration)
                .identityAttributeName(identityAttributeName)
                .groupAttributeName(groupAttributeName)
                .requestSigningEnabled(properties.isSamlRequestSigningEnabled())
                .wantAssertionsSigned(properties.isSamlWantAssertionsSigned())
                .build();
    }

    private static ParserPool createParserPool() throws XMLParserException {
        final StaticBasicParserPool parserPool = new StaticBasicParserPool();
        parserPool.initialize();
        return parserPool;
    }

    private static HttpClient createHttpClient(final int connectTimeout, final int readTimeout) {
        final HttpClientParams clientParams = new HttpClientParams();
        clientParams.setParameter(HttpConnectionParams.CONNECTION_TIMEOUT, connectTimeout);
        clientParams.setParameter(HttpConnectionParams.SO_TIMEOUT, readTimeout);

        final HttpClient httpClient = new HttpClient(clientParams);
        return httpClient;
    }

    private static void configureCustomTLSSocketFactory(final TlsConfiguration tlsConfiguration) throws TlsException {
        final SSLSocketFactory sslSocketFactory = SslContextFactory.createSSLSocketFactory(tlsConfiguration);
        final ProtocolSocketFactory socketFactory = new CustomTLSProtocolSocketFactory(sslSocketFactory);

        // Consider not using global registration of protocol here as it would potentially impact other uses of commons http client
        // with in nifi-framework-nar, currently there are no other usages, see https://hc.apache.org/httpclient-3.x/sslguide.html
        final Protocol p = new Protocol("https", socketFactory, 443);
        Protocol.registerProtocol(p.getScheme(), p);
    }

    private static SAMLProcessor createSAMLProcessor(final ParserPool parserPool, final VelocityEngine velocityEngine, final HttpClient httpClient) {
        final HTTPSOAP11Binding httpsoap11Binding = new HTTPSOAP11Binding(parserPool);
        final HTTPPAOS11Binding httppaos11Binding = new HTTPPAOS11Binding(parserPool);
        final HTTPPostBinding httpPostBinding = new HTTPPostBinding(parserPool, velocityEngine);
        final HTTPRedirectDeflateBinding httpRedirectDeflateBinding = new HTTPRedirectDeflateBinding(parserPool);

        final ArtifactResolutionProfileImpl artifactResolutionProfile = new ArtifactResolutionProfileImpl(httpClient);
        artifactResolutionProfile.setProcessor(new SAMLProcessorImpl(httpsoap11Binding));

        final HTTPArtifactBinding httpArtifactBinding = new HTTPArtifactBinding(
                parserPool, velocityEngine, artifactResolutionProfile);

        final Collection<SAMLBinding> bindings = new ArrayList<>();
        bindings.add(httpRedirectDeflateBinding);
        bindings.add(httpPostBinding);
        bindings.add(httpArtifactBinding);
        bindings.add(httpsoap11Binding);
        bindings.add(httppaos11Binding);

        return new SAMLProcessorImpl(bindings);
    }

    private static NiFiSAMLContextProvider createContextProvider(final MetadataManager metadataManager, final KeyManager keyManager) throws ServletException {
        final NiFiSAMLContextProviderImpl contextProvider = new NiFiSAMLContextProviderImpl();
        contextProvider.setMetadata(metadataManager);
        contextProvider.setKeyManager(keyManager);

        // Note - the default is HttpSessionStorageFactory, but since we don't use HttpSessions we can't rely on that,
        // setting this to the EmptyStorageFactory simply disables checking of the InResponseTo field, if we ever want
        // to bring that back we could possibly implement our own in-memory storage factory
        // https://docs.spring.io/spring-security-saml/docs/current/reference/html/chapter-troubleshooting.html#d5e1935
        contextProvider.setStorageFactory(new EmptyStorageFactory());

        contextProvider.afterPropertiesSet();
        return contextProvider;
    }

    private static WebSSOProfileOptions createWebSSOProfileOptions() {
        final WebSSOProfileOptions webSSOProfileOptions = new WebSSOProfileOptions();
        webSSOProfileOptions.setIncludeScoping(false);
        return webSSOProfileOptions;
    }

    private static WebSSOProfile createWebSSOProfile(final MetadataManager metadataManager, final SAMLProcessor processor) throws Exception {
        final WebSSOProfileImpl webSSOProfile = new WebSSOProfileImpl(processor, metadataManager);
        webSSOProfile.afterPropertiesSet();
        return webSSOProfile;
    }

    private static WebSSOProfile createWebSSOProfileECP(final MetadataManager metadataManager, final SAMLProcessor processor) throws Exception {
        final WebSSOProfileECPImpl webSSOProfileECP = new WebSSOProfileECPImpl();
        webSSOProfileECP.setProcessor(processor);
        webSSOProfileECP.setMetadata(metadataManager);
        webSSOProfileECP.afterPropertiesSet();
        return webSSOProfileECP;
    }

    private static WebSSOProfile createWebSSOProfileHok(final MetadataManager metadataManager, final SAMLProcessor processor) throws Exception {
        final WebSSOProfileHoKImpl webSSOProfileHok = new WebSSOProfileHoKImpl();
        webSSOProfileHok.setProcessor(processor);
        webSSOProfileHok.setMetadata(metadataManager);
        webSSOProfileHok.afterPropertiesSet();
        return webSSOProfileHok;
    }

    private static WebSSOProfileConsumer createWebSSOProfileConsumer(final MetadataManager metadataManager, final SAMLProcessor processor) throws Exception {
        final WebSSOProfileConsumerImpl webSSOProfileConsumer = new WebSSOProfileConsumerImpl();
        webSSOProfileConsumer.setProcessor(processor);
        webSSOProfileConsumer.setMetadata(metadataManager);
        webSSOProfileConsumer.afterPropertiesSet();
        return webSSOProfileConsumer;
    }

    private static WebSSOProfileConsumer createWebSSOProfileHokConsumer(final MetadataManager metadataManager, final SAMLProcessor processor) throws Exception {
        final WebSSOProfileConsumerHoKImpl webSSOProfileHoKConsumer = new WebSSOProfileConsumerHoKImpl();
        webSSOProfileHoKConsumer.setProcessor(processor);
        webSSOProfileHoKConsumer.setMetadata(metadataManager);
        webSSOProfileHoKConsumer.afterPropertiesSet();
        return webSSOProfileHoKConsumer;
    }

    private static SingleLogoutProfile createSingeLogoutProfile(final MetadataManager metadataManager, final SAMLProcessor processor) throws Exception {
        final SingleLogoutProfileImpl singleLogoutProfile = new SingleLogoutProfileImpl();
        singleLogoutProfile.setProcessor(processor);
        singleLogoutProfile.setMetadata(metadataManager);
        singleLogoutProfile.afterPropertiesSet();
        return singleLogoutProfile;
    }

    private static SAMLLogger createSAMLLogger(final NiFiProperties properties) {
        final SAMLDefaultLogger samlLogger = new SAMLDefaultLogger();
        if (properties.isSamlMessageLoggingEnabled()) {
            samlLogger.setLogAllMessages(true);
            samlLogger.setLogErrors(true);
            samlLogger.setLogMessagesOnException(true);
        } else {
            samlLogger.setLogAllMessages(false);
            samlLogger.setLogErrors(false);
            samlLogger.setLogMessagesOnException(false);
        }
        return samlLogger;
    }

    private static KeyManager createKeyManager(final TlsConfiguration tlsConfiguration) throws TlsException, KeyStoreException {
        final String keystorePath = tlsConfiguration.getKeystorePath();
        final char[] keystorePasswordChars = tlsConfiguration.getKeystorePassword().toCharArray();
        final String keystoreType = tlsConfiguration.getKeystoreType().getType();

        final String truststorePath = tlsConfiguration.getTruststorePath();
        final char[] truststorePasswordChars = tlsConfiguration.getTruststorePassword().toCharArray();
        final String truststoreType = tlsConfiguration.getTruststoreType().getType();

        final KeyStore keyStore = KeyStoreUtils.loadKeyStore(keystorePath, keystorePasswordChars, keystoreType);
        final KeyStore trustStore = KeyStoreUtils.loadTrustStore(truststorePath, truststorePasswordChars, truststoreType);

        final String keyAlias = getPrivateKeyAlias(keyStore, keystorePath);
        LOGGER.info("Default key alias = {}", keyAlias);

        // if no key password was provided, then assume the keystore password is the same as the key password.
        final String keyPassword = StringUtils.isBlank(tlsConfiguration.getKeyPassword()) ? tlsConfiguration.getKeystorePassword() : tlsConfiguration.getKeyPassword();

        final Map<String,String> keyPasswords = new HashMap<>();
        if (!StringUtils.isBlank(keyPassword)) {
            keyPasswords.put(keyAlias, keyPassword);
        }

        final KeyManager keystoreKeyManager = new JKSKeyManager(keyStore, keyPasswords, keyAlias);
        final KeyManager truststoreKeyManager = new JKSKeyManager(trustStore, Collections.emptyMap(), null);
        return new CompositeKeyManager(keystoreKeyManager, truststoreKeyManager);
    }

    private static String getPrivateKeyAlias(final KeyStore keyStore, final String keystorePath) throws KeyStoreException {
        final Set<String> keyAliases = getKeyAliases(keyStore);

        int privateKeyAliases = 0;
        for (final String keyAlias : keyAliases) {
            if (keyStore.isKeyEntry(keyAlias)) {
                privateKeyAliases++;
            }
        }

        if (privateKeyAliases == 0) {
            throw new RuntimeException("Unable to determine signing key, the keystore '" + keystorePath + "' does not contain any private keys");
        }

        if (privateKeyAliases > 1) {
            throw new RuntimeException("Unable to determine signing key, the keystore '" + keystorePath + "' contains more than one private key");
        }

        String firstPrivateKeyAlias = null;
        for (final String keyAlias : keyAliases) {
            if (keyStore.isKeyEntry(keyAlias)) {
                firstPrivateKeyAlias = keyAlias;
                break;
            }
        }

        return firstPrivateKeyAlias;
    }

    private static Set<String> getKeyAliases(final KeyStore keyStore) throws KeyStoreException {
        final Set<String> availableKeys = new HashSet<String>();
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            availableKeys.add(aliases.nextElement());
        }
        return availableKeys;
    }

    private static ExtendedMetadata createExtendedMetadata(final String signingAlgorithm, final boolean signMetadata) {
        final ExtendedMetadata extendedMetadata = new ExtendedMetadata();
        extendedMetadata.setIdpDiscoveryEnabled(true);
        extendedMetadata.setSigningAlgorithm(signingAlgorithm);
        extendedMetadata.setSignMetadata(signMetadata);
        extendedMetadata.setEcpEnabled(true);
        return extendedMetadata;
    }

    private static MetadataProvider createIdpMetadataProvider(final URI idpMetadataLocation, final HttpClient httpClient,
                                                              final Timer timer, final ParserPool parserPool) throws Exception {
        if (idpMetadataLocation.getScheme().startsWith("http")) {
            return createHttpIdpMetadataProvider(idpMetadataLocation, httpClient, timer, parserPool);
        } else {
            return createFileIdpMetadataProvider(idpMetadataLocation, parserPool);
        }
    }

    private static MetadataProvider createFileIdpMetadataProvider(final URI idpMetadataLocation, final ParserPool parserPool)
            throws MetadataProviderException {
        final String idpMetadataFilePath = idpMetadataLocation.getPath();
        final File idpMetadataFile = new File(idpMetadataFilePath);
        LOGGER.info("Loading IDP metadata from file located at: " + idpMetadataFile.getAbsolutePath());

        final FilesystemMetadataProvider filesystemMetadataProvider = new FilesystemMetadataProvider(idpMetadataFile);
        filesystemMetadataProvider.setParserPool(parserPool);
        filesystemMetadataProvider.initialize();
        return filesystemMetadataProvider;
    }

    private static MetadataProvider createHttpIdpMetadataProvider(final URI idpMetadataLocation, final HttpClient httpClient,
                                                                  final Timer timer, final ParserPool parserPool) throws Exception {
        final String idpMetadataUrl = idpMetadataLocation.toString();
        final HTTPMetadataProvider httpMetadataProvider = new HTTPMetadataProvider(timer, httpClient, idpMetadataUrl);
        httpMetadataProvider.setParserPool(parserPool);
        httpMetadataProvider.initialize();
        return httpMetadataProvider;
    }

    private static MetadataManager createMetadataManager(final MetadataProvider idpMetadataProvider, final ExtendedMetadata extendedMetadata, final KeyManager keyManager)
            throws MetadataProviderException {
        final ExtendedMetadataDelegate idpExtendedMetadataDelegate = new ExtendedMetadataDelegate(idpMetadataProvider, extendedMetadata);
        idpExtendedMetadataDelegate.setMetadataTrustCheck(true);
        idpExtendedMetadataDelegate.setMetadataRequireSignature(false);

        final MetadataManager metadataManager = new CachingMetadataManager(Arrays.asList(idpExtendedMetadataDelegate));
        metadataManager.setKeyManager(keyManager);
        metadataManager.afterPropertiesSet();
        return metadataManager;
    }

    private static void configureGlobalSecurityDefaults(final KeyManager keyManager, final String signingAlgorithm, final String digestAlgorithm) {
        final BasicSecurityConfiguration securityConfiguration = (BasicSecurityConfiguration) Configuration.getGlobalSecurityConfiguration();

        if (!StringUtils.isBlank(signingAlgorithm)) {
            final Credential defaultCredential = keyManager.getDefaultCredential();
            final Key signingKey = SecurityHelper.extractSigningKey(defaultCredential);

            // ensure that the requested signature algorithm can be produced by the type of key we have (i.e. RSA key -> rsa-sha1 signature)
            final String keyAlgorithm = signingKey.getAlgorithm();
            if (!signingAlgorithm.contains(keyAlgorithm.toLowerCase())) {
                throw new IllegalStateException("Key algorithm '" + keyAlgorithm + "' cannot be used to create signatures of type '" + signingAlgorithm + "'");
            }

            securityConfiguration.registerSignatureAlgorithmURI(keyAlgorithm, signingAlgorithm);
        }

        if (!StringUtils.isBlank(digestAlgorithm)) {
            securityConfiguration.setSignatureReferenceDigestMethod(digestAlgorithm);
        }
    }

}
