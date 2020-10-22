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
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.saml.NiFiSAMLContextProvider;
import org.apache.nifi.web.security.saml.SAMLConfiguration;
import org.apache.nifi.web.security.saml.SAMLConfigurationFactory;
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

import javax.servlet.ServletException;
import java.io.File;
import java.net.URI;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
        LOGGER.info("SAML Service Provider Entity ID = '{}'", new Object[]{spEntityId});

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
        LOGGER.info("SAML Identity Provider Metadata Location = '{}'", new Object[]{idpMetadataLocation});

        final String authExpirationFromProperties = properties.getSamlAuthenticationExpiration();
        LOGGER.info("SAML Authentication Expiration = '{}'", new Object[]{authExpirationFromProperties});

        final long authExpiration;
        try {
            authExpiration = Math.round(FormatUtils.getPreciseTimeDuration(authExpirationFromProperties, TimeUnit.MILLISECONDS));
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid SAML authentication expiration: " + authExpirationFromProperties);
        }

        final String groupAttributeName = properties.getSamlGroupAttributeName();
        if (!StringUtils.isBlank(groupAttributeName)) {
            LOGGER.info("SAML Group Attribute Name = '{}'", new Object[]{groupAttributeName});
        }

        // Initialize spring-security-saml/OpenSAML objects...

        final SAMLBootstrap samlBootstrap = new SAMLBootstrap();
        samlBootstrap.postProcessBeanFactory(null);

        final ParserPool parserPool = createParserPool();
        final VelocityEngine velocityEngine = VelocityFactory.getEngine();
        final HttpClient httpClient = createHttpClient();
        final KeyManager keyManager = createKeyManager(properties);

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
                .groupAttributeName(groupAttributeName)
                .build();
    }

    private static ParserPool createParserPool() throws XMLParserException {
        final StaticBasicParserPool parserPool = new StaticBasicParserPool();
        parserPool.initialize();
        return parserPool;
    }

    private static HttpClient createHttpClient() {
        final HttpClientParams clientParams = new HttpClientParams();
        clientParams.setSoTimeout(20000);
        clientParams.setConnectionManagerTimeout(20000);
        return new HttpClient(clientParams);
    }

    private static SAMLProcessor createSAMLProcessor(final ParserPool parserPool, final VelocityEngine velocityEngine,
                                                     final HttpClient httpClient) {

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

    private static KeyManager createKeyManager(final NiFiProperties properties) throws TlsException, KeyStoreException {
        final TlsConfiguration tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);

        final String keystorePath = tlsConfiguration.getKeystorePath();
        final char[] keystorePasswordChars = tlsConfiguration.getKeystorePassword().toCharArray();
        final String keystoreType = tlsConfiguration.getKeystoreType().getType();

        final KeyStore keyStore = KeyStoreUtils.loadKeyStore(keystorePath, keystorePasswordChars, keystoreType);

        final String keyAlias = properties.getSamlSigningKeyAlias();
        if(StringUtils.isBlank(keyAlias)) {
            throw new RuntimeException("Signing Key Alias is required when configuring SAML");
        }

        final Set<String> keyAliases = getKeyAliases(keyStore);
        if (!keyAliases.contains(keyAlias)) {
            throw new RuntimeException("The specified Singing Key Alias '" + keyAlias + "' does not exist in the specified keystore");
        }

        final Map<String,String> keyPasswords = new HashMap<>();
        final String keyPassword = tlsConfiguration.getKeyPassword();
        if (!StringUtils.isBlank(keyPassword)) {
            keyPasswords.put(keyAlias, keyPassword);
        }

        final KeyManager keyManager = new JKSKeyManager(keyStore, keyPasswords, keyAlias);
        return keyManager;
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

    private static MetadataProvider createIdpMetadataProvider(final URI idpMetadataLocation, final HttpClient httpClient, final Timer timer,
                                                              final ParserPool parserPool) throws MetadataProviderException {
        final MetadataProvider metadataProvider;
        if (idpMetadataLocation.getScheme().startsWith("http")) {
            final String idpMetadataUrl = idpMetadataLocation.toString();
            final HTTPMetadataProvider httpMetadataProvider = new HTTPMetadataProvider(timer, httpClient, idpMetadataUrl);
            httpMetadataProvider.setParserPool(parserPool);
            httpMetadataProvider.initialize();
            metadataProvider = httpMetadataProvider;
        } else {
            final String idpMetadataFilePath = idpMetadataLocation.getPath();
            final File idpMetadataFile = new File(idpMetadataFilePath);
            LOGGER.info("Loading IDP metadata from file located at: " + idpMetadataFile.getAbsolutePath());

            final FilesystemMetadataProvider filesystemMetadataProvider = new FilesystemMetadataProvider(idpMetadataFile);
            filesystemMetadataProvider.setParserPool(parserPool);
            filesystemMetadataProvider.initialize();
            metadataProvider = filesystemMetadataProvider;
        }
        return metadataProvider;
    }

    private static MetadataManager createMetadataManager(final MetadataProvider idpMetadataProvider, final ExtendedMetadata extendedMetadata,
                                                         final KeyManager keyManager)
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
