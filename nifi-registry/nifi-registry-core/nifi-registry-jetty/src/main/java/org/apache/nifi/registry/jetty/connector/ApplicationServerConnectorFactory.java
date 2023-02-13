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
package org.apache.nifi.registry.jetty.connector;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.jetty.configuration.connector.ApplicationLayerProtocol;
import org.apache.nifi.jetty.configuration.connector.StandardServerConnectorFactory;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.util.TlsPlatform;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Registry Application extension of Standard Jetty Server Connector Factory
 */
public class ApplicationServerConnectorFactory extends StandardServerConnectorFactory {

    private static final int HEADER_SIZE = 16384;

    private static final String CIPHER_SUITE_SEPARATOR_PATTERN = ",\\s*";

    private static final String DEFAULT_HOST = null;

    private final String includeCipherSuites;

    private final String excludeCipherSuites;

    private final String host;

    private SslContextFactory.Server sslContextFactory;

    public ApplicationServerConnectorFactory(final Server server, final NiFiRegistryProperties properties) {
        super(server, getPort(properties));

        host = getHost(properties);
        includeCipherSuites = properties.getHttpsCipherSuitesInclude();
        excludeCipherSuites = properties.getHttpsCipherSuitesExclude();

        if (properties.isHTTPSConfigured()) {
            if (properties.getNeedClientAuth()) {
                setNeedClientAuth(true);
            } else {
                setWantClientAuth(true);
            }

            final SSLContext sslContext = buildSslContext(properties);
            setSslContext(sslContext);

            setApplicationLayerProtocols(properties);

            // Set Transport Layer Security Protocols based on platform configuration
            setIncludeSecurityProtocols(TlsPlatform.getPreferredProtocols().toArray(new String[0]));
        }
    }

    /**
     * Get Server Connector using configured properties
     *
     * @return Server Connector
     */
    @Override
    public ServerConnector getServerConnector() {
        final ServerConnector serverConnector = super.getServerConnector();
        serverConnector.setHost(host);
        return serverConnector;
    }

    /**
     * Get Jetty Server SSL Context Factory and reuse the same instance for multiple invocations
     *
     * @return Jetty Server SSL Context Factory
     */
    @Override
    protected SslContextFactory.Server getSslContextFactory() {
        if (sslContextFactory == null) {
            sslContextFactory = super.getSslContextFactory();

            if (StringUtils.isNotBlank(includeCipherSuites)) {
                final String[] cipherSuites = getCipherSuites(includeCipherSuites);
                sslContextFactory.setIncludeCipherSuites(cipherSuites);
            }
            if (StringUtils.isNotBlank(excludeCipherSuites)) {
                final String[] cipherSuites = getCipherSuites(excludeCipherSuites);
                sslContextFactory.setExcludeCipherSuites(cipherSuites);
            }
        }

        return sslContextFactory;
    }

    /**
     * Get HTTP Configuration with additional settings
     *
     * @return HTTP Configuration
     */
    @Override
    protected HttpConfiguration getHttpConfiguration() {
        final HttpConfiguration httpConfiguration = super.getHttpConfiguration();

        httpConfiguration.setRequestHeaderSize(HEADER_SIZE);
        httpConfiguration.setResponseHeaderSize(HEADER_SIZE);

        return httpConfiguration;
    }

    private String[] getCipherSuites(final String cipherSuitesProperty) {
        return cipherSuitesProperty.split(CIPHER_SUITE_SEPARATOR_PATTERN);
    }

    private SSLContext buildSslContext(final NiFiRegistryProperties properties) {
        final KeyStore keyStore = buildKeyStore(properties);
        final char[] keyPassword = getKeyPassword(properties);
        final KeyStore trustStore = buildTrustStore(properties);

        return new StandardSslContextBuilder()
                .keyStore(keyStore)
                .keyPassword(keyPassword)
                .trustStore(trustStore)
                .build();
    }

    private char[] getKeyPassword(final NiFiRegistryProperties properties) {
        final String keyStorePassword = getRequiredProperty(properties, NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD);
        final String keyPassword = properties.getProperty(NiFiRegistryProperties.SECURITY_KEY_PASSWD, keyStorePassword);
        return keyPassword.toCharArray();
    }

    private KeyStore buildKeyStore(final NiFiRegistryProperties properties) {
        final String keyStore = getRequiredProperty(properties, NiFiRegistryProperties.SECURITY_KEYSTORE);
        final String keyStoreType = getRequiredProperty(properties, NiFiRegistryProperties.SECURITY_KEYSTORE_TYPE);
        final String keyStorePassword = getRequiredProperty(properties, NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD);
        return buildStore(keyStore, keyStoreType, keyStorePassword);
    }

    private KeyStore buildTrustStore(final NiFiRegistryProperties properties) {
        final String trustStore = getRequiredProperty(properties, NiFiRegistryProperties.SECURITY_TRUSTSTORE);
        final String trustStoreType = getRequiredProperty(properties, NiFiRegistryProperties.SECURITY_TRUSTSTORE_TYPE);
        final String trustStorePassword = getRequiredProperty(properties, NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD);
        return buildStore(trustStore, trustStoreType, trustStorePassword);
    }

    private KeyStore buildStore(
            final String store,
            final String storeType,
            final String storePassword
    ) {
        final Path trustStorePath = Paths.get(store);
        try (final InputStream inputStream = Files.newInputStream(trustStorePath)) {
            return new StandardKeyStoreBuilder()
                    .type(storeType)
                    .password(storePassword.toCharArray())
                    .inputStream(inputStream)
                    .build();
        } catch (final IOException e) {
            final String message = String.format("Store Path [%s] read failed", store);
            throw new IllegalStateException(message, e);
        }
    }

    private String getRequiredProperty(final NiFiRegistryProperties properties, final String property) {
        final String requiredProperty = properties.getProperty(property);
        if (requiredProperty == null || requiredProperty.isEmpty()) {
            throw new IllegalStateException(String.format("Required Property [%s] not configured", property));
        }
        return requiredProperty;
    }

    private void setApplicationLayerProtocols(final NiFiRegistryProperties properties) {
        final Set<String> protocols = properties.getWebHttpsApplicationProtocols();

        final Set<ApplicationLayerProtocol> applicationLayerProtocols = Arrays.stream(ApplicationLayerProtocol.values())
                .filter(
                        applicationLayerProtocol -> protocols.contains(applicationLayerProtocol.getProtocol())
                )
                .collect(Collectors.toSet());
        setApplicationLayerProtocols(applicationLayerProtocols);
    }

    private static String getHost(final NiFiRegistryProperties properties) {
        final String host;

        if (properties.isHTTPSConfigured()) {
            host = properties.getHttpsHost();
        } else {
            host = properties.getHttpHost();
        }

        return StringUtils.defaultIfEmpty(host, DEFAULT_HOST);
    }

    private static int getPort(final NiFiRegistryProperties properties) {
        final Integer httpsPort = properties.getSslPort();
        final Integer httpPort = properties.getPort();

        if (ObjectUtils.allNull(httpsPort, httpPort)) {
            throw new IllegalStateException("Invalid port configuration: Neither nifi.registry.web.https.port nor nifi.registry.web.http.port specified");
        } else if (ObjectUtils.allNotNull(httpsPort, httpPort)) {
            throw new IllegalStateException("Invalid port configuration: Both nifi.registry.web.https.port and nifi.registry.web.http.port specified");
        }

        return ObjectUtils.defaultIfNull(httpsPort, httpPort);
    }
}
