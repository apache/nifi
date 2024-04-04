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
package org.apache.nifi.web.server.connector;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.nifi.jetty.configuration.connector.ApplicationLayerProtocol;
import org.apache.nifi.jetty.configuration.connector.StandardServerConnectorFactory;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.server.util.StoreScanner;
import org.eclipse.jetty.server.HostHeaderCustomizer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.net.ssl.SSLContext;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.security.util.SslContextFactory.createSslContext;

/**
 * Framework extension of Server Connector Factory configures additional settings based on application properties
 */
public class FrameworkServerConnectorFactory extends StandardServerConnectorFactory {
    private static final String DEFAULT_AUTO_REFRESH_INTERVAL = "30 s";

    private static final int IDLE_TIMEOUT_MULTIPLIER = 2;

    private static final String CIPHER_SUITE_SEPARATOR_PATTERN = ",\\s*";

    private final int headerSize;

    private final int idleTimeout;

    private final Integer storeScanInterval;

    private final String includeCipherSuites;

    private final String excludeCipherSuites;

    private TlsConfiguration tlsConfiguration;

    private SslContextFactory.Server sslContextFactory;

    /**
     * Framework Server Connector Factory Constructor with required properties
     *
     * @param server Jetty Server
     * @param properties NiFi Properties
     */
    public FrameworkServerConnectorFactory(final Server server, final NiFiProperties properties) {
        super(server, getPort(properties));

        includeCipherSuites = properties.getProperty(NiFiProperties.WEB_HTTPS_CIPHERSUITES_INCLUDE);
        excludeCipherSuites = properties.getProperty(NiFiProperties.WEB_HTTPS_CIPHERSUITES_EXCLUDE);
        headerSize = DataUnit.parseDataSize(properties.getWebMaxHeaderSize(), DataUnit.B).intValue();
        idleTimeout = getIdleTimeout(properties);

        if (properties.isHTTPSConfigured()) {
            tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);
            try {
                final SSLContext sslContext = createSslContext(tlsConfiguration);
                setSslContext(sslContext);
            } catch (final TlsException e) {
                throw new IllegalStateException("Invalid nifi.web.https configuration in nifi.properties", e);
            }

            if (properties.isClientAuthRequiredForRestApi()) {
                setNeedClientAuth(true);
            } else {
                setWantClientAuth(true);
            }

            if (properties.isSecurityAutoReloadEnabled()) {
                final String securityAutoReloadInterval = properties.getSecurityAutoReloadInterval();
                final double reloadIntervalSeconds = FormatUtils.getPreciseTimeDuration(securityAutoReloadInterval, TimeUnit.SECONDS);
                storeScanInterval = (int) reloadIntervalSeconds;
            } else {
                storeScanInterval = null;
            }

            setApplicationLayerProtocols(properties);

            // Set Transport Layer Security Protocols based on platform configuration
            setIncludeSecurityProtocols(TlsPlatform.getPreferredProtocols().toArray(new String[0]));
        } else {
            storeScanInterval = null;
        }
    }

    /**
     * Get HTTP Configuration with additional settings based on application properties
     *
     * @return HTTP Configuration
     */
    @Override
    protected HttpConfiguration getHttpConfiguration() {
        final HttpConfiguration httpConfiguration = super.getHttpConfiguration();

        httpConfiguration.setRequestHeaderSize(headerSize);
        httpConfiguration.setResponseHeaderSize(headerSize);
        httpConfiguration.setIdleTimeout(idleTimeout);

        // Add HostHeaderCustomizer to set Host Header for HTTP/2 and HostHeaderHandler
        httpConfiguration.addCustomizer(new HostHeaderCustomizer());

        return httpConfiguration;
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

            if (storeScanInterval != null) {
                sslContextFactory.setKeyStorePath(tlsConfiguration.getKeystorePath());
                final StoreScanner keyStoreScanner = new StoreScanner(sslContextFactory, tlsConfiguration, sslContextFactory.getKeyStoreResource());
                keyStoreScanner.setScanInterval(storeScanInterval);
                getServer().addBean(keyStoreScanner);

                sslContextFactory.setTrustStorePath(tlsConfiguration.getTruststorePath());
                final StoreScanner trustStoreScanner = new StoreScanner(sslContextFactory, tlsConfiguration, sslContextFactory.getTrustStoreResource());
                trustStoreScanner.setScanInterval(storeScanInterval);
                getServer().addBean(trustStoreScanner);
            }
        }

        return sslContextFactory;
    }

    private void setApplicationLayerProtocols(final NiFiProperties properties) {
        final Set<String> protocols = properties.getWebHttpsApplicationProtocols();

        final Set<ApplicationLayerProtocol> applicationLayerProtocols = Arrays.stream(ApplicationLayerProtocol.values())
                .filter(
                        applicationLayerProtocol -> protocols.contains(applicationLayerProtocol.getProtocol())
                )
                .collect(Collectors.toSet());
        setApplicationLayerProtocols(applicationLayerProtocols);
    }

    private int getIdleTimeout(final NiFiProperties properties) {
        final String autoRefreshInterval = StringUtils.defaultIfBlank(properties.getAutoRefreshInterval(), DEFAULT_AUTO_REFRESH_INTERVAL);
        final double autoRefreshMilliseconds = FormatUtils.getPreciseTimeDuration(autoRefreshInterval, TimeUnit.MILLISECONDS);
        return Math.multiplyExact((int) autoRefreshMilliseconds, IDLE_TIMEOUT_MULTIPLIER);
    }

    private String[] getCipherSuites(final String cipherSuitesProperty) {
        return cipherSuitesProperty.split(CIPHER_SUITE_SEPARATOR_PATTERN);
    }

    private static int getPort(final NiFiProperties properties) {
        final Integer httpsPort = properties.getSslPort();
        final Integer httpPort = properties.getPort();

        if (ObjectUtils.allNull(httpsPort, httpPort)) {
            throw new IllegalStateException("Invalid port configuration in nifi.properties: Neither nifi.web.https.port nor nifi.web.http.port specified");
        } else if (ObjectUtils.allNotNull(httpsPort, httpPort)) {
            throw new IllegalStateException("Invalid port configuration in nifi.properties: Both nifi.web.https.port and nifi.web.http.port specified");
        }

        return ObjectUtils.defaultIfNull(httpsPort, httpPort);
    }
}
