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
package org.apache.nifi.registry.web;


import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This customizer fixes integration tests. The customizer is the only way we can pass config from Spring Boot to Jetty.
 * It sets the endpointIdentificationAlgorithm to null, which stops the Jetty server attempting to validate a hostname in the client certificate's SAN.
 **/
@Component
public class JettyITServerCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JettyITServerCustomizer.class);

    @Autowired
    private ServerProperties serverProperties;

    private static final int HEADER_BUFFER_SIZE = 16 * 1024; // 16kb

    @Override
    public void customize(final JettyServletWebServerFactory factory) {
        LOGGER.info("Customizing Jetty server for integration tests...");

        factory.addServerCustomizers((server) -> {
            final Ssl sslProperties = serverProperties.getSsl();
            if (sslProperties != null) {
                createSslContextFactory(sslProperties);
                ServerConnector con = (ServerConnector) server.getConnectors()[0];
                int existingConnectorPort = con.getLocalPort();

                // create the http configuration
                final HttpConfiguration httpConfiguration = new HttpConfiguration();
                httpConfiguration.setRequestHeaderSize(HEADER_BUFFER_SIZE);
                httpConfiguration.setResponseHeaderSize(HEADER_BUFFER_SIZE);

                // add some secure config
                final HttpConfiguration httpsConfiguration = new HttpConfiguration(httpConfiguration);
                httpsConfiguration.setSecureScheme("https");
                httpsConfiguration.setSecurePort(existingConnectorPort);
                httpsConfiguration.addCustomizer(new SecureRequestCustomizer());

                // build the connector with the endpoint identification algorithm set to null
                final ServerConnector httpsConnector = new ServerConnector(server,
                        new SslConnectionFactory(createSslContextFactory(sslProperties), "http/1.1"),
                        new HttpConnectionFactory(httpsConfiguration));
                server.removeConnector(con);
                server.addConnector(httpsConnector);
            }
        });

        LOGGER.info("JettyServer is customized");
    }

    private SslContextFactory createSslContextFactory(Ssl properties) {
        // Calling SslContextFactory.Server() calls setEndpointIdentificationAlgorithm(null).
        // This ensures that Jetty server does not attempt to validate a hostname in the client certificate's SAN.
        final SslContextFactory.Server contextFactory = new SslContextFactory.Server();

        // if needClientAuth is false then set want to true so we can optionally use certs
        if(properties.getClientAuth() == Ssl.ClientAuth.NEED) {
            LOGGER.info("Setting Jetty's SSLContextFactory needClientAuth to true");
            contextFactory.setNeedClientAuth(true);
        } else {
            LOGGER.info("Setting Jetty's SSLContextFactory wantClientAuth to true");
            contextFactory.setWantClientAuth(true);
        }

        /* below code sets JSSE system properties when values are provided */
        // keystore properties
        if (StringUtils.isNotBlank(properties.getKeyStore())) {
            contextFactory.setKeyStorePath(properties.getKeyStore());
        }
        if (StringUtils.isNotBlank(properties.getKeyStoreType())) {
            contextFactory.setKeyStoreType(properties.getKeyStoreType());
        }
        final String keystorePassword = properties.getKeyStorePassword();
        final String keyPassword = properties.getKeyPassword();

        if (StringUtils.isEmpty(keystorePassword)) {
            throw new IllegalArgumentException("The keystore password cannot be null or empty");
        } else {
            // if no key password was provided, then assume the key password is the same as the keystore password.
            final String defaultKeyPassword = (StringUtils.isBlank(keyPassword)) ? keystorePassword : keyPassword;
            contextFactory.setKeyStorePassword(keystorePassword);
            contextFactory.setKeyManagerPassword(defaultKeyPassword);
        }

        // truststore properties
        if (StringUtils.isNotBlank(properties.getTrustStore())) {
            contextFactory.setTrustStorePath(properties.getTrustStore());
        }
        if (StringUtils.isNotBlank(properties.getTrustStoreType())) {
            contextFactory.setTrustStoreType(properties.getTrustStoreType());
        }
        if (StringUtils.isNotBlank(properties.getTrustStorePassword())) {
            contextFactory.setTrustStorePassword(properties.getTrustStorePassword());
        }

        return contextFactory;
    }

}
