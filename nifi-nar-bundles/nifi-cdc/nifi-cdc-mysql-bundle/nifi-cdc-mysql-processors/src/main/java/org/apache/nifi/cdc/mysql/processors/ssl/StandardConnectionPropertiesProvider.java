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
package org.apache.nifi.cdc.mysql.processors.ssl;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Standard implementation of Connection Properties Provider
 */
public class StandardConnectionPropertiesProvider implements ConnectionPropertiesProvider {
    private static final String COMMA_SEPARATOR = ",";

    private final SSLMode sslMode;

    private final TlsConfiguration tlsConfiguration;

    public StandardConnectionPropertiesProvider(
            final SSLMode sslMode,
            final TlsConfiguration tlsConfiguration
    ) {
        this.sslMode = Objects.requireNonNull(sslMode, "SSL Mode required");
        this.tlsConfiguration = tlsConfiguration;
    }

    /**
     * Get Connection Properties based on SSL Mode and TLS Configuration
     *
     * @return JDBC Connection Properties
     */
    @Override
    public Map<String, String> getConnectionProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();

        if (SSLMode.DISABLED == sslMode) {
            properties.put(SecurityProperty.USE_SSL.getProperty(), Boolean.FALSE.toString());
        } else {
            // Enable TLS negotiation for all modes
            properties.put(SecurityProperty.USE_SSL.getProperty(), Boolean.TRUE.toString());

            if (SSLMode.PREFERRED == sslMode) {
                properties.put(SecurityProperty.REQUIRE_SSL.getProperty(), Boolean.FALSE.toString());
            } else {
                // Modes other than preferred require SSL
                properties.put(SecurityProperty.REQUIRE_SSL.getProperty(), Boolean.TRUE.toString());
            }

            if (SSLMode.VERIFY_IDENTITY == sslMode) {
                properties.put(SecurityProperty.VERIFY_SERVER_CERTIFICATE.getProperty(), Boolean.TRUE.toString());
            }

            if (tlsConfiguration == null) {
                // Set preferred protocols based on Java platform configuration
                final String protocols = String.join(COMMA_SEPARATOR, TlsPlatform.getPreferredProtocols());
                properties.put(SecurityProperty.ENABLED_TLS_PROTOCOLS.getProperty(), protocols);
            } else {
                final Map<String, String> certificateProperties = getCertificateProperties();
                properties.putAll(certificateProperties);
            }
        }

        return properties;
    }

    private Map<String, String> getCertificateProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();

        final String protocols = String.join(COMMA_SEPARATOR, tlsConfiguration.getEnabledProtocols());
        properties.put(SecurityProperty.ENABLED_TLS_PROTOCOLS.getProperty(), protocols);

        if (tlsConfiguration.isKeystorePopulated()) {
            properties.put(SecurityProperty.CLIENT_CERTIFICATE_KEY_STORE_URL.getProperty(), tlsConfiguration.getKeystorePath());
            properties.put(SecurityProperty.CLIENT_CERTIFICATE_KEY_STORE_TYPE.getProperty(), tlsConfiguration.getKeystoreType().getType());
            properties.put(SecurityProperty.CLIENT_CERTIFICATE_KEY_STORE_PASSWORD.getProperty(), tlsConfiguration.getKeystorePassword());
        }

        if (tlsConfiguration.isTruststorePopulated()) {
            properties.put(SecurityProperty.TRUST_CERTIFICATE_KEY_STORE_URL.getProperty(), tlsConfiguration.getTruststorePath());
            properties.put(SecurityProperty.TRUST_CERTIFICATE_KEY_STORE_TYPE.getProperty(), tlsConfiguration.getTruststoreType().getType());
            properties.put(SecurityProperty.TRUST_CERTIFICATE_KEY_STORE_PASSWORD.getProperty(), tlsConfiguration.getTruststorePassword());
        }

        return properties;
    }
}
