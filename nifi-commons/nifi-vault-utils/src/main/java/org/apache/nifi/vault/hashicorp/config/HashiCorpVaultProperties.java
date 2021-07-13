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
package org.apache.nifi.vault.hashicorp.config;

import org.apache.nifi.vault.hashicorp.HashiCorpVaultConfigurationException;

import java.io.File;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

/**
 * Properties to configure the HashiCorpVaultCommunicationService.  The only properties considered mandatory are uri and
 * authPropertiesFilename. See the following link for valid vault authentication properties (default is
 * vault.authentication=TOKEN, expecting a vault.token property to be supplied).
 *
 * @see <a href="https://docs.spring.io/spring-vault/docs/2.3.1/reference/html/#vault.core.environment-vault-configuration">
 *     https://docs.spring.io/spring-vault/docs/2.3.1/reference/html/#vault.core.environment-vault-configuration</a>
 */
public class HashiCorpVaultProperties {
    public static final String HTTPS = "https";
    private final String uri;
    private final String authPropertiesFilename;
    private final HashiCorpVaultSslProperties ssl;
    private final Optional<String> connectionTimeout;
    private final Optional<String> readTimeout;

    private HashiCorpVaultProperties(final String uri, String keyStore, final String keyStoreType, final String keyStorePassword, final String trustStore,
                                     final String trustStoreType, final String trustStorePassword, final String authPropertiesFilename,
                                     final String enabledTlsCipherSuites, final String enabledTlsProtocols, final String connectionTimeout, final String readTimeout) {
        Objects.requireNonNull(uri, "Vault URI is required");
        Objects.requireNonNull(authPropertiesFilename, "Vault auth properties filename is required");
        this.uri = uri;
        this.authPropertiesFilename = authPropertiesFilename;
        this.ssl = new HashiCorpVaultSslProperties(keyStore, keyStoreType, keyStorePassword, trustStore, trustStoreType, trustStorePassword,
                enabledTlsCipherSuites, enabledTlsProtocols);
        this.connectionTimeout = connectionTimeout == null ? Optional.empty() : Optional.of(connectionTimeout);
        this.readTimeout = readTimeout == null ? Optional.empty() : Optional.of(readTimeout);

        if (uri.startsWith(HTTPS)) {
            Objects.requireNonNull(keyStore, "KeyStore is required with an https URI");
            Objects.requireNonNull(keyStorePassword, "KeyStore password is required with an https URI");
            Objects.requireNonNull(keyStoreType, "KeyStore type is required with an https URI");
            Objects.requireNonNull(trustStore, "TrustStore is required with an https URI");
            Objects.requireNonNull(trustStorePassword, "TrustStore password is required with an https URI");
            Objects.requireNonNull(trustStoreType, "TrustStore type is required with an https URI");
        }
        validateAuthProperties();
    }

    private void validateAuthProperties() throws HashiCorpVaultConfigurationException {
        final File authPropertiesFile = Paths.get(authPropertiesFilename).toFile();
        if (!authPropertiesFile.exists()) {
            throw new HashiCorpVaultConfigurationException(String.format("Auth properties file [%s] does not exist", authPropertiesFilename));
        }
    }

    @HashiCorpVaultProperty
    public String getUri() {
        return uri;
    }

    @HashiCorpVaultProperty
    public HashiCorpVaultSslProperties getSsl() {
        return ssl;
    }

    @HashiCorpVaultProperty(key = "authentication.properties.file")
    public String getAuthPropertiesFilename() {
        return authPropertiesFilename;
    }

    public Optional<String> getConnectionTimeout() {
        return connectionTimeout;
    }

    public Optional<String> getReadTimeout() {
        return readTimeout;
    }

    /**
     * Builder for HashiCorpVaultProperties.  The only properties that are considered mandatory are uri and authPropertiesFilename.
     */
    public static class HashiCorpVaultPropertiesBuilder {
        private String uri;
        private String keyStore;
        private String keyStoreType;
        private String keyStorePassword;
        private String trustStore;
        private String trustStoreType;
        private String trustStorePassword;
        private String authPropertiesFilename;
        private String enabledTlsCipherSuites;
        private String enabledTlsProtocols;
        private String connectionTimeout;
        private String readTimeout;

        /**
         * Set the Vault URI (e.g., http://localhost:8200).  If using https protocol, the KeyStore and TrustStore
         * properties are expected to also be set.
         * @param uri Vault's URI
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Sets the path to the keyStore.
         * @param keyStore Path to the keyStore
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setKeyStore(String keyStore) {
            this.keyStore = keyStore;
            return this;
        }

        /**
         * Sets keyStore type (e.g., JKS, PKCS12).
         * @param keyStoreType KeyStore type
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setKeyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
            return this;
        }

        /**
         * Sets the keyStore password.
         * @param keyStorePassword KeyStore password
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        /**
         * Sets the path to the trustStore.
         * @param trustStore Path to the trustStore
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setTrustStore(String trustStore) {
            this.trustStore = trustStore;
            return this;
        }

        /**
         * Sets the trustStore type (e.g., JKS, PKCS12).
         * @param trustStoreType TrustStore type
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setTrustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * Sets the trustStore passsword.
         * @param trustStorePassword TrustStore password
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        /**
         * Sets the path to the vault authentication properties file.  See the following link for valid
         * vault authentication properties (default is vault.authentication=TOKEN, expecting a vault.token property
         * to be supplied).
         * @see <a href="https://docs.spring.io/spring-vault/docs/2.3.1/reference/html/#vault.core.environment-vault-configuration">
         *     https://docs.spring.io/spring-vault/docs/2.3.1/reference/html/#vault.core.environment-vault-configuration</a>
         * @param authPropertiesFilename The filename of a properties file containing Spring Vault authentication
         *                               properties
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setAuthPropertiesFilename(String authPropertiesFilename) {
            this.authPropertiesFilename = authPropertiesFilename;
            return this;
        }

        /**
         * Sets an optional comma-separated list of enabled TLS cipher suites.
         * @param enabledTlsCipherSuites Enabled TLS cipher suites (only these will be enabled)
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setEnabledTlsCipherSuites(String enabledTlsCipherSuites) {
            this.enabledTlsCipherSuites = enabledTlsCipherSuites;
            return this;
        }

        /**
         * Sets an optional comma-separated list of enabled TLS protocols.
         * @param enabledTlsProtocols Enabled TLS protocols (only these will be enabled)
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setEnabledTlsProtocols(String enabledTlsProtocols) {
            this.enabledTlsProtocols = enabledTlsProtocols;
            return this;
        }

        /**
         * Sets the connection timeout for the HTTP client, using the standard NiFi duration format (e.g., 5 secs)
         * @param connectionTimeout Connection timeout (default is 5 secs)
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setConnectionTimeout(String connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets the read timeout for the HTTP client, using the standard NiFi duration format (e.g., 15 secs).
         * @param readTimeout Read timeout (default is 15 secs)
         * @return
         */
        public HashiCorpVaultPropertiesBuilder setReadTimeout(String readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * Build the VaultProperties.
         * @return
         */
        public HashiCorpVaultProperties build() {
            return new HashiCorpVaultProperties(uri, keyStore, keyStoreType, keyStorePassword, trustStore, trustStoreType,
                    trustStorePassword, authPropertiesFilename, enabledTlsCipherSuites, enabledTlsProtocols, connectionTimeout, readTimeout);
        }
    }
}
