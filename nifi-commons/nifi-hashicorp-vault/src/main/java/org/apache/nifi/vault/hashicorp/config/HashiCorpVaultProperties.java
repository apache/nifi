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
    private final int kvVersion;

    private HashiCorpVaultProperties(final HashiCorpVaultPropertiesBuilder builder) {
        this.uri =  Objects.requireNonNull(builder.uri, "Vault URI is required");
        this.authPropertiesFilename = Objects.requireNonNull(builder.authPropertiesFilename, "Vault auth properties filename is required");
        this.ssl = new HashiCorpVaultSslProperties(builder.keyStore, builder.keyStoreType, builder.keyStorePassword,
                builder.trustStore, builder.trustStoreType, builder.trustStorePassword, builder.enabledTlsCipherSuites, builder.enabledTlsProtocols);
        this.connectionTimeout = builder.connectionTimeout == null ? Optional.empty() : Optional.of(builder.connectionTimeout);
        this.readTimeout = builder.readTimeout == null ? Optional.empty() : Optional.of(builder.readTimeout);
        this.kvVersion = builder.kvVersion;
        if (kvVersion != 1 && kvVersion != 2) {
            throw new HashiCorpVaultConfigurationException("Key/Value version " + kvVersion + " is not supported");
        }

        if (uri.startsWith(HTTPS)) {
            Objects.requireNonNull(builder.keyStore, "KeyStore is required with an https URI");
            Objects.requireNonNull(builder.keyStorePassword, "KeyStore password is required with an https URI");
            Objects.requireNonNull(builder.keyStoreType, "KeyStore type is required with an https URI");
            Objects.requireNonNull(builder.trustStore, "TrustStore is required with an https URI");
            Objects.requireNonNull(builder.trustStorePassword, "TrustStore password is required with an https URI");
            Objects.requireNonNull(builder.trustStoreType, "TrustStore type is required with an https URI");
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

    @HashiCorpVaultProperty(key = "kv.version")
    public int getKvVersion() {
        return kvVersion;
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
        private int kvVersion = 1;

        /**
         * Set the Vault URI (e.g., http://localhost:8200).  If using https protocol, the KeyStore and TrustStore
         * properties are expected to also be set.
         * @param uri Vault's URI
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Sets the Key/Value secrets engine version (1 or 2).
         * @param kvVersion The Key/Value engine version
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setKvVersion(int kvVersion) {
            this.kvVersion = kvVersion;
            return this;
        }

        /**
         * Sets the path to the keyStore.
         * @param keyStore Path to the keyStore
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setKeyStore(String keyStore) {
            this.keyStore = keyStore;
            return this;
        }

        /**
         * Sets keyStore type (e.g., JKS, PKCS12).
         * @param keyStoreType KeyStore type
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setKeyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
            return this;
        }

        /**
         * Sets the keyStore password.
         * @param keyStorePassword KeyStore password
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        /**
         * Sets the path to the trustStore.
         * @param trustStore Path to the trustStore
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setTrustStore(String trustStore) {
            this.trustStore = trustStore;
            return this;
        }

        /**
         * Sets the trustStore type (e.g., JKS, PKCS12).
         * @param trustStoreType TrustStore type
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setTrustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * Sets the trustStore passsword.
         * @param trustStorePassword TrustStore password
         * @return Builder
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
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setAuthPropertiesFilename(String authPropertiesFilename) {
            this.authPropertiesFilename = authPropertiesFilename;
            return this;
        }

        /**
         * Sets an optional comma-separated list of enabled TLS cipher suites.
         * @param enabledTlsCipherSuites Enabled TLS cipher suites (only these will be enabled)
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setEnabledTlsCipherSuites(String enabledTlsCipherSuites) {
            this.enabledTlsCipherSuites = enabledTlsCipherSuites;
            return this;
        }

        /**
         * Sets an optional comma-separated list of enabled TLS protocols.
         * @param enabledTlsProtocols Enabled TLS protocols (only these will be enabled)
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setEnabledTlsProtocols(String enabledTlsProtocols) {
            this.enabledTlsProtocols = enabledTlsProtocols;
            return this;
        }

        /**
         * Sets the connection timeout for the HTTP client, using the standard NiFi duration format (e.g., 5 secs)
         * @param connectionTimeout Connection timeout (default is 5 secs)
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setConnectionTimeout(String connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets the read timeout for the HTTP client, using the standard NiFi duration format (e.g., 15 secs).
         * @param readTimeout Read timeout (default is 15 secs)
         * @return Builder
         */
        public HashiCorpVaultPropertiesBuilder setReadTimeout(String readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * Build the VaultProperties.
         * @return Builder
         */
        public HashiCorpVaultProperties build() {
            return new HashiCorpVaultProperties(this);
        }
    }
}
