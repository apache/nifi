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
package org.apache.nifi.vault.config;

import java.util.Objects;
import java.util.Optional;

/**
 * Properties to configure the VaultCommunicationService.  The only properties considered mandatory are uri and
 * authPropertiesFilename. See the following link for valid vault authentication properties (default is
 * vault.authentication=TOKEN, expecting a vault.token property to be supplied).
 *
 * @see <a href="https://docs.spring.io/spring-vault/docs/2.3.1/reference/html/#vault.core.environment-vault-configuration">
 *     https://docs.spring.io/spring-vault/docs/2.3.1/reference/html/#vault.core.environment-vault-configuration</a>
 */
public class VaultProperties {
    public static final String HTTPS = "https";
    private final String uri;
    private final String keystore;
    private final String keystoreType;
    private final String keystorePassword;
    private final String truststore;
    private final String truststoreType;
    private final String truststorePassword;
    private final String authPropertiesFilename;
    private final String enabledTlsCipherSuites;
    private final String enabledTlsProtocols;
    private final Optional<String> connectionTimeout;
    private final Optional<String> readTimeout;

    private VaultProperties(final String uri, String keystore, final String keystoreType, final String keystorePassword, final String truststore,
                            final String truststoreType, final String truststorePassword, final String authPropertiesFilename,
                            final String enabledTlsCipherSuites, final String enabledTlsProtocols, final String connectionTimeout, final String readTimeout) {
        Objects.requireNonNull(uri, "Vault URI is required");
        Objects.requireNonNull(authPropertiesFilename, "Vault auth properties filename is required");
        this.uri = uri;
        this.keystore = keystore;
        this.keystoreType = keystoreType;
        this.keystorePassword = keystorePassword;
        this.truststore = truststore;
        this.truststoreType = truststoreType;
        this.truststorePassword = truststorePassword;
        this.authPropertiesFilename = authPropertiesFilename;
        this.enabledTlsCipherSuites = enabledTlsCipherSuites;
        this.enabledTlsProtocols = enabledTlsProtocols;
        this.connectionTimeout = connectionTimeout == null ? Optional.empty() : Optional.of(connectionTimeout);
        this.readTimeout = readTimeout == null ? Optional.empty() : Optional.of(readTimeout);

        if (uri.startsWith(HTTPS)) {
            Objects.requireNonNull(keystore, "Keystore is required with an https URI");
            Objects.requireNonNull(keystorePassword, "Keystore password is required with an https URI");
            Objects.requireNonNull(keystoreType, "Keystore type is required with an https URI");
            Objects.requireNonNull(truststore, "Truststore is required with an https URI");
            Objects.requireNonNull(truststorePassword, "Truststore password is required with an https URI");
            Objects.requireNonNull(truststoreType, "Truststore type is required with an https URI");
        }
    }

    @VaultProperty(key = VaultEnvironment.VAULT_URI)
    public String getUri() {
        return uri;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_KEYSTORE)
    public String getKeystore() {
        return keystore;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_KEYSTORE_TYPE)
    public String getKeystoreType() {
        return keystoreType;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_KEYSTORE_PASSWORD)
    public String getKeystorePassword() {
        return keystorePassword;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_TRUSTSTORE)
    public String getTruststore() {
        return truststore;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_TRUSTSTORE_TYPE)
    public String getTruststoreType() {
        return truststoreType;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_TRUSTSTORE_PASSWORD)
    public String getTruststorePassword() {
        return truststorePassword;
    }

    public String getAuthPropertiesFilename() {
        return authPropertiesFilename;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_ENABLED_CIPHER_SUITES)
    public String getEnabledTlsCipherSuites() {
        return enabledTlsCipherSuites;
    }

    @VaultProperty(key = VaultEnvironment.VAULT_SSL_ENABLED_PROTOCOLS)
    public String getEnabledTlsProtocols() {
        return enabledTlsProtocols;
    }

    public Optional<String> getConnectionTimeout() {
        return connectionTimeout;
    }

    public Optional<String> getReadTimeout() {
        return readTimeout;
    }

    /**
     * Builder for VaultProperties.  The only properties that are considered mandatory are uri and authPropertiesFilename.
     */
    public static class VaultPropertiesBuilder {
        private String uri;
        private String keystore;
        private String keystoreType;
        private String keystorePassword;
        private String truststore;
        private String truststoreType;
        private String truststorePassword;
        private String authPropertiesFilename;
        private String enabledTlsCipherSuites;
        private String enabledTlsProtocols;
        private String connectionTimeout;
        private String readTimeout;

        /**
         * Set the Vault URI (e.g., http://localhost:8200).  If using https protocol, the Keystore and Truststore
         * properties are expected to also be set.
         * @param uri Vault's URI
         * @return
         */
        public VaultPropertiesBuilder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Sets the path to the keystore.
         * @param keystore Path to the keystore
         * @return
         */
        public VaultPropertiesBuilder setKeystore(String keystore) {
            this.keystore = keystore;
            return this;
        }

        /**
         * Sets keystore type (e.g., JKS, PKCS12).
         * @param keystoreType Keystore type
         * @return
         */
        public VaultPropertiesBuilder setKeystoreType(String keystoreType) {
            this.keystoreType = keystoreType;
            return this;
        }

        /**
         * Sets the keystore password.
         * @param keystorePassword Keystore password
         * @return
         */
        public VaultPropertiesBuilder setKeystorePassword(String keystorePassword) {
            this.keystorePassword = keystorePassword;
            return this;
        }

        /**
         * Sets the path to the truststore.
         * @param truststore Path to the truststore
         * @return
         */
        public VaultPropertiesBuilder setTruststore(String truststore) {
            this.truststore = truststore;
            return this;
        }

        /**
         * Sets the truststore type (e.g., JKS, PKCS12).
         * @param truststoreType Truststore type
         * @return
         */
        public VaultPropertiesBuilder setTruststoreType(String truststoreType) {
            this.truststoreType = truststoreType;
            return this;
        }

        /**
         * Sets the truststore passsword.
         * @param truststorePassword Truststore password
         * @return
         */
        public VaultPropertiesBuilder setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
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
        public VaultPropertiesBuilder setAuthPropertiesFilename(String authPropertiesFilename) {
            this.authPropertiesFilename = authPropertiesFilename;
            return this;
        }

        /**
         * Sets an optional comma-separated list of enabled TLS cipher suites.
         * @param enabledTlsCipherSuites Enabled TLS cipher suites (only these will be enabled)
         * @return
         */
        public VaultPropertiesBuilder setEnabledTlsCipherSuites(String enabledTlsCipherSuites) {
            this.enabledTlsCipherSuites = enabledTlsCipherSuites;
            return this;
        }

        /**
         * Sets an optional comma-separated list of enabled TLS protocols.
         * @param enabledTlsProtocols Enabled TLS protocols (only these will be enabled)
         * @return
         */
        public VaultPropertiesBuilder setEnabledTlsProtocols(String enabledTlsProtocols) {
            this.enabledTlsProtocols = enabledTlsProtocols;
            return this;
        }

        /**
         * Sets the connection timeout for the HTTP client, using the standard NiFi duration format (e.g., 5 secs)
         * @param connectionTimeout Connection timeout (default is 5 secs)
         * @return
         */
        public VaultPropertiesBuilder setConnectionTimeout(String connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets the read timeout for the HTTP client, using the standard NiFi duration format (e.g., 15 secs).
         * @param readTimeout Read timeout (default is 15 secs)
         * @return
         */
        public VaultPropertiesBuilder setReadTimeout(String readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * Build the VaultProperties.
         * @return
         */
        public VaultProperties build() {
            return new VaultProperties(uri, keystore, keystoreType, keystorePassword, truststore, truststoreType,
                    truststorePassword, authPropertiesFilename, enabledTlsCipherSuites, enabledTlsProtocols, connectionTimeout, readTimeout);
        }
    }
}
