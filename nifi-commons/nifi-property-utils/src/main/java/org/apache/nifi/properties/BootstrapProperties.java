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
package org.apache.nifi.properties;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Properties representing bootstrap.conf.
 */
public class BootstrapProperties extends StandardReadableProperties {
    private static final String PROPERTY_KEY_FORMAT = "%s.%s";
    private static final String BOOTSTRAP_SENSITIVE_KEY = "bootstrap.sensitive.key";

    // HashiCorp Vault Sensitive Property Provider configuration
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_TRANSIT_PATH = "bootstrap.sensitive.props.hashicorp.vault.transit.path";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_URI = "bootstrap.sensitive.props.hashicorp.vault.uri";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_AUTH_PROPS_FILE = "bootstrap.sensitive.props.hashicorp.vault.auth.props.file";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_CONNECTION_TIMEOUT = "bootstrap.sensitive.props.hashicorp.vault.connection.timeout";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_READ_TIMEOUT = "bootstrap.sensitive.props.hashicorp.vault.read.timeout";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_ENABLED_TLS_CIPHER_SUITES = "bootstrap.sensitive.props.hashicorp.vault.enabled.tls.cipher.suites";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_ENABLED_TLS_PROTOCOLS = "bootstrap.sensitive.props.hashicorp.vault.enabled.tls.protocols";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_KEYSTORE = "bootstrap.sensitive.props.hashicorp.vault.keystore";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_KEYSTORE_TYPE = "bootstrap.sensitive.props.hashicorp.vault.keystoreType";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_KEYSTORE_PASSWD = "bootstrap.sensitive.props.hashicorp.vault.keystorePasswd";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_TRUSTSTORE = "bootstrap.sensitive.props.hashicorp.vault.truststore";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_TRUSTSTORE_TYPE = "bootstrap.sensitive.props.hashicorp.vault.truststoreType";
    private static final String BOOTSTRAP_SPP_HASHICORP_VAULT_TRUSTSTORE_PASSWD = "bootstrap.sensitive.props.hashicorp.vault.truststorePasswd";

    private final String propertyPrefix;
    private final Path configFilePath;

    public BootstrapProperties(final String propertyPrefix, final Properties properties, final Path configFilePath) {
        super(new Properties());

        Objects.requireNonNull(properties, "Properties are required");
        this.propertyPrefix = Objects.requireNonNull(propertyPrefix, "Property prefix is required");
        this.configFilePath = configFilePath;

        this.filterProperties(properties);

    }

    /**
     * Ensures that blank or empty properties are returned as null.
     * @param key The property key
     * @param defaultValue The default value to use if the value is null or empty
     * @return The property value (null if empty or blank)
     */
    @Override
    public String getProperty(final String key, final String defaultValue) {
        final String property = super.getProperty(key, defaultValue);
        return isBlank(property) ? null : property;
    }

    /**
     * Ensures that blank or empty properties are returned as null.
     * @param key The property key
     * @return The property value (null if empty or blank)
     */
    @Override
    public String getProperty(final String key) {
        final String property = super.getProperty(key);
        return isBlank(property) ? null : property;
    }

    /**
     * Returns the path to the bootstrap config file.
     * @return The path to the file
     */
    public Path getConfigFilePath() {
        return configFilePath;
    }

    /**
     * Includes only the properties starting with the propertyPrefix.
     * @param properties Unfiltered properties
     */
    private void filterProperties(final Properties properties) {
        getRawProperties().clear();
        final Properties filteredProperties = new Properties();
        for(final Enumeration<Object> e = properties.keys() ; e.hasMoreElements(); ) {
            final String key = e.nextElement().toString();
            if (key.startsWith(propertyPrefix)) {
                filteredProperties.put(key, properties.getProperty(key));
            }
        }
        getRawProperties().putAll(filteredProperties);
    }

    private String getPropertyKey(final String subKey) {
        return String.format(PROPERTY_KEY_FORMAT, propertyPrefix, subKey);
    }

    /**
     * Returns the bootstrap sensitive key.
     * @return The bootstrap sensitive key
     */
    public Optional<String> getBootstrapSensitiveKey() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SENSITIVE_KEY)));
    }

    /**
     * Returns the Secrets Engine path for a HashiCorp Vault Transit Sensitive Property Provider.
     * @return The Vault path of the Transit Secrets Engine
     */
    public Optional<String> getHashiCorpVaultTransitPath() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_TRANSIT_PATH)));
    }

    /**
     * Returns the Vault URI for a HashiCorp Vault Sensitive Property Provider.
     * @return The Vault URI
     */
    public Optional<String> getHashiCorpVaultUri() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_URI)));
    }

    /**
     * Returns the Authentication properties filename for a HashiCorp Vault Sensitive Property Provider.
     * @return The Authentication properties filename
     */
    public Optional<String> getHashiCorpVaultAuthPropsFilename() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_AUTH_PROPS_FILE)));
    }

    /**
     * Returns the read timeout for a HashiCorp Vault Sensitive Property Provider.
     * @return The read timeout
     */
    public Optional<String> getHashiCorpVaultReadTimeout() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_READ_TIMEOUT)));
    }

    /**
     * Returns the connection timeout for a HashiCorp Vault Sensitive Property Provider.
     * @return The connection timeout
     */
    public Optional<String> getHashiCorpVaultConnectionTimeout() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_CONNECTION_TIMEOUT)));
    }

    /**
     * Returns the enabled TLS cipher suites for a HashiCorp Vault Sensitive Property Provider.
     * @return The enabled TLS cipher suites
     */
    public Optional<String> getHashiCorpVaultEnabledTlsCipherSuites() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_ENABLED_TLS_CIPHER_SUITES)));
    }

    /**
     * Returns the enabled TLS protocols for a HashiCorp Vault Sensitive Property Provider.
     * @return The enabled TLS protocols
     */
    public Optional<String> getHashiCorpVaultEnabledTlsProtocols() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_ENABLED_TLS_PROTOCOLS)));
    }

    /**
     * Returns the keystore path for a HashiCorp Vault Sensitive Property Provider.
     * @return The keystore path
     */
    public Optional<String> getHashiCorpVaultKeystore() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_KEYSTORE)));
    }

    /**
     * Returns the keystore type for a HashiCorp Vault Sensitive Property Provider.
     * @return The keystore type
     */
    public Optional<String> getHashiCorpVaultKeystoreType() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_KEYSTORE_TYPE)));
    }

    /**
     * Returns the keystore password for a HashiCorp Vault Sensitive Property Provider.
     * @return The keystore password
     */
    public Optional<String> getHashiCorpVaultKeystorePassword() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_KEYSTORE_PASSWD)));
    }

    /**
     * Returns the truststore path for a HashiCorp Vault Sensitive Property Provider.
     * @return The truststore path
     */
    public Optional<String> getHashiCorpVaultTruststore() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_TRUSTSTORE)));
    }

    /**
     * Returns the truststore type for a HashiCorp Vault Sensitive Property Provider.
     * @return The truststore type
     */
    public Optional<String> getHashiCorpVaultTruststoreType() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_TRUSTSTORE_TYPE)));
    }

    /**
     * Returns the truststore password for a HashiCorp Vault Sensitive Property Provider.
     * @return The truststore password
     */
    public Optional<String> getHashiCorpVaultTruststorePassword() {
        return Optional.ofNullable(getProperty(getPropertyKey(BOOTSTRAP_SPP_HASHICORP_VAULT_TRUSTSTORE_PASSWD)));
    }

    @Override
    public String toString() {
        return String.format("Bootstrap properties [%s] with prefix [%s]", configFilePath, propertyPrefix);
    }

    /**
     * An empty instance of BootstrapProperties.
     */
    public static final BootstrapProperties EMPTY = new BootstrapProperties("", new Properties(), Paths.get("conf/bootstrap.conf")) {
        @Override
        public Set<String> getPropertyKeys() {
            return null;
        }

        @Override
        public String getProperty(String key) {
            return null;
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }
    };

    private static boolean isBlank(final String string) {
        return (string == null) || string.isEmpty() || string.trim().isEmpty();
    }
}
