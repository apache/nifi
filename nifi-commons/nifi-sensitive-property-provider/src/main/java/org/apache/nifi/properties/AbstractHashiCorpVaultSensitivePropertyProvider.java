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

import org.apache.nifi.vault.hashicorp.HashiCorpVaultCommunicationService;
import org.apache.nifi.vault.hashicorp.StandardHashiCorpVaultCommunicationService;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

public abstract class AbstractHashiCorpVaultSensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private static final String VAULT_PREFIX = "vault";

    private static final String URI = "vault.uri";
    private static final String AUTH_PROPS_FILE = "vault.auth.props.file";
    private static final String CONNECTION_TIMEOUT = "vault.connection.timeout";
    private static final String READ_TIMEOUT = "vault.read.timeout";
    private static final String ENABLED_TLS_CIPHER_SUITES = "vault.enabled.tls.cipher.suites";
    private static final String ENABLED_TLS_PROTOCOLS = "vault.enabled.tls.protocols";
    private static final String KEYSTORE = "vault.keystore";
    private static final String KEYSTORE_TYPE = "vault.keystoreType";
    private static final String KEYSTORE_PASSWD = "vault.keystorePasswd";
    private static final String TRUSTSTORE = "vault.truststore";
    private static final String TRUSTSTORE_TYPE = "vault.truststoreType";
    private static final String TRUSTSTORE_PASSWD = "vault.truststorePasswd";

    private final String path;
    private final HashiCorpVaultCommunicationService vaultCommunicationService;
    private final BootstrapProperties vaultBootstrapProperties;

    AbstractHashiCorpVaultSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) {
        super(bootstrapProperties);

        vaultBootstrapProperties = getVaultBootstrapProperties(bootstrapProperties);
        path = getSecretsEnginePath(vaultBootstrapProperties);
        if (hasRequiredVaultProperties()) {
            vaultCommunicationService = new StandardHashiCorpVaultCommunicationService(getVaultProperties());
        } else {
            vaultCommunicationService = null;
        }
    }

    /**
     * Return the configured Secrets Engine path for this sensitive property provider.
     * @param vaultBootstrapProperties The Properties from the file located at bootstrap.sensitive.props.hashicorp.vault.properties
     * @return The Secrets Engine path
     */
    protected abstract String getSecretsEnginePath(final BootstrapProperties vaultBootstrapProperties);

    private static BootstrapProperties getVaultBootstrapProperties(final BootstrapProperties bootstrapProperties) {
        final BootstrapProperties vaultBootstrapProperties;
        if (bootstrapProperties.getHashiCorpVaultPropertiesFile().isPresent()) {
            final String vaultPropertiesFilename = bootstrapProperties.getHashiCorpVaultPropertiesFile().get();
            try {
                vaultBootstrapProperties = AbstractBootstrapPropertiesLoader.loadBootstrapProperties(
                        Paths.get(vaultPropertiesFilename), VAULT_PREFIX);
            } catch (IOException e) {
                throw new SensitivePropertyProtectionException("Could not load " + vaultPropertiesFilename, e);
            }
        } else {
            vaultBootstrapProperties = null;
        }
        return vaultBootstrapProperties;
    }

    private HashiCorpVaultProperties getVaultProperties() {
        final HashiCorpVaultProperties.HashiCorpVaultPropertiesBuilder builder = new HashiCorpVaultProperties.HashiCorpVaultPropertiesBuilder();
        Optional.ofNullable(vaultBootstrapProperties.getProperty(URI)).ifPresent(builder::setUri);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(AUTH_PROPS_FILE)).ifPresent(builder::setAuthPropertiesFilename);

        Optional.ofNullable(vaultBootstrapProperties.getProperty(ENABLED_TLS_PROTOCOLS)).ifPresent(builder::setEnabledTlsProtocols);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(ENABLED_TLS_CIPHER_SUITES)).ifPresent(builder::setEnabledTlsCipherSuites);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(KEYSTORE)).ifPresent(builder::setKeyStore);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(KEYSTORE_TYPE)).ifPresent(builder::setKeyStoreType);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(KEYSTORE_PASSWD)).ifPresent(builder::setKeyStorePassword);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(TRUSTSTORE)).ifPresent(builder::setTrustStore);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(TRUSTSTORE_TYPE)).ifPresent(builder::setTrustStoreType);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(TRUSTSTORE_PASSWD)).ifPresent(builder::setTrustStorePassword);

        Optional.ofNullable(vaultBootstrapProperties.getProperty(READ_TIMEOUT)).ifPresent(builder::setReadTimeout);
        Optional.ofNullable(vaultBootstrapProperties.getProperty(CONNECTION_TIMEOUT)).ifPresent(builder::setConnectionTimeout);

        return builder.build();
    }

    /**
     * Returns the Secrets Engine path.
     * @return The Secrets Engine path
     */
    protected String getPath() {
        return path;
    }

    protected HashiCorpVaultCommunicationService getVaultCommunicationService() {
        if (vaultCommunicationService == null) {
            throw new SensitivePropertyProtectionException(getIdentifierKey() + " protection scheme is not configured in bootstrap.conf");
        }
        return vaultCommunicationService;
    }

    @Override
    public boolean isSupported() {
        return hasRequiredVaultProperties();
    }

    /**
     * Returns the Vault-specific bootstrap properties (e.g., bootstrap-vault.properties)
     * @return The Vault-specific bootstrap properties
     */
    protected BootstrapProperties getVaultBootstrapProperties() {
        return vaultBootstrapProperties;
    }

    private boolean hasRequiredVaultProperties() {
        return vaultBootstrapProperties != null
                && (vaultBootstrapProperties.getProperty(URI) != null)
                && (vaultBootstrapProperties.getProperty(AUTH_PROPS_FILE) != null);
    }

    /**
     * Return true if the relevant Secrets Engine-specific properties are configured.
     * @param vaultBootstrapProperties The Vault-specific bootstrap properties
     * @return true if the relevant Secrets Engine-specific properties are configured
     */
    protected abstract boolean hasRequiredSecretsEngineProperties(final BootstrapProperties vaultBootstrapProperties);

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties},
     * in the format 'vault/{secretsEngine}/{secretsEnginePath}'.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return getProtectionScheme().getIdentifier(path);
    }

}
