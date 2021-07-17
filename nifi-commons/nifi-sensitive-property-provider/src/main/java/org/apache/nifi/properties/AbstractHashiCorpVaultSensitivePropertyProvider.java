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

import org.apache.nifi.properties.BootstrapProperties.BootstrapPropertyKey;
import org.apache.nifi.vault.hashicorp.HashiCorpVaultCommunicationService;
import org.apache.nifi.vault.hashicorp.StandardHashiCorpVaultCommunicationService;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration.VaultConfigurationKey;
import org.springframework.core.env.PropertySource;

import java.io.IOException;
import java.nio.file.Paths;

public abstract class AbstractHashiCorpVaultSensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private static final String VAULT_PREFIX = "vault";

    private final String path;
    private final HashiCorpVaultCommunicationService vaultCommunicationService;
    private final BootstrapProperties vaultBootstrapProperties;

    AbstractHashiCorpVaultSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) {
        super(bootstrapProperties);

        final String vaultBootstrapConfFilename = bootstrapProperties
                .getProperty(BootstrapPropertyKey.HASHICORP_VAULT_SENSITIVE_PROPERTY_PROVIDER_CONF).orElse(null);
        vaultBootstrapProperties = getVaultBootstrapProperties(vaultBootstrapConfFilename);
        path = getSecretsEnginePath(vaultBootstrapProperties);
        if (hasRequiredVaultProperties()) {
            try {
                vaultCommunicationService = new StandardHashiCorpVaultCommunicationService(getVaultPropertySource(vaultBootstrapConfFilename));
            } catch (IOException e) {
                throw new SensitivePropertyProtectionException("Error configuring HashiCorpVaultCommunicationService", e);
            }
        } else {
            vaultCommunicationService = null;
        }
    }

    /**
     * Return the configured Secrets Engine path for this sensitive property provider.
     * @param vaultBootstrapProperties The Properties from the file located at bootstrap.protection.hashicorp.vault.conf
     * @return The Secrets Engine path
     */
    protected abstract String getSecretsEnginePath(final BootstrapProperties vaultBootstrapProperties);

    private static BootstrapProperties getVaultBootstrapProperties(final String vaultBootstrapConfFilename) {
        final BootstrapProperties vaultBootstrapProperties;
        if (vaultBootstrapConfFilename != null) {
            try {
                vaultBootstrapProperties = AbstractBootstrapPropertiesLoader.loadBootstrapProperties(
                        Paths.get(vaultBootstrapConfFilename), VAULT_PREFIX);
            } catch (IOException e) {
                throw new SensitivePropertyProtectionException("Could not load " + vaultBootstrapConfFilename, e);
            }
        } else {
            vaultBootstrapProperties = null;
        }
        return vaultBootstrapProperties;
    }

    private PropertySource<?> getVaultPropertySource(final String vaultBootstrapConfFilename) throws IOException {
        return HashiCorpVaultConfiguration.createPropertiesFileSource(vaultBootstrapConfFilename);
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
            throw new SensitivePropertyProtectionException(getIdentifierKey() + " protection scheme is not fully configured in hashicorp-vault-bootstrap.conf");
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
                && (vaultBootstrapProperties.getProperty(VaultConfigurationKey.URI.getKey()) != null)
                && hasRequiredSecretsEngineProperties(vaultBootstrapProperties);
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
