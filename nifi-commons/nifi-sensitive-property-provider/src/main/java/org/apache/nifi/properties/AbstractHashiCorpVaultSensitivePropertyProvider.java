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

import java.util.function.Supplier;

public abstract class AbstractHashiCorpVaultSensitivePropertyProvider extends AbstractSensitivePropertyProvider {

    private final String path;
    private final HashiCorpVaultCommunicationService vaultCommunicationService;

    AbstractHashiCorpVaultSensitivePropertyProvider(final BootstrapProperties bootstrapProperties, final Supplier<String> pathSupplier) {
        super(bootstrapProperties);
        path = pathSupplier.get();

        if (hasRequiredVaultProperties(bootstrapProperties)) {
            vaultCommunicationService = new StandardHashiCorpVaultCommunicationService(getVaultProperties(bootstrapProperties));
        } else {
            vaultCommunicationService = null;
        }
    }

    private static HashiCorpVaultProperties getVaultProperties(final BootstrapProperties bootstrapProperties) {
        final HashiCorpVaultProperties.HashiCorpVaultPropertiesBuilder builder = new HashiCorpVaultProperties.HashiCorpVaultPropertiesBuilder();
        bootstrapProperties.getHashiCorpVaultUri().ifPresent(uri -> builder.setUri(uri));
        bootstrapProperties.getHashiCorpVaultAuthPropsFilename().ifPresent(filename -> builder.setAuthPropertiesFilename(filename));

        bootstrapProperties.getHashiCorpVaultEnabledTlsProtocols().ifPresent(enabledProtocols -> builder.setEnabledTlsProtocols(enabledProtocols));
        bootstrapProperties.getHashiCorpVaultEnabledTlsCipherSuites().ifPresent(enabledCipherSuites -> builder.setEnabledTlsCipherSuites(enabledCipherSuites));
        bootstrapProperties.getHashiCorpVaultKeystore().ifPresent(keystore -> builder.setKeyStore(keystore));
        bootstrapProperties.getHashiCorpVaultKeystoreType().ifPresent(keystoreType -> builder.setKeyStoreType(keystoreType));
        bootstrapProperties.getHashiCorpVaultKeystorePassword().ifPresent(keystorePassword -> builder.setKeyStorePassword(keystorePassword));
        bootstrapProperties.getHashiCorpVaultTruststore().ifPresent(truststore -> builder.setTrustStore(truststore));
        bootstrapProperties.getHashiCorpVaultTruststoreType().ifPresent(truststoreType -> builder.setTrustStoreType(truststoreType));
        bootstrapProperties.getHashiCorpVaultTruststorePassword().ifPresent(truststorePassword -> builder.setTrustStorePassword(truststorePassword));

        bootstrapProperties.getHashiCorpVaultReadTimeout().ifPresent(timeout -> builder.setReadTimeout(timeout));
        bootstrapProperties.getHashiCorpVaultConnectionTimeout().ifPresent(timeout -> builder.setConnectionTimeout(timeout));

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
    protected boolean isSupported(final BootstrapProperties bootstrapProperties) {
        return hasRequiredVaultProperties(bootstrapProperties);
    }

    private boolean hasRequiredVaultProperties(final BootstrapProperties bootstrapProperties) {
        return bootstrapProperties.getHashiCorpVaultUri().isPresent()
                && bootstrapProperties.getHashiCorpVaultAuthPropsFilename().isPresent();
    }

    /**
     * Return true if the relevant Secrets Engine-specific properties are configured.
     * @param bootstrapProperties Bootstrap properties
     * @return true if the relevant Secrets Engine-specific properties are configured
     */
    protected abstract boolean hasRequiredSecretsEngineProperties(final BootstrapProperties bootstrapProperties);

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
