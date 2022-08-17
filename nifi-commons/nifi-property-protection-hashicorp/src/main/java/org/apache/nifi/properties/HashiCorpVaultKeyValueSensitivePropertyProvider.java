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

import java.util.Objects;

/**
 * Uses the HashiCorp Vault Key/Value version 1 Secrets Engine to store sensitive values.
 */
public class HashiCorpVaultKeyValueSensitivePropertyProvider extends AbstractHashiCorpVaultSensitivePropertyProvider {

    private static final String KEY_VALUE_PATH = "vault.kv.path";

    private static final String IDENTIFIER_KEY_FORMAT = "hashicorp/vault/kv/%s";

    HashiCorpVaultKeyValueSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) {
        super(bootstrapProperties);
    }

    @Override
    protected String getSecretsEnginePath(final BootstrapProperties vaultBootstrapProperties) {
        if (vaultBootstrapProperties == null) {
            return null;
        }
        return vaultBootstrapProperties.getProperty(KEY_VALUE_PATH);
    }

    @Override
    public String getIdentifierKey() {
        return String.format(IDENTIFIER_KEY_FORMAT, getPath());
    }

    /**
     * Stores the sensitive value in Vault and returns a description of the secret.
     *
     * @param unprotectedValue the sensitive value
     * @param context The property context, unused in this provider
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception writing the secret
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        requireNotBlank(unprotectedValue);
        Objects.requireNonNull(context, "Context is required to protect a value");

        getVaultCommunicationService().writeKeyValueSecret(getPath(), context.getContextKey(), unprotectedValue);
        return String.format("%s/%s", getPath(), context.getContextKey());
    }

    /**
     * Returns the secret value, as read from Vault.
     *
     * @param protectedValue The value read from {@code nifi.properties} file.  Ignored in this provider.
     * @param context The property context, from which the Vault secret name is pulled
     * @return the raw value to be used by the application
     * @throws SensitivePropertyProtectionException if there is an error retrieving the secret
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        Objects.requireNonNull(context, "Context is required to unprotect a value");

        return getVaultCommunicationService().readKeyValueSecret(getPath(), context.getContextKey())
                .orElseThrow(() -> new SensitivePropertyProtectionException(String
                        .format("Secret [%s] not found in Vault Key/Value engine at [%s]", context.getContextKey(), getPath())));
    }
}
