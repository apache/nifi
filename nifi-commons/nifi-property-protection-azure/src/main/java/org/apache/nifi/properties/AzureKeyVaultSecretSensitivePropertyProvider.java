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

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;

import java.util.Objects;

/**
 * Microsoft Azure Key Vault Secret implementation of Sensitive Property Provider for externalized storage of properties
 */
public class AzureKeyVaultSecretSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final String FORWARD_SLASH = "/";

    private static final String PERIOD = "\\.";

    private static final String DASH = "-";

    private static final String IDENTIFIER_KEY = "azure/keyvault/secret";

    private SecretClient secretClient;

    AzureKeyVaultSecretSensitivePropertyProvider(final SecretClient secretClient) {
        this.secretClient = secretClient;
    }

    /**
     * Get Identifier key using Protection Scheme
     *
     * @return Identifier key
     */
    @Override
    public String getIdentifierKey() {
        return IDENTIFIER_KEY;
    }

    /**
     * Is Provider supported returns status based on configuration of Secret Client
     *
     * @return Supported status
     */
    @Override
    public boolean isSupported() {
        return secretClient != null;
    }

    /**
     * Protect value stores a Secret in Azure Key Vault using the Property Context Key as the Secret Name
     *
     * @param unprotectedValue Value to be stored
     * @param context Property Context containing Context Key used to store Secret
     * @return Key Vault Secret Identifier
     * @throws SensitivePropertyProtectionException Thrown when storing Secret failed
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        Objects.requireNonNull(unprotectedValue, "Value required");
        final String secretName = getSecretName(context);
        try {
            final KeyVaultSecret keyVaultSecret = secretClient.setSecret(secretName, unprotectedValue);
            return keyVaultSecret.getId();
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException(String.format("Set Secret [%s] failed", secretName), e);
        }
    }

    /**
     * Unprotect value retrieves a Secret from Azure Key Vault using Property Context Key
     *
     * @param protectedValue Key Vault Secret Identifier is not used
     * @param context Property Context containing Context Key used to retrieve Secret
     * @return Secret Value
     * @throws SensitivePropertyProtectionException Thrown when Secret not found or retrieval failed
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        final String secretName = getSecretName(context);
        try {
            final KeyVaultSecret keyVaultSecret = secretClient.getSecret(secretName);
            return keyVaultSecret.getValue();
        } catch (final ResourceNotFoundException e) {
            throw new SensitivePropertyProtectionException(String.format("Secret [%s] not found", secretName), e);
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException(String.format("Secret [%s] request failed", secretName), e);
        }
    }

    /**
     * Clean up not implemented
     */
    @Override
    public void cleanUp() {

    }

    private String getSecretName(final ProtectedPropertyContext context) {
        final String contextKey = Objects.requireNonNull(context, "Context required").getContextKey();
        // Replace forward slash and period with dash since Azure Key Vault Secret Names do not support certain characters
        return contextKey.replaceAll(FORWARD_SLASH, DASH).replaceAll(PERIOD, DASH);
    }
}
