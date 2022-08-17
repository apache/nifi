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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Uses the HashiCorp Vault Transit Secrets Engine to encrypt sensitive values at rest.
 */
public class HashiCorpVaultTransitSensitivePropertyProvider extends AbstractHashiCorpVaultSensitivePropertyProvider {
    private static final Charset PROPERTY_CHARSET = StandardCharsets.UTF_8;
    private static final String TRANSIT_PATH = "vault.transit.path";

    private static final String IDENTIFIER_KEY_FORMAT = "hashicorp/vault/transit/%s";

    HashiCorpVaultTransitSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) {
        super(bootstrapProperties);
    }

    @Override
    protected String getSecretsEnginePath(final BootstrapProperties vaultBootstrapProperties) {
        if (vaultBootstrapProperties == null) {
            return null;
        }
        return vaultBootstrapProperties.getProperty(TRANSIT_PATH);
    }

    @Override
    public String getIdentifierKey() {
        return String.format(IDENTIFIER_KEY_FORMAT, getPath());
    }

    /**
     * Returns the encrypted cipher text.
     *
     * @param unprotectedValue the sensitive value
     * @param context The property context, unused in this provider
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception encrypting the value
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        requireNotBlank(unprotectedValue);
        return getVaultCommunicationService().encrypt(getPath(), unprotectedValue.getBytes(PROPERTY_CHARSET));
    }

    /**
     * Returns the decrypted plaintext.
     *
     * @param protectedValue the cipher text read from the {@code nifi.properties} file
     * @param context The property context, unused in this provider
     * @return the raw value to be used by the application
     * @throws SensitivePropertyProtectionException if there is an error decrypting the cipher text
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) throws SensitivePropertyProtectionException {
        requireNotBlank(protectedValue);
        return new String(getVaultCommunicationService().decrypt(getPath(), protectedValue), PROPERTY_CHARSET);
    }
}
