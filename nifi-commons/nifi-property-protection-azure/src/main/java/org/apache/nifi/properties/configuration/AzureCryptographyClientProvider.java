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
package org.apache.nifi.properties.configuration;

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import org.apache.nifi.properties.SensitivePropertyProtectionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Microsoft Azure Cryptography Client Provider
 */
public class AzureCryptographyClientProvider extends AzureClientProvider<CryptographyClient> {
    protected static final String KEY_ID_PROPERTY = "azure.keyvault.key.id";

    private static final Set<String> REQUIRED_PROPERTY_NAMES = new HashSet<>(Collections.singletonList(KEY_ID_PROPERTY));

    /**
     * Get Configured Client using Default Azure Credentials Builder and configured Key Identifier
     *
     * @param clientProperties Client Properties
     * @return Cryptography Client
     */
    @Override
    protected CryptographyClient getConfiguredClient(final Properties clientProperties) {
        final String keyIdentifier = clientProperties.getProperty(KEY_ID_PROPERTY);
        logger.debug("Azure Cryptography Client with Key Identifier [{}]", keyIdentifier);

        try {
            return new CryptographyClientBuilder()
                    .credential(getDefaultTokenCredential())
                    .keyIdentifier(keyIdentifier)
                    .buildClient();
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException("Azure Cryptography Builder Client Failed using Default Credentials", e);
        }
    }

    /**
     * Get required property names for Azure Cryptography Client
     *
     * @return Required client property names
     */
    @Override
    protected Set<String> getRequiredPropertyNames() {
        return REQUIRED_PROPERTY_NAMES;
    }
}
