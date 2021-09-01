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

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.SensitivePropertyProtectionException;

import java.util.Properties;

/**
 * Microsoft Azure Cryptography Client Provider
 */
public class AzureCryptographyClientProvider extends BootstrapPropertiesClientProvider<CryptographyClient> {
    private static final String KEY_ID_PROPERTY = "azure.keyvault.key.id";

    public AzureCryptographyClientProvider() {
        super(BootstrapProperties.BootstrapPropertyKey.AZURE_KEYVAULT_SENSITIVE_PROPERTY_PROVIDER_CONF);
    }

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
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .keyIdentifier(keyIdentifier)
                    .buildClient();
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException("Azure Cryptography Builder Client Failed using Default Credentials", e);
        }
    }
}
