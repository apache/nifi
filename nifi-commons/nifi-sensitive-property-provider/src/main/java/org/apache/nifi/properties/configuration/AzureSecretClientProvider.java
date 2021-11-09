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

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import org.apache.nifi.properties.SensitivePropertyProtectionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Microsoft Azure Secret Client Provider
 */
public class AzureSecretClientProvider extends AzureClientProvider<SecretClient> {
    protected static final String URI_PROPERTY = "azure.keyvault.uri";

    private static final Set<String> REQUIRED_PROPERTY_NAMES = new HashSet<>(Collections.singletonList(URI_PROPERTY));

    /**
     * Get Secret Client using Default Azure Credentials Builder and default configuration from environment variables
     *
     * @param clientProperties Client Properties
     * @return Secret Client
     */
    @Override
    protected SecretClient getConfiguredClient(final Properties clientProperties) {
        final String uri = clientProperties.getProperty(URI_PROPERTY);
        logger.debug("Azure Secret Client with URI [{}]", uri);

        try {
            final TokenCredential credential = new DefaultAzureCredentialBuilder().build();
            return new SecretClientBuilder()
                    .credential(credential)
                    .vaultUrl(uri)
                    .buildClient();
        } catch (final RuntimeException e) {
            throw new SensitivePropertyProtectionException("Azure Secret Builder Client Failed using Default Credentials", e);
        }
    }

    /**
     * Get required property names for Azure Secret Client
     *
     * @return Required client property names
     */
    @Override
    protected Set<String> getRequiredPropertyNames() {
        return REQUIRED_PROPERTY_NAMES;
    }
}
