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
import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.util.StringUtils;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Abstract Microsoft Azure Client Provider
 */
public abstract class AzureClientProvider<T> extends BootstrapPropertiesClientProvider<T> {
    public AzureClientProvider() {
        super(BootstrapProperties.BootstrapPropertyKey.AZURE_KEYVAULT_SENSITIVE_PROPERTY_PROVIDER_CONF);
    }

    /**
     * Get Client using Client Properties
     *
     * @param clientProperties Client Properties can be null
     * @return Configured Client or empty when Client Properties object is null
     */
    @Override
    public Optional<T> getClient(final Properties clientProperties) {
        return isMissingProperties(clientProperties) ? Optional.empty() : Optional.of(getConfiguredClient(clientProperties));
    }

    /**
     * Get Default Azure Token Credential using Default Credentials Builder for environment variables and system properties
     *
     * @return Token Credential
     */
    protected TokenCredential getDefaultTokenCredential() {
        return new DefaultAzureCredentialBuilder().build();
    }

    /**
     * Get Property Names required for initializing client in order to perform initial validation
     *
     * @return Set of required client property names
     */
    protected abstract Set<String> getRequiredPropertyNames();

    private boolean isMissingProperties(final Properties clientProperties) {
        return clientProperties == null || getRequiredPropertyNames().stream().anyMatch(propertyName ->
            StringUtils.isBlank(clientProperties.getProperty(propertyName))
        );
    }
}
