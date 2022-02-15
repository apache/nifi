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

/**
 * Abstract Microsoft Azure Client Provider
 */
public abstract class AzureClientProvider<T> extends BootstrapPropertiesClientProvider<T> {
    public AzureClientProvider() {
        super(BootstrapProperties.BootstrapPropertyKey.AZURE_KEYVAULT_SENSITIVE_PROPERTY_PROVIDER_CONF);
    }

    /**
     * Get Default Azure Token Credential using Default Credentials Builder for environment variables and system properties
     *
     * @return Token Credential
     */
    protected TokenCredential getDefaultTokenCredential() {
        return new DefaultAzureCredentialBuilder().build();
    }
}
