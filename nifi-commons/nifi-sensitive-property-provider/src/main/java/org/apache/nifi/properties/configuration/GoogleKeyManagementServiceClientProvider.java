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

import com.google.cloud.kms.v1.KeyManagementServiceClient;

import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.properties.SensitivePropertyProtectionException;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * Google Key Management Service Client Provider
 */
public class GoogleKeyManagementServiceClientProvider extends BootstrapPropertiesClientProvider<KeyManagementServiceClient> {
    public GoogleKeyManagementServiceClientProvider() {
        super(BootstrapProperties.BootstrapPropertyKey.GCP_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF);
    }

    /**
     * Get Configured Client using default Key Management Service Client settings
     *
     * @param clientProperties Client Properties
     * @return Key Management Service Client
     */
    @Override
    protected KeyManagementServiceClient getConfiguredClient(final Properties clientProperties) {
        try {
            return KeyManagementServiceClient.create();
        } catch (final IOException e) {
            throw new SensitivePropertyProtectionException("Google Key Management Service Create Failed", e);
        }
    }

    @Override
    protected Set<String> getRequiredPropertyNames() {
        return Collections.emptySet();
    }
}
