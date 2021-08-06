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
package org.apache.nifi.registry.web.api;

import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.util.KeystoreType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.nifi.registry.web.api.IntegrationTestBase.loadNiFiRegistryProperties;

// Do not add Spring annotations that would cause this class to be picked up by a ComponentScan. It must be imported manually.
public class SecureITClientConfiguration {

    @Value("${nifi.registry.client.properties.file}")
    String clientPropertiesFileLocation;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private NiFiRegistryClientConfig clientConfig;

    @Bean
    public NiFiRegistryClientConfig getNiFiRegistryClientConfig() {
        readLock.lock();
        try {
            if (clientConfig == null) {
                final NiFiRegistryProperties clientProperties = loadNiFiRegistryProperties(clientPropertiesFileLocation);
                clientConfig = createNiFiRegistryClientConfig(clientProperties);
            }
        } finally {
            readLock.unlock();
        }
        return clientConfig;
    }

    /**
     * A helper method for loading a NiFiRegistryClientConfig corresponding to a NiFiRegistryProperties object
     * holding the values needed to create a client configuration context.
     *
     * @param clientProperties A NiFiRegistryProperties object holding the config for client keystore, truststore, etc.
     * @return A NiFiRegistryClientConfig instance based on the properties file contents
     */
    private static NiFiRegistryClientConfig createNiFiRegistryClientConfig(NiFiRegistryProperties clientProperties) {

        NiFiRegistryClientConfig.Builder configBuilder = new NiFiRegistryClientConfig.Builder();

        // load keystore/truststore if applicable
        if (clientProperties.getKeyStorePath() != null) {
            configBuilder.keystoreFilename(clientProperties.getKeyStorePath());
        }
        if (clientProperties.getKeyStoreType() != null) {
            configBuilder.keystoreType(KeystoreType.valueOf(clientProperties.getKeyStoreType()));
        }
        if (clientProperties.getKeyStorePassword() != null) {
            configBuilder.keystorePassword(clientProperties.getKeyStorePassword());
        }
        if (clientProperties.getKeyPassword() != null) {
            configBuilder.keyPassword(clientProperties.getKeyPassword());
        }
        if (clientProperties.getTrustStorePath() != null) {
            configBuilder.truststoreFilename(clientProperties.getTrustStorePath());
        }
        if (clientProperties.getTrustStoreType() != null) {
            configBuilder.truststoreType(KeystoreType.valueOf(clientProperties.getTrustStoreType()));
        }
        if (clientProperties.getTrustStorePassword() != null) {
            configBuilder.truststorePassword(clientProperties.getTrustStorePassword());
        }

        return configBuilder.build();
    }

}
