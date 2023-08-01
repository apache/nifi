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
package org.apache.nifi.security.kms;

import org.apache.nifi.security.kms.configuration.KeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyStoreKeyProviderConfiguration;

import java.security.KeyStore;

/**
 * Key Provider Factory
 */
public class KeyProviderFactory {
    /**
     * Get Key Provider based on Configuration
     *
     * @param configuration Key Provider Configuration
     * @return Key Provider
     */
    public static KeyProvider getKeyProvider(final KeyProviderConfiguration<?> configuration) {
        KeyProvider keyProvider;

        if (configuration instanceof KeyStoreKeyProviderConfiguration) {
            final KeyStoreKeyProviderConfiguration providerConfiguration = (KeyStoreKeyProviderConfiguration) configuration;
            final KeyStore keyStore = providerConfiguration.getKeyStore();
            keyProvider = new KeyStoreKeyProvider(keyStore, providerConfiguration.getKeyPassword());
        } else {
            throw new UnsupportedOperationException(String.format("Key Provider [%s] not supported", configuration.getKeyProviderClass().getName()));
        }

        return keyProvider;
    }
}
