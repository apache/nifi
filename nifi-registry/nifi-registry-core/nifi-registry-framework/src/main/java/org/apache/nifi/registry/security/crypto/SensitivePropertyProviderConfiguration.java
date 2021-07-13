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
package org.apache.nifi.registry.security.crypto;

import org.apache.nifi.properties.PropertyProtectionScheme;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.registry.properties.util.NiFiRegistryBootstrapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class SensitivePropertyProviderConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(SensitivePropertyProviderConfiguration.class);

    private static final PropertyProtectionScheme DEFAULT_SCHEME = PropertyProtectionScheme.AES_GCM;

    @Autowired(required = false)
    private CryptoKeyProvider masterKeyProvider;

    /**
     * @return a SensitivePropertyProvider initialized with the master key if present,
     *         or null if the master key is not present.
     */
    @Bean
    public SensitivePropertyProvider getProvider() {
        if (masterKeyProvider == null || masterKeyProvider.isEmpty()) {
            // This NiFi Registry was not configured with a master key, so the assumption is
            // the optional Spring bean normally provided by this method will never be needed
            return null;
        }

        try {
            // Note, this bean is intentionally NOT a singleton because we want the
            // returned provider, which has a copy of the sensitive master key material
            // to be reaped when it goes out of scope in order to decrease the time
            // key material is held in memory.
            return StandardSensitivePropertyProviderFactory
                    .withKeyAndBootstrapSupplier(masterKeyProvider.getKey(), () -> {
                        try {
                            return NiFiRegistryBootstrapUtils.loadBootstrapProperties();
                        } catch (IOException e) {
                            throw new SensitivePropertyProtectionException("Error creating Sensitive Property Provider", e);
                        }
                    })
                    .getProvider(DEFAULT_SCHEME);
        } catch (final MissingCryptoKeyException e) {
            logger.warn("Error creating Sensitive Property Provider", e);
            throw new SensitivePropertyProtectionException("Error creating Sensitive Property Provider", e);
        }
    }

}
