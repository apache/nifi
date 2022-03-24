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
package org.apache.nifi.registry.web.security;

import org.apache.nifi.registry.NiFiRegistryApiApplication;
import org.apache.nifi.registry.security.crypto.CryptoKeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.ServletContextAware;

import javax.servlet.ServletContext;

@Configuration
public class NiFiRegistryMasterKeyProviderFactory implements ServletContextAware {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryMasterKeyProviderFactory.class);

    private CryptoKeyProvider masterKeyProvider = null;

    @Bean
    public CryptoKeyProvider getNiFiRegistryMasterKeyProvider() {
        return masterKeyProvider;
    }

    @Override
    public void setServletContext(ServletContext servletContext) {
        Object rawKeyProviderObject = servletContext.getAttribute(NiFiRegistryApiApplication.NIFI_REGISTRY_MASTER_KEY_ATTRIBUTE);

        if (rawKeyProviderObject == null) {
            logger.warn("Value of {} was null. " +
                    "{} bean will not be available in Application Context, so any attempt to load protected property values may fail.",
                    NiFiRegistryApiApplication.NIFI_REGISTRY_MASTER_KEY_ATTRIBUTE,
                    CryptoKeyProvider.class.getSimpleName());
            return;
        }

        if (!(rawKeyProviderObject instanceof CryptoKeyProvider)) {
            logger.warn("Expected value of {} to be of type {}, but instead got {}. " +
                    "{} bean will NOT be available in Application Context, so any attempt to load protected property values may fail.",
                    NiFiRegistryApiApplication.NIFI_REGISTRY_MASTER_KEY_ATTRIBUTE,
                    CryptoKeyProvider.class.getName(),
                    rawKeyProviderObject.getClass().getName(),
                    CryptoKeyProvider.class.getSimpleName());
            return;
        }

        logger.info("Updating Application Context with {} bean for obtaining NiFi Registry master key.", CryptoKeyProvider.class.getSimpleName());
        masterKeyProvider = (CryptoKeyProvider) rawKeyProviderObject;
    }

}
