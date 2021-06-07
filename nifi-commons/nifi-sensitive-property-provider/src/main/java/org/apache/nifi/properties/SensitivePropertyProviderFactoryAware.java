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
package org.apache.nifi.properties;

import java.util.function.Supplier;

/**
 * Provides a default SensitivePropertyProviderFactory to subclasses.
 */
public class SensitivePropertyProviderFactoryAware {

    private SensitivePropertyProviderFactory sensitivePropertyProviderFactory;

    protected SensitivePropertyProviderFactory getSensitivePropertyProviderFactory() throws SensitivePropertyProtectionException {
        if (sensitivePropertyProviderFactory == null) {
            sensitivePropertyProviderFactory = StandardSensitivePropertyProviderFactory.withDefaults();
        }
        return sensitivePropertyProviderFactory;
    }

    /**
     * Configures and sets the SensitivePropertyProviderFactory.
     * @param keyHex An key in hex format, which some providers may use for encryption
     * @param bootstrapPropertiesSupplier The bootstrap.conf properties supplier
     * @return The configured SensitivePropertyProviderFactory
     */
    public SensitivePropertyProviderFactory configureSensitivePropertyProviderFactory(final String keyHex,
                                                                                      final Supplier<BootstrapProperties> bootstrapPropertiesSupplier) {
        sensitivePropertyProviderFactory = StandardSensitivePropertyProviderFactory.withKeyAndBootstrapSupplier(keyHex, bootstrapPropertiesSupplier);
        return sensitivePropertyProviderFactory;
    }
}
