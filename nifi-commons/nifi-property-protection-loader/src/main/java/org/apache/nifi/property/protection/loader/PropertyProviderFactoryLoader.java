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
package org.apache.nifi.property.protection.loader;

import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Loader for Sensitive Property Provider Factory
 */
public class PropertyProviderFactoryLoader {
    /**
     * Get Sensitive Property Provider Factory using ServiceLoader to find available implementations from META-INF directories
     *
     * @return Sensitive Property Provider Factory
     * @throws SensitivePropertyProtectionException Thrown when no implementations found
     */
    public SensitivePropertyProviderFactory getPropertyProviderFactory() {
        final ServiceLoader<SensitivePropertyProviderFactory> serviceLoader = ServiceLoader.load(SensitivePropertyProviderFactory.class);
        final Iterator<SensitivePropertyProviderFactory> factories = serviceLoader.iterator();

        if (factories.hasNext()) {
            return factories.next();
        } else {
            throw new SensitivePropertyProtectionException(String.format("No implementations found [%s]", SensitivePropertyProviderFactory.class.getName()));
        }
    }
}
