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
import org.apache.nifi.properties.scheme.ProtectionSchemeResolver;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Loader for Protection Scheme Resolver
 */
public class ProtectionSchemeResolverLoader {
    /**
     * Get Protection Scheme Resolver using ServiceLoader to find available implementations from META-INF directories
     *
     * @return Protection Scheme Resolver
     * @throws SensitivePropertyProtectionException Thrown when no implementations found
     */
    public ProtectionSchemeResolver getProtectionSchemeResolver() {
        final ServiceLoader<ProtectionSchemeResolver> serviceLoader = ServiceLoader.load(ProtectionSchemeResolver.class);
        final Iterator<ProtectionSchemeResolver> factories = serviceLoader.iterator();

        if (factories.hasNext()) {
            return factories.next();
        } else {
            throw new SensitivePropertyProtectionException(String.format("No implementations found [%s]", ProtectionSchemeResolver.class.getName()));
        }
    }
}
