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
package org.apache.nifi.flow.resource;

import org.apache.nifi.util.NiFiProperties;

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public final class PropertyBasedExternalResourceProviderInitializationContext implements ExternalResourceProviderInitializationContext {
    private static Set<String> GUARDED_PROPERTIES = Set.of("implementation");

    private final Map<String, String> properties;
    private final Predicate<ExternalResourceDescriptor> filter;
    private final SSLContext sslContext;

    public PropertyBasedExternalResourceProviderInitializationContext(
            final SSLContext sslContext,
            final NiFiProperties properties,
            final String prefix,
            final Predicate<ExternalResourceDescriptor> filter
    ) {
        this.properties = extractProperties(properties, prefix);
        this.filter = filter;
        this.sslContext = sslContext;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Predicate<ExternalResourceDescriptor> getFilter() {
        return filter;
    }

    @Override
    public SSLContext getSSLContext() {
        return sslContext;
    }

    private Map<String, String> extractProperties(final NiFiProperties properties, final String prefix) {
        final Map<String, String> candidates = properties.getPropertiesWithPrefix(prefix);
        final Map<String, String> result = new HashMap<>();

        for (final Map.Entry<String, String> entry : candidates.entrySet()) {
            final String parameterKey = entry.getKey().substring(prefix.length());

            if (!parameterKey.isEmpty() && !GUARDED_PROPERTIES.contains(parameterKey)) {
                result.put(parameterKey, entry.getValue());
            }
        }

        return result;
    }
}
