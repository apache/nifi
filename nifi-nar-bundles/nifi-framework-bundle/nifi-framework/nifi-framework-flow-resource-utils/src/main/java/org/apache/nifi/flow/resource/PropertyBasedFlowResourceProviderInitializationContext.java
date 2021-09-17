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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

final class PropertyBasedFlowResourceProviderInitializationContext implements FlowResourceProviderInitializationContext {
    private static Set<String> GUARDED_PROPERTIES = new HashSet<>(Arrays.asList("implementation"));

    private final Map<String, String> properties;
    private final Optional<Predicate<FlowResourceDescriptor>> filter;

    PropertyBasedFlowResourceProviderInitializationContext(
        final NiFiProperties properties,
        final String prefix,
        final Optional<Predicate<FlowResourceDescriptor>> filter
    ) {
        this.properties = extractProperties(properties, prefix);
        this.filter = filter;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Optional<Predicate<FlowResourceDescriptor>> getFilter() {
        return filter;
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
