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
package org.apache.nifi.nar;

import org.apache.nifi.util.NiFiProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A facade at front of {@code NiFiProperties} for auto loader extensions. Also limits the scope of the reachable properties.
 */
public class PropertyBasedNarProviderInitializationContext implements NarProviderInitializationContext {
    private static Set<String> GUARDED_PROPERTIES = new HashSet<>(Arrays.asList("implementation"));
    static final String BASIC_PREFIX = "nifi.nar.library.provider.";

    private final Map<String, String> properties;
    private final String name;

    public PropertyBasedNarProviderInitializationContext(final NiFiProperties properties, final String name) {
        this.properties = extractProperties(properties, name);
        this.name = name;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> extractProperties(final NiFiProperties properties, final String name) {
        final String prefix = BASIC_PREFIX + name + ".";
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
