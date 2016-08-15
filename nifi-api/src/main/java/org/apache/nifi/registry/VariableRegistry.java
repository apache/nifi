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
package org.apache.nifi.registry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a registry of variables available for use by various components and
 * extension points. This enables components to reference variable names rather
 * than explicit values which can make configurations of those components more
 * portable.
 */
public interface VariableRegistry {

    /**
     * Returns an empty registry which can be used as a more intentional null
     * value.
     */
    public static final VariableRegistry EMPTY_REGISTRY = () -> Collections.emptyMap();

    /**
     * Provides a registry containing all environment variables and system
     * properties. System properties receive precedence.
     */
    public static final VariableRegistry ENVIRONMENT_SYSTEM_REGISTRY = new VariableRegistry() {
        final Map<VariableDescriptor, String> map = new HashMap<>();

        {
            System.getenv().entrySet().stream().forEach((entry) -> {
                final VariableDescriptor desc = new VariableDescriptor.Builder(entry.getKey())
                        .description("Env Var")
                        .sensitive(false)
                        .build();
                map.put(desc, entry.getValue());
            });
            System.getProperties().entrySet().stream().forEach((entry) -> {
                final VariableDescriptor desc = new VariableDescriptor.Builder(entry.getKey().toString())
                        .description("System Property")
                        .sensitive(false)
                        .build();
                map.put(desc, entry.getValue().toString());
            });

        }

        @Override
        public Map<VariableDescriptor, String> getVariableMap() {
            return Collections.unmodifiableMap(map);

        }

    };

    /**
     * Provides access to a map of variable key/value pairs. For variables
     * considered to be sensitive care must be taken to ensure their values are
     * protected whenever stored or exposed.
     *
     * @return An immutable map of all variables in the registry
     */

    Map<VariableDescriptor, String> getVariableMap();

    /**
     * Returns the VariableDescriptor for the given key name if it exists.
     *
     * @param name the string name of the VariableDescriptor to lookup.
     * @return the variable descriptor registered for this name if it exists;
     * null otherwise
     */
    default VariableDescriptor getVariableKey(final String name) {
        if (name == null) {
            return null;
        }
        final VariableDescriptor spec = new VariableDescriptor(name);
        for (final Map.Entry<VariableDescriptor, String> entry : getVariableMap().entrySet()) {
            if (entry.getKey().equals(spec)) {
                return entry.getKey();
            }
        }
        return null;

    }

    /**
     * Gets the variable value
     *
     * @param name the string name of the VariableDescriptor that is the key of
     * the value to lookup.
     * @return the value associated with the given variable name if found; null
     * otherwise
     */
    default String getVariableValue(final String name) {
        if (name == null) {
            return null;
        }
        return getVariableMap().get(new VariableDescriptor(name));
    }

    /**
     * Gets the variable value
     *
     * @param descriptor the descriptor for which to lookup the variable value.
     * @return the variable value if the given descriptor is equivalent to one
     * of the entries in the registry; null otherwise
     */
    default String getVariableValue(final VariableDescriptor descriptor) {
        if (descriptor == null) {
            return null;
        }
        return getVariableMap().get(descriptor);
    }

}
