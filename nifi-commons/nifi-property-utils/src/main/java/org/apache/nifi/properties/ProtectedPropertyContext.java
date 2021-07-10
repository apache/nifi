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

import java.util.Arrays;
import java.util.Objects;

/**
 * A context for protected properties, encapsulating the name and location of a property.
 * @see PropertyLocation#contextFor(String)
 */
public class ProtectedPropertyContext {
    /**
     * A property location, representing a specific configuration file.
     */
    public enum PropertyLocation {
        LOGIN_IDENTITY_PROVIDERS("login-identity-providers.xml"),
        AUTHORIZERS("authorizers.xml"),
        NIFI_PROPERTIES("nifi.properties"),
        NIFI_REGISTRY_PROPERTIES("nifi-registry.properties"),
        CUSTOM("custom");

        private final String location;

        PropertyLocation(final String location) {
            this.location = location;
        }

        /**
         * Returns the location of the property, representing a configuration filename.
         * @return The location of the property
         */
        public String getLocation() {
            return location;
        }

        /**
         * Creates a ProtectedPropertyContext for the given property name.
         * @param propertyName The property name in this location
         * @return A property context representing a property with the given name at this location
         */
        public ProtectedPropertyContext contextFor(final String propertyName) {
            return new ProtectedPropertyContext(propertyName, this);
        }

        /**
         * Creates a ProtectedPropertyContext for the given property name, with a custom location.
         * @param propertyName The property name in this location
         * @param customLocation A custom location name for the property context
         * @return A property context representing a property with the given name at this location
         */
        public static ProtectedPropertyContext contextFor(final String propertyName, final String customLocation) {
            return new ProtectedPropertyContext(propertyName, customLocation);
        }

        /**
         * Selects the appropriate PropertyLocation based on a configuration filename.
         * @param filename The configuration filename
         * @return The matching PropertyLocation
         * @throws IllegalArgumentException if no PropertyLocation matched the filename
         */
        public static PropertyLocation fromFilename(final String filename) {
            Objects.requireNonNull(filename, "Filename must be specified");
            return Arrays.stream(values())
                    .filter(v -> filename.contains(v.location))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("PropertyLocation not recognized from filename [%s]", filename)));
        }
    }

    private final String propertyName;
    private final PropertyLocation propertyLocation;
    private final String customLocation;

    /**
     * Creates a property context with a property name and location.
     * @param propertyName The property name
     * @param propertyLocation The location of the property
     */
    private ProtectedPropertyContext(final String propertyName, final PropertyLocation propertyLocation) {
        this.propertyName = Objects.requireNonNull(propertyName);
        this.propertyLocation = Objects.requireNonNull(propertyLocation);
        this.customLocation = null;
    }

    /**
     * Creates a property context with a property name and custom location.
     * @param propertyName The property name
     * @param customLocation A custom location for the property
     */
    private ProtectedPropertyContext(final String propertyName, final String customLocation) {
        this.propertyName = Objects.requireNonNull(propertyName);
        this.customLocation = Objects.requireNonNull(customLocation);
        this.propertyLocation = PropertyLocation.CUSTOM;
    }

    /**
     * Returns the context key, in the format [propertyLocation]||[propertyName]
     * @return The context key
     */
    public String getContextKey() {
        final String locationName = PropertyLocation.CUSTOM == propertyLocation ? customLocation : propertyLocation.location;
        return String.format("%s||%s", locationName, propertyName);
    }
}
