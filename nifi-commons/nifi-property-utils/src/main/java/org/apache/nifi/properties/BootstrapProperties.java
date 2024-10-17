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

import java.nio.file.Path;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Properties;

/**
 * Properties representing bootstrap.conf.
 */
public class BootstrapProperties extends StandardReadableProperties {
    private final String propertyPrefix;
    private final Path configFilePath;

    public BootstrapProperties(final String propertyPrefix, final Properties properties, final Path configFilePath) {
        super(new Properties());

        Objects.requireNonNull(properties, "Properties are required");
        this.propertyPrefix = Objects.requireNonNull(propertyPrefix, "Property prefix is required");
        this.configFilePath = configFilePath;

        this.filterProperties(properties);
    }

    /**
     * Ensures that blank or empty properties are returned as null.
     * @param key The property key
     * @param defaultValue The default value to use if the value is null or empty
     * @return The property value (null if empty or blank)
     */
    @Override
    public String getProperty(final String key, final String defaultValue) {
        final String property = super.getProperty(key, defaultValue);
        return isBlank(property) ? null : property;
    }

    /**
     * Ensures that blank or empty properties are returned as null.
     * @param key The property key
     * @return The property value (null if empty or blank)
     */
    @Override
    public String getProperty(final String key) {
        final String property = super.getProperty(key);
        return isBlank(property) ? null : property;
    }

    /**
     * Includes only the properties starting with the propertyPrefix.
     * @param properties Unfiltered properties
     */
    private void filterProperties(final Properties properties) {
        getRawProperties().clear();
        final Properties filteredProperties = new Properties();
        for (final Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
            final String key = e.nextElement().toString();
            if (key.startsWith(propertyPrefix)) {
                filteredProperties.put(key, properties.getProperty(key));
            }
        }
        getRawProperties().putAll(filteredProperties);
    }

    @Override
    public String toString() {
        return String.format("Bootstrap properties [%s] with prefix [%s]", configFilePath, propertyPrefix);
    }

    private static boolean isBlank(final String string) {
        return (string == null) || string.isEmpty() || string.trim().isEmpty();
    }
}
