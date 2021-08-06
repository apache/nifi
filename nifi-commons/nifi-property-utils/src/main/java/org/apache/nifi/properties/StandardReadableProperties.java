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

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A Properties-backed implementation of ReadableProperties.
 */
public class StandardReadableProperties implements ReadableProperties {

    private final Properties rawProperties = new Properties();

    public StandardReadableProperties(final Properties properties) {
        rawProperties.putAll(properties);
    }

    public StandardReadableProperties(final Map<String, String> properties) {
        rawProperties.putAll(properties);
    }

    @Override
    public String getProperty(final String key) {
        return rawProperties.getProperty(key);
    }

    @Override
    public String getProperty(final String key, String defaultValue) {
        return rawProperties.getProperty(key, defaultValue);
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> propertyNames = new HashSet<>();
        Enumeration e = rawProperties.propertyNames();
        for (; e.hasMoreElements(); ){
            propertyNames.add((String) e.nextElement());
        }

        return propertyNames;
    }

    protected Properties getRawProperties() {
        return rawProperties;
    }

    /**
     * Returns the size of the properties.
     * @return The size of the properties (number of keys)
     */
    public int size() {
        return rawProperties.size();
    }
}
