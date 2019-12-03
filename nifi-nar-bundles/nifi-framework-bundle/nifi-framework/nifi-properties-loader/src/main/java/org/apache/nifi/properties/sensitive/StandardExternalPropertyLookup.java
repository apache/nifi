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
package org.apache.nifi.properties.sensitive;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * External value lookup that cascades on failure.  Values are first selected from System properties, and failing that, environment
 * variables, and failing that, config files.
 *
 * The class supports an optional, one-way mapping between System properties keys and environment variable names.  For example a client
 * can specify that the System property "foo.bar" maps to the environment variable "FOO_BAR".
 *
 * This sort of behavior generally mimics how some libraries programmatically configure connections to external services (e.g., AWS).
 *
 */
public class StandardExternalPropertyLookup implements ExternalProperties {
    private final File propFile;
    private final Map<String, String> envNameMap;

    /**
     * Create a {@link StandardExternalPropertyLookup} without a property file or environment name map.
     */
    public StandardExternalPropertyLookup() {
        this(null, null);
    }

    /**
     * Create a {@link StandardExternalPropertyLookup} with a named property file.
     * @param defaultPropertiesFilename default file name
     */
    public StandardExternalPropertyLookup(String defaultPropertiesFilename) {
        this(defaultPropertiesFilename, null);
    }

    /**
     * Create a {@link StandardExternalPropertyLookup} with the given property file.
     *
     * @param propertiesFilename final lookup location, or null for none.
     * @param envNameMap mapping of property names to environment name
     */
    public StandardExternalPropertyLookup(String propertiesFilename, Map<String, String> envNameMap) {
        this.propFile = StringUtils.isNotBlank(propertiesFilename) ? new File(propertiesFilename) : null;
        this.envNameMap = envNameMap != null ? envNameMap : new HashMap<>();
    }

    /**
     * Get a string value by name from the usual locations:  System, Environment, then Properties file.
     *
     * @param name the name of the value to retrieve
     * @return the value, as a String
     */
    public String get(String name) {
        return get(name, null);
    }

    /**
     * Get a string value by name from the usual locations:  System, Environment, then Properties file.
     *
     * @param name the name of the value to retrieve
     * @param missing value to return if all lookups fail
     * @return the value, as a String
     */
    public String get(String name, String missing) {
        String value;

        // check system prop first
        value = System.getProperty(name);
        if (value != null) {
            return value;
        }

        // check env second
        value = System.getenv(envNameMap.getOrDefault(name, name));
        if (value != null) {
            return value;
        }

        // check default prop file third
        if (propFile != null) {
            Properties props = new Properties();
            try {
                props.load(new FileInputStream(propFile));
                value = props.getProperty(name);
            } catch (IOException ignored) {
            }
        }

        return value != null ? value : missing;
    }
}
