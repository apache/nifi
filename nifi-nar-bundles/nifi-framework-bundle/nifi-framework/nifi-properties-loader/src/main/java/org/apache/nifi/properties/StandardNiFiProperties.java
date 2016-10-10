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
import java.util.Properties;
import java.util.Set;
import org.apache.nifi.util.NiFiProperties;

public class StandardNiFiProperties extends NiFiProperties {

    private Properties rawProperties = new Properties();

    public StandardNiFiProperties() {
        this(null);
    }

    public StandardNiFiProperties(Properties props) {
        this.rawProperties = props == null ? new Properties() : props;
    }

    /**
     * Retrieves the property value for the given property key.
     *
     * @param key the key of property value to lookup
     * @return value of property at given key or null if not found
     */
    @Override
    public String getProperty(String key) {
        return rawProperties.getProperty(key);
    }

    /**
     * Retrieves all known property keys.
     *
     * @return all known property keys
     */
    @Override
    public Set<String> getPropertyKeys() {
        Set<String> propertyNames = new HashSet<>();
        Enumeration e = getRawProperties().propertyNames();
        for (; e.hasMoreElements(); ){
            propertyNames.add((String) e.nextElement());
        }

        return propertyNames;
    }

    Properties getRawProperties() {
        if (this.rawProperties == null) {
            this.rawProperties = new Properties();
        }

        return this.rawProperties;
    }

    @Override
    public int size() {
        return getRawProperties().size();
    }

    @Override
    public String toString() {
        return "StandardNiFiProperties instance with " + size() + " properties";
    }
}
